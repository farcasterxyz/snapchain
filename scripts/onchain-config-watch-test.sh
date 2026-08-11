#!/bin/bash

# Tests for onchain-config-watch.sh. Self-contained: stubs stand in for the
# `fc` and `snapchain` binaries, the restart trigger, and — via PATH — `date`
# and `sleep`, so ticks are deterministic and instant.
#
#   ./scripts/onchain-config-watch-test.sh
#
# Stub contract: fc `config version` consumes one line per call from
# version-seq (empty/missing file = poll failure); fc `config pull` consumes
# one line per call from pull-seq ("" or missing = append the merge marker,
# NOAPPEND = change nothing, EXIT:<n> = fail, anything else = append it) and
# writes the bound version — one line per call from bound-seq, default 42 —
# to its --report-version path; fc `config slot` consumes slot-seq if
# present, else prints slot.txt (default "0 1"). snapchain --check-config
# fails iff the config contains "BAD". The date stub reads the fake clock
# (starts at epoch 0, advanced by the sleep stub), so a slot with index > 0
# computes a positive stagger wait deterministically. The watermark is
# handed in via ONCHAIN_WATCH_WATERMARK per test (unset -> 0); watermark
# advances are asserted via the "recording watermark" log line, since the
# watermark deliberately lives only in the watcher's memory. Ticks run via
# ONCHAIN_CONFIG_WATCH_ONCE.

set -euo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/onchain-config-watch.sh"
FAILURES=0
TESTS=0

setup() {
    DIR="$(mktemp -d)"
    mkdir -p "$DIR/cache" "$DIR/tmp"
    cat > "$DIR/fc" <<'EOF'
#!/bin/bash
subcmd=""
config=""
report=""
prev=""
for a in "$@"; do
    if [[ "$prev" == "config" ]]; then subcmd="$a"; fi
    if [[ "$prev" == "--config" ]]; then config="$a"; fi
    if [[ "$prev" == "--report-version" ]]; then report="$a"; fi
    prev="$a"
done
here="$(dirname "$0")"
echo "$subcmd" >> "$here/fc.calls"
consume() { # pop and print the first line of $1; fails if empty/missing
    [[ -s "$1" ]] || return 1
    head -1 "$1"
    tail -n +2 "$1" > "$1.shift" && mv "$1.shift" "$1"
}
case "$subcmd" in
version)
    printf '%s\n' "$@" > "$here/fc.version.args"
    consume "$here/version-seq" || exit 1
    ;;
pull)
    line="$(consume "$here/pull-seq" 2>/dev/null || true)"
    case "$line" in
        NOAPPEND) ;;
        EXIT:*) exit "${line#EXIT:}" ;;
        "") echo "# merged-from-registry" >> "$config" ;;
        *) echo "$line" >> "$config" ;;
    esac
    if [[ -n "$report" ]]; then
        bound="$(consume "$here/bound-seq" 2>/dev/null || echo 42)"
        printf '%s\n' "$bound" > "$report"
    fi
    ;;
slot)
    if [[ -f "$here/slot-seq" ]]; then
        consume "$here/slot-seq" || exit 1
    elif [[ -f "$here/slot.txt" ]]; then
        cat "$here/slot.txt"
    else
        echo "0 1"
    fi
    ;;
esac
EOF
    cat > "$DIR/snapchain" <<'EOF'
#!/bin/bash
config=""
while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--config-path" ]]; then config="$2"; fi
    shift
done
grep -q "BAD" "$config" && exit 1
echo "config OK"
EOF
    # A coherent fake clock: date reads it, sleep advances it. Without the
    # advance, the post-sleep window re-check would recompute the same
    # positive delta forever and the re-evaluation loop could never converge.
    # log() calls date with -u and a format string for its JSON timestamp;
    # answer those with a fixed valid instant so the shape stays checkable.
    cat > "$DIR/date" <<'EOF'
#!/bin/bash
if [[ "${1:-}" == "-u" ]]; then
    echo "1970-01-01T00:00:00Z"
else
    cat "$(dirname "$0")/clock" 2>/dev/null || echo 0
fi
EOF
    cat > "$DIR/sleep" <<'EOF'
#!/bin/bash
here="$(dirname "$0")"
echo "slept $1" >> "$here/sleep.log"
now="$(cat "$here/clock" 2>/dev/null || echo 0)"
echo $((now + $1)) > "$here/clock"
EOF
    chmod +x "$DIR/fc" "$DIR/snapchain" "$DIR/date" "$DIR/sleep"
    cat > "$DIR/config.toml" <<'EOF'
fc_network = "Mainnet"
read_node = false

[consensus]
private_key = "not-a-real-key"
EOF
}

# run_tick [env VAR=VAL ...] — one watcher tick in the sandbox. Pass the
# watermark as ONCHAIN_WATCH_WATERMARK=N (the boot-to-watcher handoff).
run_tick() {
    RC=0
    OUT="$(cd "$DIR" && env \
        PATH="$DIR:$PATH" TMPDIR="$DIR/tmp" \
        FC_BIN="$DIR/fc" SNAPCHAIN_BIN="$DIR/snapchain" \
        ONCHAIN_WATCH_NETWORK=mainnet \
        ONCHAIN_CONFIG_RESTART_CMD="touch $DIR/restarted" \
        ONCHAIN_CONFIG_RESTART_GRACE=7 \
        ONCHAIN_CONFIG_WATCH_ONCE=1 \
        "$@" \
        "$SCRIPT" config.toml 2>&1)" || RC=$?
}

check() {
    local desc="$1"
    shift
    TESTS=$((TESTS + 1))
    if "$@"; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc"
        FAILURES=$((FAILURES + 1))
    fi
}

not() { ! "$@"; }

restarted() { [[ -f "$DIR/restarted" ]]; }
no_stray_tmp() { ! find "$DIR/tmp" -mindepth 1 | grep -q .; }

#### counter unchanged → version poll only, nothing else
setup
echo 7 > "$DIR/version-seq"
run_tick ONCHAIN_WATCH_WATERMARK=7
check "unchanged: exits 0" [ "$RC" -eq 0 ]
check "unchanged: no restart" not restarted
check "unchanged: only the version call" [ "$(cat "$DIR/fc.calls")" = version ]

#### counter BELOW the watermark (stale RPC view) → equally nothing
setup
echo 6 > "$DIR/version-seq"
run_tick ONCHAIN_WATCH_WATERMARK=7
check "stale gate: no restart" not restarted
check "stale gate: no pull either" [ "$(cat "$DIR/fc.calls")" = version ]

#### counter moved, document differs → validated restart
setup
echo 8 > "$DIR/version-seq"
echo 8 > "$DIR/bound-seq"
run_tick ONCHAIN_WATCH_WATERMARK=7
check "change: exits 0" [ "$RC" -eq 0 ]
check "change: restart triggered" restarted
check "change: validated before restarting" grep -qx pull "$DIR/fc.calls"
check "change: names the version and slot" grep -q "config version 8 (slot 0)" <<< "$OUT"
check "change: watermark not advanced (boot re-derives it)" \
    not grep -q "recording watermark" <<< "$OUT"
check "change: running config untouched" not grep -q merged-from-registry "$DIR/config.toml"
check "change: no temp copies left" no_stray_tmp

#### counter moved, document byte-identical → watermark only, no restart
setup
echo 8 > "$DIR/version-seq"
echo 8 > "$DIR/bound-seq"
echo NOAPPEND > "$DIR/pull-seq"
run_tick ONCHAIN_WATCH_WATERMARK=7
check "noop bump: exits 0" [ "$RC" -eq 0 ]
check "noop bump: no restart" not restarted
check "noop bump: watermark advanced to the bound version" \
    grep -q "version 8 does not change the rendered config; recording watermark" <<< "$OUT"

#### trigger sees a new version but the pull lands on a stale backend → no watermark advance
# (The bound version is what the pulled document actually is; recording the
# unbound trigger version over stale content would swallow the real change.)
setup
echo 8 > "$DIR/version-seq"
echo 7 > "$DIR/bound-seq"
echo NOAPPEND > "$DIR/pull-seq"
run_tick ONCHAIN_WATCH_WATERMARK=7
check "stale bound: exits 0" [ "$RC" -eq 0 ]
check "stale bound: no restart" not restarted
check "stale bound: no watermark advance" not grep -q "recording watermark" <<< "$OUT"
check "stale bound: names the stale view" grep -q "stale RPC view" <<< "$OUT"

#### deliberately-bad document → caught before any restart
setup
echo 8 > "$DIR/version-seq"
echo 8 > "$DIR/bound-seq"
echo BAD > "$DIR/pull-seq"
run_tick ONCHAIN_WATCH_WATERMARK=7
check "bad config: exits 0" [ "$RC" -eq 0 ]
check "bad config: NO restart" not restarted
check "bad config: refuses loudly" grep -q "fails --check-config" <<< "$OUT"
check "bad config: watermark untouched" not grep -q "recording watermark" <<< "$OUT"
check "bad config: no temp copies left" no_stray_tmp

#### pull failure → no restart, retry later
setup
echo 8 > "$DIR/version-seq"
echo EXIT:3 > "$DIR/pull-seq"
run_tick ONCHAIN_WATCH_WATERMARK=7
check "pull failure: exits 0" [ "$RC" -eq 0 ]
check "pull failure: no restart" not restarted
check "pull failure: logs error" grep -q "config pull for version 8 failed" <<< "$OUT"

#### version poll failure (RPC down) → quiet retry
setup
: > "$DIR/version-seq"
run_tick ONCHAIN_WATCH_WATERMARK=7
check "poll failure: exits 0" [ "$RC" -eq 0 ]
check "poll failure: no restart" not restarted
check "poll failure: warns" grep -q "configVersion poll failed" <<< "$OUT"
# The sandbox's fake-clock date stub feeds log()'s timestamp too, so pin the
# shape but not the timestamp contents.
check "logs: JSON lines matching the node's shape" \
    grep -Eq '^\{"timestamp":"[^"]*","level":"WARN","fields":\{"message":"configVersion poll failed[^"]*"\},"target":"onchain-config-watch"\}$' \
    <<< "$OUT"
check "logs: no bare-text log lines" not grep -q '^\[onchain-config-watch\]' <<< "$OUT"

#### garbage slot output → no restart
setup
echo 8 > "$DIR/version-seq"
echo banana > "$DIR/slot.txt"
run_tick ONCHAIN_WATCH_WATERMARK=7
check "bad slot: no restart" not restarted
check "bad slot: logs error" grep -q "cannot compute stagger slot" <<< "$OUT"

#### an unreadable clock must not restart as if it were epoch 0
setup
echo 8 > "$DIR/version-seq"
printf '#!/bin/bash\nexit 1\n' > "$DIR/date"
run_tick ONCHAIN_WATCH_WATERMARK=7
check "clock failure: exits 0" [ "$RC" -eq 0 ]
check "clock failure: NO restart" not restarted
check "clock failure: warns" grep -q "cannot read the clock" <<< "$OUT"

#### a cmp I/O error (exit >= 2) must not masquerade as "changed"
setup
echo 8 > "$DIR/version-seq"
printf '#!/bin/bash\nexit 2\n' > "$DIR/cmp" && chmod +x "$DIR/cmp"
run_tick ONCHAIN_WATCH_WATERMARK=7
check "cmp failure: exits 0" [ "$RC" -eq 0 ]
check "cmp failure: NO restart" not restarted
check "cmp failure: warns" grep -q "cannot compare" <<< "$OUT"

#### stagger wait honored, then re-evaluated: counter reverted to watermark
setup
printf '8\n7\n' > "$DIR/version-seq"
echo 8 > "$DIR/bound-seq"
echo "1 2" > "$DIR/slot.txt"   # fake clock starts at 0 → slot 1 waits 1 window
run_tick ONCHAIN_WATCH_WATERMARK=7 ONCHAIN_CONFIG_STAGGER_WINDOW=200
check "reverted while waiting: exits 0" [ "$RC" -eq 0 ]
check "reverted while waiting: waited for the window" grep -qx "slept 200" "$DIR/sleep.log"
check "reverted while waiting: NO restart" not restarted
check "reverted while waiting: says so" grep -q "no longer applies" <<< "$OUT"

#### re-evaluation catches a revert to identical content under a NEW version
setup
printf '8\n9\n' > "$DIR/version-seq"
printf '8\n9\n' > "$DIR/bound-seq"
printf '# merged-from-registry\nNOAPPEND\n' > "$DIR/pull-seq"
echo "1 2" > "$DIR/slot.txt"
run_tick ONCHAIN_WATCH_WATERMARK=7 ONCHAIN_CONFIG_STAGGER_WINDOW=200
check "content revert while waiting: NO restart" not restarted
check "content revert while waiting: watermark set to the new version" \
    grep -q "version 9); recording watermark" <<< "$OUT"

#### a still-standing change survives the re-evaluation and restarts
setup
printf '8\n8\n' > "$DIR/version-seq"
printf '8\n8\n' > "$DIR/bound-seq"
echo "1 2" > "$DIR/slot.txt"
run_tick ONCHAIN_WATCH_WATERMARK=7 ONCHAIN_CONFIG_STAGGER_WINDOW=200
check "standing change: waited then restarted" restarted
check "standing change: exactly one wait" \
    [ "$(grep -cx "slept 200" "$DIR/sleep.log")" -eq 1 ]
# The stubbed restart command "succeeds" but nothing actually stops the
# container (this process), which is exactly the hung-shutdown case: after
# the grace sleep the watcher must re-arm loudly instead of exiting.
check "standing change: sent-but-survived waits out the restart grace" \
    grep -qx "slept 7" "$DIR/sleep.log"
check "standing change: sent-but-survived re-arms loudly" \
    grep -q "still running 7s after the restart trigger" <<< "$OUT"
check "standing change: sent-but-survived exits 0" [ "$RC" -eq 0 ]

#### a mid-wait write that moves this node's slot re-enters the wait loop
setup
printf '8\n8\n8\n' > "$DIR/version-seq"
printf '8\n8\n8\n' > "$DIR/bound-seq"
printf '1 2\n2 3\n2 3\n' > "$DIR/slot-seq"
run_tick ONCHAIN_WATCH_WATERMARK=7 ONCHAIN_CONFIG_STAGGER_WINDOW=200
check "slot shift: exits 0" [ "$RC" -eq 0 ]
check "slot shift: waited once for the old slot, once for the new" \
    [ "$(grep -cx "slept 200" "$DIR/sleep.log")" -eq 2 ]
check "slot shift: restarted only from the recomputed window" restarted
check "slot shift: restart names the new slot" grep -q "(slot 2)" <<< "$OUT"

#### restart command failure → loud, watcher survives
setup
echo 8 > "$DIR/version-seq"
run_tick ONCHAIN_WATCH_WATERMARK=7 ONCHAIN_CONFIG_RESTART_CMD=false
check "restart failure: exits 0" [ "$RC" -eq 0 ]
check "restart failure: logged loudly" grep -q "restart command failed" <<< "$OUT"
check "restart failure: watermark untouched" not grep -q "recording watermark" <<< "$OUT"

#### loop mode: a failed restart does not silently kill the watcher
setup
echo 8 > "$DIR/version-seq"
(cd "$DIR" && env \
    PATH="$DIR:$PATH" TMPDIR="$DIR/tmp" \
    FC_BIN="$DIR/fc" SNAPCHAIN_BIN="$DIR/snapchain" \
    ONCHAIN_WATCH_NETWORK=mainnet \
    ONCHAIN_WATCH_WATERMARK=7 \
    ONCHAIN_CONFIG_RESTART_CMD=false \
    ONCHAIN_CONFIG_POLL_INTERVAL=1 \
    "$SCRIPT" config.toml > "$DIR/loop.log" 2>&1 & echo $! > "$DIR/loop.pid")
# Wait for the tick-2 marker (the poll failure that proves the loop survived
# tick 1's failed restart) rather than a fixed real-time sleep: under load, a
# fixed sleep races the background loop's scheduling and flakes. /bin/sleep
# bypasses the PATH stub — this wait is real wall-clock, not the fake clock.
for _ in $(seq 100); do
    grep -q "configVersion poll failed" "$DIR/loop.log" 2>/dev/null && break
    /bin/sleep 0.1
done
kill "$(cat "$DIR/loop.pid")" 2>/dev/null || true
check "loop mode: restart failure logged" grep -q "restart command failed" "$DIR/loop.log"
check "loop mode: loop continued past the failure" \
    grep -q "configVersion poll failed" "$DIR/loop.log"

#### registry/rpc env propagate to the version poll
setup
echo 7 > "$DIR/version-seq"
run_tick ONCHAIN_WATCH_WATERMARK=7 \
    ONCHAIN_CONFIG_REGISTRY=0xabc ONCHAIN_CONFIG_RPC_URL=https://rpc.example
check "registry args: --registry forwarded" grep -qx 0xabc "$DIR/fc.version.args"
check "registry args: --rpc-url forwarded" grep -qx https://rpc.example "$DIR/fc.version.args"

#### no handoff watermark (fallback boot) → treated as 0, one catch-up pass
setup
echo 5 > "$DIR/version-seq"
echo 5 > "$DIR/bound-seq"
echo NOAPPEND > "$DIR/pull-seq"
run_tick
check "no handoff: no restart when content already current" not restarted
check "no handoff: watermark re-derived from the bound pull" \
    grep -q "version 5 does not change the rendered config" <<< "$OUT"

#### garbage handoff watermark → sanitized to 0, same catch-up
setup
echo 5 > "$DIR/version-seq"
echo 5 > "$DIR/bound-seq"
echo NOAPPEND > "$DIR/pull-seq"
run_tick ONCHAIN_WATCH_WATERMARK=banana
check "garbage handoff: no restart, catch-up ran" \
    grep -q "recording watermark" <<< "$OUT"

#### a version too wide for bash's signed-64-bit arithmetic is a failed poll,
#### not a negative number that parks the watcher as "current" forever
setup
echo 99999999999999999999 > "$DIR/version-seq"   # 20 digits > the 18-digit bound
run_tick ONCHAIN_WATCH_WATERMARK=7
check "overflow-width version: treated as a failed poll" \
    grep -q "configVersion poll failed" <<< "$OUT"
check "overflow-width version: NO restart" not restarted

#### a stagger window whose grace tenth cannot cover one evaluation's latency
#### would livelock the restart path — refused at startup, not at rollout time
setup
RC=0
OUT="$(env FC_BIN="$DIR/fc" SNAPCHAIN_BIN="$DIR/snapchain" \
    ONCHAIN_WATCH_NETWORK=mainnet ONCHAIN_CONFIG_STAGGER_WINDOW=60 \
    ONCHAIN_CONFIG_WATCH_ONCE=1 "$SCRIPT" "$DIR/config.toml" 2>&1)" || RC=$?
check "tiny stagger window: exits nonzero" [ "$RC" -ne 0 ]
check "tiny stagger window: says why" grep -q "must be >= 120" <<< "$OUT"

#### missing required network env → refuses to start
setup
RC=0
OUT="$(env -u ONCHAIN_WATCH_NETWORK FC_BIN="$DIR/fc" SNAPCHAIN_BIN="$DIR/snapchain" \
    ONCHAIN_CONFIG_WATCH_ONCE=1 "$SCRIPT" "$DIR/config.toml" 2>&1)" || RC=$?
check "missing network: exits nonzero" [ "$RC" -ne 0 ]
check "missing network: says why" grep -q ONCHAIN_WATCH_NETWORK <<< "$OUT"

#### window arithmetic (pure function, via sourcing)
# $SCRIPT's source guard makes sourcing definitions-only.
# shellcheck source=/dev/null
source "$SCRIPT"
check "window: single validator restarts immediately" \
    [ "$(seconds_until_window 12345 0 1 900)" = 0 ]
# A union shrunk to one key must NOT restart its non-members immediately —
# they share the trailing window: cycle 2*900, sentinel start 900, pos
# 12345 % 1800 = 1545 -> (900 - 1545 + 1800) % 1800 = 1155.
check "window: count-of-one sentinel keeps the trailing window" \
    [ "$(seconds_until_window 12345 1 1 900)" = 1155 ]
check "window: zero-count (defensive) immediate" \
    [ "$(seconds_until_window 12345 0 0 900)" = 0 ]
check "window: at own window start" [ "$(seconds_until_window 0 0 4 1000)" = 0 ]
check "window: waits for own slot" [ "$(seconds_until_window 0 2 4 1000)" = 2000 ]
# pos 2500 is INSIDE slot 2's window [2000,3000) — but only window STARTS
# trigger, so the wait wraps a full cycle: (2000 - 2500 + 5000) % 5000 = 4500.
check "window: mid-own-window waits for the next cycle" \
    [ "$(seconds_until_window 2500 2 4 1000)" = 4500 ]
check "window: sentinel slot is the trailing window" \
    [ "$(seconds_until_window 0 4 4 1000)" = 4000 ]
check "window: wraps across cycle boundary" \
    [ "$(seconds_until_window 4999 1 4 1000)" = 1001 ]
# Grace: the post-sleep re-check can land a beat late; the first tenth of the
# window still counts as its start.
check "window: grace absorbs drift just past the start" \
    [ "$(seconds_until_window 2050 2 4 1000)" = 0 ]
check "window: past the grace waits for the next cycle" \
    [ "$(seconds_until_window 2110 2 4 1000)" = 4890 ]
# The grace floor: window/10 alone can be outrun by one evaluation's RPC
# latency, so short windows get a flat 60s grace (still < the 120s window
# minimum, so grace can never reach the next slot's window).
check "window: grace floor covers a slow evaluation on a short window" \
    [ "$(seconds_until_window 450 2 4 200)" = 0 ]
check "window: past the floored grace waits for the next cycle" \
    [ "$(seconds_until_window 461 2 4 200)" = 939 ]

echo
if [[ "$FAILURES" -gt 0 ]]; then
    echo "$FAILURES/$TESTS checks FAILED"
    exit 1
fi
echo "all $TESTS checks passed"
