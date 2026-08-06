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
# NOAPPEND = change nothing, EXIT:<n> = fail, anything else = append it); fc
# `config slot` prints slot.txt (default "0 1"). snapchain --check-config
# fails iff the config contains "BAD". The date stub pins epoch 0, so a slot
# with index > 0 always computes a positive stagger wait; the sleep stub logs
# to sleep.log instead of sleeping. Ticks run via ONCHAIN_CONFIG_WATCH_ONCE.

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
prev=""
for a in "$@"; do
    if [[ "$prev" == "config" ]]; then subcmd="$a"; fi
    if [[ "$prev" == "--config" ]]; then config="$a"; fi
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
    cat > "$DIR/date" <<'EOF'
#!/bin/bash
cat "$(dirname "$0")/clock" 2>/dev/null || echo 0
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

# run_tick [env VAR=VAL ...] — one watcher tick in the sandbox. Watermark
# file: cache/config.toml.version (write it before, inspect it after).
run_tick() {
    RC=0
    OUT="$(cd "$DIR" && env \
        PATH="$DIR:$PATH" TMPDIR="$DIR/tmp" \
        FC_BIN="$DIR/fc" SNAPCHAIN_BIN="$DIR/snapchain" \
        ONCHAIN_WATCH_NETWORK=mainnet \
        ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml" \
        ONCHAIN_CONFIG_RESTART_CMD="touch $DIR/restarted" \
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
watermark() { cat "$DIR/cache/config.toml.version" 2>/dev/null || echo missing; }
no_stray_tmp() { ! find "$DIR/tmp" -mindepth 1 | grep -q .; }

#### counter unchanged → version poll only, nothing else
setup
echo 7 > "$DIR/version-seq"
echo 7 > "$DIR/cache/config.toml.version"
run_tick
check "unchanged: exits 0" [ "$RC" -eq 0 ]
check "unchanged: no restart" not restarted
check "unchanged: only the version call" [ "$(cat "$DIR/fc.calls")" = version ]

#### counter moved, document differs → validated restart
setup
echo 8 > "$DIR/version-seq"
echo 7 > "$DIR/cache/config.toml.version"
run_tick
check "change: exits 0" [ "$RC" -eq 0 ]
check "change: restart triggered" restarted
check "change: validated before restarting" grep -qx pull "$DIR/fc.calls"
check "change: names the version and slot" grep -q "config version 8 (slot 0)" <<< "$OUT"
check "change: watermark untouched (boot records it)" [ "$(watermark)" = 7 ]
check "change: running config untouched" not grep -q merged-from-registry "$DIR/config.toml"
check "change: no temp copies left" no_stray_tmp

#### counter moved, document byte-identical → watermark only, no restart
setup
echo 8 > "$DIR/version-seq"
echo NOAPPEND > "$DIR/pull-seq"
echo 7 > "$DIR/cache/config.toml.version"
run_tick
check "noop bump: exits 0" [ "$RC" -eq 0 ]
check "noop bump: no restart" not restarted
check "noop bump: watermark advanced" [ "$(watermark)" = 8 ]
check "noop bump: says so" grep -q "does not change the rendered config" <<< "$OUT"

#### deliberately-bad document → caught before any restart
setup
echo 8 > "$DIR/version-seq"
echo BAD > "$DIR/pull-seq"
echo 7 > "$DIR/cache/config.toml.version"
run_tick
check "bad config: exits 0" [ "$RC" -eq 0 ]
check "bad config: NO restart" not restarted
check "bad config: refuses loudly" grep -q "fails --check-config" <<< "$OUT"
check "bad config: watermark untouched" [ "$(watermark)" = 7 ]
check "bad config: no temp copies left" no_stray_tmp

#### pull failure → no restart, retry later
setup
echo 8 > "$DIR/version-seq"
echo EXIT:3 > "$DIR/pull-seq"
echo 7 > "$DIR/cache/config.toml.version"
run_tick
check "pull failure: exits 0" [ "$RC" -eq 0 ]
check "pull failure: no restart" not restarted
check "pull failure: logs error" grep -q "config pull for version 8 failed" <<< "$OUT"

#### version poll failure (RPC down) → quiet retry
setup
: > "$DIR/version-seq"
echo 7 > "$DIR/cache/config.toml.version"
run_tick
check "poll failure: exits 0" [ "$RC" -eq 0 ]
check "poll failure: no restart" not restarted
check "poll failure: warns" grep -q "configVersion poll failed" <<< "$OUT"

#### garbage slot output → no restart
setup
echo 8 > "$DIR/version-seq"
echo 7 > "$DIR/cache/config.toml.version"
echo banana > "$DIR/slot.txt"
run_tick
check "bad slot: no restart" not restarted
check "bad slot: logs error" grep -q "cannot compute stagger slot" <<< "$OUT"

#### stagger wait honored, then re-evaluated: counter reverted to watermark
setup
printf '8\n7\n' > "$DIR/version-seq"
echo 7 > "$DIR/cache/config.toml.version"
echo "1 2" > "$DIR/slot.txt"   # epoch pinned to 0 → slot 1 waits 1 window
run_tick ONCHAIN_CONFIG_STAGGER_WINDOW=100
check "reverted while waiting: exits 0" [ "$RC" -eq 0 ]
check "reverted while waiting: waited for the window" grep -qx "slept 100" "$DIR/sleep.log"
check "reverted while waiting: NO restart" not restarted
check "reverted while waiting: says so" grep -q "no longer applies" <<< "$OUT"

#### re-evaluation catches a revert to identical content under a NEW version
setup
printf '8\n9\n' > "$DIR/version-seq"
printf '# merged-from-registry\nNOAPPEND\n' > "$DIR/pull-seq"
echo 7 > "$DIR/cache/config.toml.version"
echo "1 2" > "$DIR/slot.txt"
run_tick ONCHAIN_CONFIG_STAGGER_WINDOW=100
check "content revert while waiting: NO restart" not restarted
check "content revert while waiting: watermark set to the new version" \
    [ "$(watermark)" = 9 ]

#### a still-standing change survives the re-evaluation and restarts
setup
printf '8\n8\n' > "$DIR/version-seq"
echo 7 > "$DIR/cache/config.toml.version"
echo "1 2" > "$DIR/slot.txt"
run_tick ONCHAIN_CONFIG_STAGGER_WINDOW=100
check "standing change: waited then restarted" restarted
check "standing change: exactly one wait" [ "$(wc -l < "$DIR/sleep.log")" -eq 1 ]

#### a mid-wait write that moves this node's slot re-enters the wait loop
setup
printf '8\n8\n8\n' > "$DIR/version-seq"
printf '1 2\n2 3\n2 3\n' > "$DIR/slot-seq"
echo 7 > "$DIR/cache/config.toml.version"
run_tick ONCHAIN_CONFIG_STAGGER_WINDOW=100
check "slot shift: exits 0" [ "$RC" -eq 0 ]
check "slot shift: waited once for the old slot, once for the new" \
    [ "$(wc -l < "$DIR/sleep.log")" -eq 2 ]
check "slot shift: restarted only from the recomputed window" restarted
check "slot shift: restart names the new slot" grep -q "(slot 2)" <<< "$OUT"

#### restart command failure → loud, watermark untouched, watcher survives
setup
echo 8 > "$DIR/version-seq"
echo 7 > "$DIR/cache/config.toml.version"
run_tick ONCHAIN_CONFIG_RESTART_CMD=false
check "restart failure: exits 0" [ "$RC" -eq 0 ]
check "restart failure: logged loudly" grep -q "restart command failed" <<< "$OUT"
check "restart failure: watermark untouched" [ "$(watermark)" = 7 ]

#### loop mode: a failed restart does not silently kill the watcher
setup
echo 8 > "$DIR/version-seq"
echo 7 > "$DIR/cache/config.toml.version"
(cd "$DIR" && env \
    PATH="$DIR:$PATH" TMPDIR="$DIR/tmp" \
    FC_BIN="$DIR/fc" SNAPCHAIN_BIN="$DIR/snapchain" \
    ONCHAIN_WATCH_NETWORK=mainnet \
    ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml" \
    ONCHAIN_CONFIG_RESTART_CMD=false \
    ONCHAIN_CONFIG_POLL_INTERVAL=1 \
    "$SCRIPT" config.toml > "$DIR/loop.log" 2>&1 & echo $! > "$DIR/loop.pid")
/bin/sleep 1
kill "$(cat "$DIR/loop.pid")" 2>/dev/null || true
check "loop mode: restart failure logged" grep -q "restart command failed" "$DIR/loop.log"
check "loop mode: loop continued past the failure" \
    grep -q "configVersion poll failed" "$DIR/loop.log"

#### registry/rpc env propagate to the version poll
setup
echo 7 > "$DIR/version-seq"
echo 7 > "$DIR/cache/config.toml.version"
run_tick ONCHAIN_CONFIG_REGISTRY=0xabc ONCHAIN_CONFIG_RPC_URL=https://rpc.example
check "registry args: --registry forwarded" grep -qx 0xabc "$DIR/fc.version.args"
check "registry args: --rpc-url forwarded" grep -qx https://rpc.example "$DIR/fc.version.args"

#### no watermark file at all (fresh volume) → treated as 0, one catch-up pass
setup
echo 5 > "$DIR/version-seq"
echo NOAPPEND > "$DIR/pull-seq"
run_tick
check "fresh volume: no restart when content already current" not restarted
check "fresh volume: watermark seeded" [ "$(watermark)" = 5 ]

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
check "window: zero-count (defensive) immediate" \
    [ "$(seconds_until_window 12345 0 0 900)" = 0 ]
check "window: at own window start" [ "$(seconds_until_window 0 0 4 100)" = 0 ]
check "window: waits for own slot" [ "$(seconds_until_window 0 2 4 100)" = 200 ]
# pos 250 is INSIDE slot 2's window [200,300) — but only window STARTS
# trigger, so the wait wraps a full cycle: (200 - 250 + 500) % 500 = 450.
check "window: mid-own-window waits for the next cycle" \
    [ "$(seconds_until_window 250 2 4 100)" = 450 ]
check "window: sentinel slot is the trailing window" \
    [ "$(seconds_until_window 0 4 4 100)" = 400 ]
check "window: wraps across cycle boundary" \
    [ "$(seconds_until_window 499 1 4 100)" = 101 ]
# Grace: the post-sleep re-check can land a beat late; the first tenth of the
# window still counts as its start.
check "window: grace absorbs drift just past the start" \
    [ "$(seconds_until_window 205 2 4 100)" = 0 ]
check "window: past the grace waits for the next cycle" \
    [ "$(seconds_until_window 211 2 4 100)" = 489 ]

echo
if [[ "$FAILURES" -gt 0 ]]; then
    echo "$FAILURES/$TESTS checks FAILED"
    exit 1
fi
echo "all $TESTS checks passed"
