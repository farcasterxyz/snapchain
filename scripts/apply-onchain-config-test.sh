#!/bin/bash

# Tests for apply-onchain-config.sh. Self-contained: stubs stand in for the
# `fc` and `snapchain` binaries, so this runs anywhere bash does.
#
#   ./scripts/apply-onchain-config-test.sh

set -euo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/apply-onchain-config.sh"
FAILURES=0
TESTS=0

# Each test gets a fresh sandbox with stub binaries and a validator config.
# Stub behavior is content-driven: the fc stub records `config pull` argv to
# fc.args and appends a marker to the config (FC_STUB_APPEND overrides the
# marker, FC_STUB_NO_APPEND simulates a pull that changes nothing,
# FC_STUB_EXIT fails the pull), and writes FC_STUB_BOUND_VERSION (default
# 42) to the --report-version path unless FC_STUB_NO_REPORT is set. Each
# call appends its subcommand to fc.calls. The snapchain stub fails
# --check-config iff the config contains "BAD". The watcher spawn is
# intercepted by a stub (ONCHAIN_CONFIG_WATCH_BIN) that records its argv and
# environment to watch.spawned — tests must never start the real watch loop.
setup() {
    DIR="$(mktemp -d)"
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
echo "$subcmd" >> "$(dirname "$0")/fc.calls"
[[ "${FC_STUB_EXIT:-0}" != 0 ]] && exit "${FC_STUB_EXIT}"
case "$subcmd" in
pull)
    printf '%s\n' "$@" > "$(dirname "$0")/fc.args"
    # Mirror real fc: it refuses read_node = true configs in write mode (it
    # only checks the file — the SNAPCHAIN_READ_NODE env overlay is invisible
    # to it).
    if grep -Eq '^[[:space:]]*read_node[[:space:]]*=[[:space:]]*true' "$config"; then
        echo "read_node = true; the registry manages validator config only" >&2
        exit 1
    fi
    if [[ -z "${FC_STUB_NO_APPEND:-}" ]]; then
        echo "${FC_STUB_APPEND:-# merged-from-registry}" >> "$config"
    fi
    if [[ -n "$report" && -z "${FC_STUB_NO_REPORT:-}" ]]; then
        printf '%s\n' "${FC_STUB_BOUND_VERSION:-42}" > "$report"
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
    cat > "$DIR/watch-stub" <<'EOF'
#!/bin/bash
{
    echo "argv: $*"
    echo "network: ${ONCHAIN_WATCH_NETWORK:-}"
    echo "watermark: ${ONCHAIN_WATCH_WATERMARK:-}"
} > "$(dirname "$0")/watch.spawned"
EOF
    chmod +x "$DIR/fc" "$DIR/snapchain" "$DIR/watch-stub"
    cat > "$DIR/config.toml" <<'EOF'
fc_network = "Mainnet"
read_node = false
l1_rpc_url = "https://l1.example"
EOF
}

# run_script [env VAR=VAL ...] — invokes the script in the sandbox with the
# stub binaries wired up; captures exit code in RC and output in OUT. No
# ONCHAIN_CONFIG_ENABLED is injected: the default (on) is itself under test.
run_script() {
    RC=0
    OUT="$(cd "$DIR" && env \
        FC_BIN="$DIR/fc" SNAPCHAIN_BIN="$DIR/snapchain" \
        ONCHAIN_CONFIG_WATCH_BIN="$DIR/watch-stub" \
        "$@" \
        "$SCRIPT" config.toml 2>&1)" || RC=$?
}

watcher_spawned() { [[ -f "$DIR/watch.spawned" ]]; }

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

# `check "desc" not some_command` — bare `!` is shell syntax, not a command,
# so it can't ride through check's "$@".
not() { ! "$@"; }

fc_was_called() { [[ -f "$DIR/fc.args" ]]; }

#### the gate: on by default, false/0 opt out, garbage refuses
setup
run_script ONCHAIN_CONFIG_ENABLED=false
check "opt-out (false): exits 0" [ "$RC" -eq 0 ]
check "opt-out (false): fc not called" not fc_was_called
check "opt-out (false): config untouched" not grep -q merged-from-registry "$DIR/config.toml"

setup
run_script ONCHAIN_CONFIG_ENABLED=0
check "opt-out (0): fc not called" not fc_was_called

#### default-on: unset runs the pull; so does the empty string the compose
#### environment: block passes through when the host leaves the var unset
setup
run_script
check "default-on (unset): exits 0" [ "$RC" -eq 0 ]
check "default-on (unset): merge applied" grep -q merged-from-registry "$DIR/config.toml"

setup
run_script ONCHAIN_CONFIG_ENABLED=
check "default-on (empty): merge applied" grep -q merged-from-registry "$DIR/config.toml"

setup
run_script ONCHAIN_CONFIG_ENABLED=true
check "explicit true: merge applied" grep -q merged-from-registry "$DIR/config.toml"

#### a typo'd kill switch must fail loudly, not silently enable. DELIBERATE
#### (reviewed 2026-08-11): this includes disable-shaped spellings — FALSE,
#### off, no — refusing to boot rather than guessing at intent. The only
#### accepted spellings are the exact lowercase true/1/false/0.
setup
run_script ONCHAIN_CONFIG_ENABLED=flase
check "typo'd gate: exits nonzero" [ "$RC" -ne 0 ]
check "typo'd gate: fc not called" not fc_was_called
check "typo'd gate: names the value" grep -q "unrecognized ONCHAIN_CONFIG_ENABLED='flase'" <<< "$OUT"
setup
run_script ONCHAIN_CONFIG_ENABLED=FALSE
check "uppercase FALSE: exits nonzero (pinned as deliberate)" [ "$RC" -ne 0 ]
setup
run_script ONCHAIN_CONFIG_ENABLED=off
check "off: exits nonzero (pinned as deliberate)" [ "$RC" -ne 0 ]

#### read node (file) → no-op
setup
sed -i.bak 's/read_node = false/read_node = true/' "$DIR/config.toml"
run_script
check "read node: exits 0" [ "$RC" -eq 0 ]
check "read node: fc not called" not fc_was_called

#### read node (env overlay wins over file)
setup
run_script SNAPCHAIN_READ_NODE=true
check "read node via env overlay: exits 0" [ "$RC" -eq 0 ]
check "read node via env overlay: fc not called" not fc_was_called

#### success path: merge lands, cache written with tight perms
setup
run_script ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "success: exits 0" [ "$RC" -eq 0 ]
check "success: merge applied" grep -q merged-from-registry "$DIR/config.toml"
check "success: cache matches config" cmp -s "$DIR/config.toml" "$DIR/cache/config.toml"
perms="$(stat -c '%A' "$DIR/cache/config.toml" 2>/dev/null \
    || stat -f '%Sp' "$DIR/cache/config.toml")" # GNU stat, BSD fallback
check "success: cache not world-readable ($perms)" [ "$perms" = "-rw-------" ]

#### cache volume unwritable → warn but still boot (config is already valid)
setup
mkdir -p "$DIR/rocache"
chmod 555 "$DIR/rocache"
run_script ONCHAIN_CONFIG_CACHE="$DIR/rocache/sub/config.toml"
chmod 755 "$DIR/rocache"
check "unwritable cache: exits 0" [ "$RC" -eq 0 ]
check "unwritable cache: merge still applied" grep -q merged-from-registry "$DIR/config.toml"
check "unwritable cache: warns" grep -q "failed to write last-known-good" <<< "$OUT"

#### argument construction
setup
run_script ONCHAIN_CONFIG_REGISTRY=0xabc ONCHAIN_CONFIG_RPC_URL=https://rpc.example
check "args: network lowercased from config" grep -qx mainnet "$DIR/fc.args"
check "args: registry passed" grep -qx 0xabc "$DIR/fc.args"
check "args: rpc-url passed" grep -qx https://rpc.example "$DIR/fc.args"

setup
sed -i.bak 's/"Mainnet"/"Testnet"/' "$DIR/config.toml"
run_script
check "args: testnet network derived" grep -qx testnet "$DIR/fc.args"
check "args: no --registry when env unset" not grep -qx -- --registry "$DIR/fc.args"
check "args: no --rpc-url when env unset" not grep -qx -- --rpc-url "$DIR/fc.args"

#### env overlay wins for network too
setup
run_script SNAPCHAIN_FC_NETWORK=Testnet
check "args: SNAPCHAIN_FC_NETWORK overrides file" grep -qx testnet "$DIR/fc.args"

#### unparseable network → hard fail
setup
sed -i.bak '/fc_network/d' "$DIR/config.toml"
run_script
check "missing fc_network: exits nonzero" [ "$RC" -ne 0 ]

# Writes a cache fixture whose identity (fc_network, private_key) matches the
# sandbox's fresh config, as a real prior successful pull would have produced.
write_matching_cache() {
    mkdir -p "$DIR/cache"
    printf 'fc_network = "Mainnet"\ncached = "last-known-good"\n' > "$DIR/cache/config.toml"
}

#### pull fails + valid cache → boot from last-known-good
setup
write_matching_cache
run_script FC_STUB_EXIT=7 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "rpc-fail fallback: exits 0" [ "$RC" -eq 0 ]
check "rpc-fail fallback: config restored from cache" grep -q last-known-good "$DIR/config.toml"
check "rpc-fail fallback: logs loudly" \
    grep -q '"level":"WARN".*config pull failed' <<< "$OUT"

#### pull fails + no cache → boot the static config, watcher armed
setup
cp "$DIR/config.toml" "$DIR/pristine.toml"
run_script FC_STUB_EXIT=7 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "rpc-fail no cache: exits 0" [ "$RC" -eq 0 ]
check "rpc-fail no cache: boots the untouched static config" \
    cmp -s "$DIR/config.toml" "$DIR/pristine.toml"
check "rpc-fail no cache: warns loudly" \
    grep -q '"level":"WARN".*booting the static config WITHOUT registry-managed keys' <<< "$OUT"
check "rpc-fail no cache: watcher spawned with empty watermark" \
    grep -qx "watermark: " "$DIR/watch.spawned"

#### pull fails + caching disabled entirely → same static fallback
setup
cp "$DIR/config.toml" "$DIR/pristine.toml"
run_script FC_STUB_EXIT=7
check "rpc-fail cache disabled: exits 0" [ "$RC" -eq 0 ]
check "rpc-fail cache disabled: boots the static config" \
    cmp -s "$DIR/config.toml" "$DIR/pristine.toml"

#### pull succeeds but merge fails validation + valid cache → fall back
setup
write_matching_cache
run_script FC_STUB_APPEND="BAD" ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "bad-merge fallback: exits 0" [ "$RC" -eq 0 ]
check "bad-merge fallback: config restored from cache" grep -q last-known-good "$DIR/config.toml"

#### pull succeeds but merge fails validation + no cache → the static
#### fallback must restore the PRISTINE config, not boot the merged residue
setup
cp "$DIR/config.toml" "$DIR/pristine.toml"
run_script FC_STUB_APPEND="BAD"
check "bad-merge no cache: exits 0" [ "$RC" -eq 0 ]
check "bad-merge no cache: merged residue gone" not grep -q "BAD" "$DIR/config.toml"
check "bad-merge no cache: config is byte-identical to pre-pull" \
    cmp -s "$DIR/config.toml" "$DIR/pristine.toml"

#### pull fails + cache is itself invalid → fall through to static
setup
cp "$DIR/config.toml" "$DIR/pristine.toml"
mkdir -p "$DIR/cache"
printf 'fc_network = "Mainnet"\nBAD\n' > "$DIR/cache/config.toml"
run_script FC_STUB_EXIT=7 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "corrupt cache: exits 0" [ "$RC" -eq 0 ]
check "corrupt cache: complains loudly" grep -q "cached config failed" <<< "$OUT"
check "corrupt cache: static config booted, cache bytes gone" \
    cmp -s "$DIR/config.toml" "$DIR/pristine.toml"

#### pull fails + cache is for a different network → fall through to static
setup
cp "$DIR/config.toml" "$DIR/pristine.toml"
mkdir -p "$DIR/cache"
printf 'fc_network = "Testnet"\ncached = "last-known-good"\n' > "$DIR/cache/config.toml"
run_script FC_STUB_EXIT=7 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "wrong-network cache: exits 0" [ "$RC" -eq 0 ]
check "wrong-network cache: cache NOT restored" not grep -q last-known-good "$DIR/config.toml"
check "wrong-network cache: static config booted" \
    cmp -s "$DIR/config.toml" "$DIR/pristine.toml"
check "wrong-network cache: names the mismatch" \
    grep -q "is for network 'testnet'" <<< "$OUT"

#### pull fails + cache has a different consensus key → fall through to static
setup
echo 'private_key = "current-key"' >> "$DIR/config.toml"
cp "$DIR/config.toml" "$DIR/pristine.toml"
mkdir -p "$DIR/cache"
printf 'fc_network = "Mainnet"\nprivate_key = "rotated-away-key"\ncached = "last-known-good"\n' \
    > "$DIR/cache/config.toml"
run_script FC_STUB_EXIT=7 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "rotated-key cache: exits 0" [ "$RC" -eq 0 ]
check "rotated-key cache: cache NOT restored" not grep -q last-known-good "$DIR/config.toml"
check "rotated-key cache: static config booted with the current key" \
    cmp -s "$DIR/config.toml" "$DIR/pristine.toml"
check "rotated-key cache: key values not printed" not grep -q "rotated-away-key" <<< "$OUT"

#### snapshot unavailable + failed pull → validate config.toml in place and
#### boot it (fc writes atomically, so the file is still the pristine static
#### config; refusing a valid config over a missing COPY would be wrong)
setup
cp "$DIR/config.toml" "$DIR/pristine.toml"
mkdir -p "$DIR/ro-tmp"
chmod 555 "$DIR/ro-tmp"
run_script TMPDIR="$DIR/ro-tmp" FC_STUB_EXIT=7
chmod 755 "$DIR/ro-tmp"
check "no-snapshot last rung: exits 0" [ "$RC" -eq 0 ]
check "no-snapshot last rung: boots the pristine config" \
    cmp -s "$DIR/config.toml" "$DIR/pristine.toml"
check "no-snapshot last rung: says it validated in place" \
    grep -q "validates as written" <<< "$OUT"

#### snapshot unavailable + bad merge → the in-place file is merged residue,
#### which fails validation: refusal is the only honest outcome left
setup
mkdir -p "$DIR/ro-tmp"
chmod 555 "$DIR/ro-tmp"
run_script TMPDIR="$DIR/ro-tmp" FC_STUB_APPEND="BAD"
chmod 755 "$DIR/ro-tmp"
check "no-snapshot bad merge: exits nonzero" [ "$RC" -ne 0 ]
check "no-snapshot bad merge: names the real failure" \
    grep -q "merged config failed --check-config" <<< "$OUT"

#### cache publication leaves no temp files behind
no_stray_cache_files() {
    local f name
    for f in "$DIR/cache"/*; do
        [[ -e "$f" ]] || continue # unexpanded glob on an empty dir
        name="$(basename "$f")"
        case "$name" in
            config.toml | config.toml.prev) ;;
            *) return 1 ;;
        esac
    done
    return 0
}
setup
run_script ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "tmp cleanup on success: only cache artifacts remain" no_stray_cache_files

#### log lines are node-shaped JSON (same fields as tracing_subscriber .json())
setup
run_script
check "logs: JSON lines matching the node's shape" \
    grep -Eq '^\{"timestamp":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z","level":"INFO","fields":\{"message":"pull OK[^"]*"\},"target":"apply-onchain-config"\}$' \
    <<< "$OUT"
check "logs: no bare-text log lines" not grep -q '^\[apply-onchain-config\]' <<< "$OUT"

#### missing config file → hard fail
setup
rm "$DIR/config.toml"
run_script
check "missing config: exits nonzero" [ "$RC" -ne 0 ]

#### C9: bound version reported by the pull becomes the watcher handoff
setup
run_script ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "watermark: pull asked to report the bound version" \
    grep -qx -- --report-version "$DIR/fc.args"
check "watermark: applied version logged" grep -q "applied configVersion 42" <<< "$OUT"
check "watermark: handed to the watcher via env" \
    grep -q "watermark: 42" "$DIR/watch.spawned"
check "watermark: single fc call — no separate unbound version read" \
    [ "$(cat "$DIR/fc.calls")" = pull ]

#### C9: pull reports no usable version → boot anyway, empty handoff
setup
run_script FC_STUB_NO_REPORT=1 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "no-report: exits 0" [ "$RC" -eq 0 ]
check "no-report: merge still applied" grep -q merged-from-registry "$DIR/config.toml"
check "no-report: warns" grep -q "no usable configVersion" <<< "$OUT"
check "no-report: watcher spawned with empty watermark" \
    grep -qx "watermark: " "$DIR/watch.spawned"

#### C9: no cache configured → handoff still works
setup
run_script
check "no cache: watcher still spawned" watcher_spawned
check "no cache: watermark still handed off" grep -q "watermark: 42" "$DIR/watch.spawned"

#### C9: previous known-good rotated to .prev when the cache content changes
setup
write_matching_cache
run_script ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "prev rotation: exits 0" [ "$RC" -eq 0 ]
check "prev rotation: .prev holds the outgoing cache" \
    grep -q last-known-good "$DIR/cache/config.toml.prev"
check "prev rotation: cache holds the fresh merge" \
    cmp -s "$DIR/config.toml" "$DIR/cache/config.toml"

#### C9: unchanged pull → cache not rotated, older .prev preserved
setup
run_script ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"   # seed cache == merged config
echo "older-prev-content" > "$DIR/cache/config.toml.prev"
cp "$DIR/cache/config.toml" "$DIR/config.toml"             # fresh boot writes same config
run_script FC_STUB_NO_APPEND=1 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "no-change pull: exits 0" [ "$RC" -eq 0 ]
check "no-change pull: .prev untouched" \
    grep -qx older-prev-content "$DIR/cache/config.toml.prev"

#### C9: watcher spawn — success path, with the derived network in its env
setup
run_script ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "watcher: spawned on success" watcher_spawned
check "watcher: given the config path" grep -q "argv: config.toml" "$DIR/watch.spawned"
check "watcher: given the derived network" grep -q "network: mainnet" "$DIR/watch.spawned"

#### C9: watcher spawned on the cache-fallback path too (RPC outage recovery)
setup
write_matching_cache
run_script FC_STUB_EXIT=7 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "watcher: spawned on cache fallback" watcher_spawned

#### C9: watcher spawned on the static-fallback path too (fresh node, RPC down)
setup
run_script FC_STUB_EXIT=7
check "watcher: spawned on static fallback" watcher_spawned

#### C9: no watcher when opted out, on read nodes, or on refused boots
setup
run_script ONCHAIN_CONFIG_ENABLED=false
check "watcher: not spawned when opted out" not watcher_spawned
setup
run_script SNAPCHAIN_READ_NODE=true
check "watcher: not spawned on read nodes" not watcher_spawned
setup
echo "BAD" >> "$DIR/config.toml" # static config itself invalid: nothing bootable
run_script
check "watcher: not spawned on a refused boot" not watcher_spawned

#### C9: ONCHAIN_CONFIG_POLL_INTERVAL=0 opts out of the watcher
setup
run_script ONCHAIN_CONFIG_POLL_INTERVAL=0 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "poll disabled: exits 0" [ "$RC" -eq 0 ]
check "poll disabled: watcher not spawned" not watcher_spawned
check "poll disabled: says so" grep -q "only apply on the next restart" <<< "$OUT"

#### C9: missing watcher script → warn but boot
setup
run_script ONCHAIN_CONFIG_WATCH_BIN="$DIR/does-not-exist" \
    ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "missing watcher: exits 0" [ "$RC" -eq 0 ]
check "missing watcher: warns" grep -q "missing or not executable" <<< "$OUT"

#### snapshot hygiene: orphans from hard-killed boots (SIGKILL skips the EXIT
#### trap) are swept at start, and normal paths leave nothing behind — these
#### files carry consensus.private_key
setup
mkdir -p "$DIR/tmp"
touch "$DIR/tmp/onchain-config-static.stale0"
run_script TMPDIR="$DIR/tmp"
check "snapshot sweep: stale orphan removed" \
    not ls "$DIR/tmp"/onchain-config-static.* 2>/dev/null
setup
mkdir -p "$DIR/tmp"
run_script TMPDIR="$DIR/tmp" FC_STUB_EXIT=7
check "snapshot cleanup: nothing left after a fallback boot" \
    not ls "$DIR/tmp"/onchain-config-static.* 2>/dev/null

#### boot-side garbage --report-version content: wider than 18 digits would
#### wrap negative in the watcher's arithmetic — must degrade to an empty
#### watermark, never hand it over
setup
run_script FC_STUB_BOUND_VERSION=9999999999999999999999
check "garbage bound version: exits 0" [ "$RC" -eq 0 ]
check "garbage bound version: warns" grep -q "no usable configVersion" <<< "$OUT"
check "garbage bound version: empty watermark handed off" \
    grep -qx "watermark: " "$DIR/watch.spawned"

#### cache-fallback watermark: fc may have written the version report before
#### validation failed — the handoff must still be empty (nothing was applied)
setup
write_matching_cache
run_script FC_STUB_APPEND="BAD" ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "cache-fallback watermark: empty handoff" \
    grep -qx "watermark: " "$DIR/watch.spawned"

#### fallback-boot counter: climbs across stale boots, cleared by success
setup
run_script FC_STUB_EXIT=7 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
run_script FC_STUB_EXIT=7 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "fallback counter: counts both stale boots" \
    grep -qx 2 "$DIR/cache/config.toml.fallback-boots"
check "fallback counter: logged for monitors" \
    grep -q "fallback boot #2 since the last successful pull" <<< "$OUT"
run_script ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "fallback counter: cleared by a successful pull" \
    not test -f "$DIR/cache/config.toml.fallback-boots"

#### every line this script emits is JSON — fc/snapchain child output is
#### wrapped or dropped, because one bare line breaks JSON-line log parsing
setup
run_script ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "logs: success path emits no bare lines" not grep -qv '^{' <<< "$OUT"
setup
run_script FC_STUB_EXIT=7
check "logs: fallback path emits no bare lines" not grep -qv '^{' <<< "$OUT"

echo
if [[ "$FAILURES" -gt 0 ]]; then
    echo "$FAILURES/$TESTS checks FAILED"
    exit 1
fi
echo "all $TESTS checks passed"
