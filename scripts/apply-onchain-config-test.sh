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
# Stub behavior is content-driven: the fc stub records its argv and appends a
# marker to the config (simulating the merge; FC_STUB_APPEND overrides it, and
# FC_STUB_EXIT forces a pull failure), and the snapchain stub fails
# --check-config iff the config contains "BAD".
setup() {
    DIR="$(mktemp -d)"
    cat > "$DIR/fc" <<'EOF'
#!/bin/bash
printf '%s\n' "$@" > "$(dirname "$0")/fc.args"
[[ "${FC_STUB_EXIT:-0}" != 0 ]] && exit "${FC_STUB_EXIT}"
config=""
while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--config" ]]; then config="$2"; fi
    shift
done
# Mirror real fc: it refuses read_node = true configs in write mode (it only
# checks the file — the SNAPCHAIN_READ_NODE env overlay is invisible to it).
if grep -Eq '^[[:space:]]*read_node[[:space:]]*=[[:space:]]*true' "$config"; then
    echo "read_node = true; the registry manages validator config only" >&2
    exit 1
fi
echo "${FC_STUB_APPEND:-# merged-from-registry}" >> "$config"
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
    chmod +x "$DIR/fc" "$DIR/snapchain"
    cat > "$DIR/config.toml" <<'EOF'
fc_network = "Mainnet"
read_node = false
l1_rpc_url = "https://l1.example"
EOF
}

# run_script [env VAR=VAL ...] — invokes the script in the sandbox with the
# stub binaries wired up; captures exit code in RC and output in OUT.
run_script() {
    RC=0
    OUT="$(cd "$DIR" && env \
        FC_BIN="$DIR/fc" SNAPCHAIN_BIN="$DIR/snapchain" \
        ONCHAIN_CONFIG_ENABLED=true "$@" \
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

# `check "desc" not some_command` — bare `!` is shell syntax, not a command,
# so it can't ride through check's "$@".
not() { ! "$@"; }

fc_was_called() { [[ -f "$DIR/fc.args" ]]; }

#### disabled → no-op, fc never runs
setup
run_script ONCHAIN_CONFIG_ENABLED=
check "disabled: exits 0" [ "$RC" -eq 0 ]
check "disabled: fc not called" not fc_was_called
check "disabled: config untouched" not grep -q merged-from-registry "$DIR/config.toml"

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

#### pull fails + no cache → refuse to boot
setup
run_script FC_STUB_EXIT=7 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "rpc-fail no cache: exits nonzero" [ "$RC" -ne 0 ]

#### pull fails + caching disabled entirely → refuse to boot
setup
run_script FC_STUB_EXIT=7
check "rpc-fail cache disabled: exits nonzero" [ "$RC" -ne 0 ]

#### pull succeeds but merge fails validation + valid cache → fall back
setup
write_matching_cache
run_script FC_STUB_APPEND="BAD" ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "bad-merge fallback: exits 0" [ "$RC" -eq 0 ]
check "bad-merge fallback: config restored from cache" grep -q last-known-good "$DIR/config.toml"

#### pull fails + cache is itself invalid → refuse to boot
setup
mkdir -p "$DIR/cache"
printf 'fc_network = "Mainnet"\nBAD\n' > "$DIR/cache/config.toml"
run_script FC_STUB_EXIT=7 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "corrupt cache: exits nonzero" [ "$RC" -ne 0 ]
check "corrupt cache: refuses loudly" grep -q "cached config failed" <<< "$OUT"

#### pull fails + cache is for a different network → refuse to boot
setup
mkdir -p "$DIR/cache"
printf 'fc_network = "Testnet"\ncached = "last-known-good"\n' > "$DIR/cache/config.toml"
run_script FC_STUB_EXIT=7 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "wrong-network cache: exits nonzero" [ "$RC" -ne 0 ]
check "wrong-network cache: config NOT restored" not grep -q last-known-good "$DIR/config.toml"
check "wrong-network cache: names the mismatch" \
    grep -q "is for network 'testnet'" <<< "$OUT"

#### pull fails + cache has a different consensus key → refuse to boot
setup
echo 'private_key = "current-key"' >> "$DIR/config.toml"
mkdir -p "$DIR/cache"
printf 'fc_network = "Mainnet"\nprivate_key = "rotated-away-key"\ncached = "last-known-good"\n' \
    > "$DIR/cache/config.toml"
run_script FC_STUB_EXIT=7 ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "rotated-key cache: exits nonzero" [ "$RC" -ne 0 ]
check "rotated-key cache: config NOT restored" not grep -q last-known-good "$DIR/config.toml"
check "rotated-key cache: key values not printed" not grep -q "rotated-away-key" <<< "$OUT"

#### cache publication leaves no temp files behind
setup
run_script ONCHAIN_CONFIG_CACHE="$DIR/cache/config.toml"
check "tmp cleanup on success: only the cache file remains" \
    [ "$(ls "$DIR/cache")" = "config.toml" ]

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

echo
if [[ "$FAILURES" -gt 0 ]]; then
    echo "$FAILURES/$TESTS checks FAILED"
    exit 1
fi
echo "all $TESTS checks passed"
