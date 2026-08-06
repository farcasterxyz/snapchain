#!/bin/bash

# apply-onchain-config.sh — pull registry-managed validator config before boot.
#
# Invoked from the compose entrypoints after the heredoc writes config.toml and
# before `exec $0 $@` hands off to the node. Runs `fc config pull` to merge the
# onchain-managed keys (consensus.validator_sets, gossip.bootstrap_peers,
# gossip.direct_peers) into the freshly written config, validates the result
# with `snapchain --check-config`, and keeps a last-known-good copy so an L1
# RPC outage cannot stop a validator from booting.
#
# Usage: apply-onchain-config.sh [config-path]   (default: config.toml)
#
# Environment:
#   ONCHAIN_CONFIG_ENABLED   "true"/"1" to enable. Anything else no-ops, leaving
#                            the written config untouched. Off by default: until
#                            the registry addresses are baked into fc (NEYN-13022)
#                            a default-on pull would fail fleet-wide with no cache
#                            to fall back on. Also the operator escape hatch — a
#                            mounted or inline config wins verbatim when unset.
#   ONCHAIN_CONFIG_REGISTRY  Registry contract address, passed as --registry.
#                            Optional once fc has baked-in addresses.
#   ONCHAIN_CONFIG_RPC_URL   JSON-RPC URL for the chain the registry lives on,
#                            passed as --rpc-url. Required on testnet (registry
#                            is on Sepolia; the config's l1_rpc_url must stay on
#                            Ethereum mainnet for ENS). On mainnet fc falls back
#                            to l1_rpc_url from the config file.
#   ONCHAIN_CONFIG_CACHE     Path for the last-known-good merged config. Must be
#                            on a volume — config.toml itself lives in the
#                            container's writable layer and is rewritten every
#                            start. Empty disables caching and RPC-failure
#                            fallback (any pull failure is then fatal).
#   FC_BIN, SNAPCHAIN_BIN    Binary locations (default /app/fc, /app/snapchain).
#                            Overridable for non-default layouts (external
#                            validators) and for tests.
#   ONCHAIN_CONFIG_POLL_INTERVAL
#                            Seconds between configVersion() polls in the watch
#                            loop this script spawns on success (default 300;
#                            0 disables the watcher entirely).
#   ONCHAIN_CONFIG_WATCH_BIN Watcher script location (default: sibling
#                            onchain-config-watch.sh). Test hook.
#
# Alongside the cache this script maintains, when ONCHAIN_CONFIG_CACHE is set:
#   $CACHE.version  configVersion() watermark — the counter value read just
#                   before the last successfully applied pull. The watch loop
#                   compares the live counter against it (by inequality) to
#                   gate restarts.
#   $CACHE.prev     the previous known-good config, rotated out when a pull
#                   changes the cache — the manual-rollback artifact that can
#                   restore the old config without an RPC round-trip.
#
# Exit codes: 0 = config ready to boot (pulled, restored from cache, or no-op);
# nonzero = do not boot (entrypoints call this as `apply-onchain-config.sh || exit 1`,
# letting `restart: always` retry rather than starting a node on a bad config).

set -euo pipefail

CONFIG_PATH="${1:-config.toml}"
FC_BIN="${FC_BIN:-/app/fc}"
SNAPCHAIN_BIN="${SNAPCHAIN_BIN:-/app/snapchain}"
CACHE_PATH="${ONCHAIN_CONFIG_CACHE:-}"

# log LEVEL MESSAGE… — one JSON line per call, shaped like the node's own
# tracing_subscriber .json() output ({"timestamp","level","fields":{"message"},
# "target"}), because this stderr shares a docker-logs stream with the node and
# one bare text line breaks JSON-line log parsing downstream. Levels follow
# tracing: INFO/WARN/ERROR. Second-precision timestamps — BSD date (macOS,
# where the tests also run) has no %N.
log() {
    local level="$1" msg
    shift
    msg="$*"
    msg=${msg//\\/\\\\}
    msg=${msg//\"/\\\"}
    printf '{"timestamp":"%s","level":"%s","fields":{"message":"%s"},"target":"apply-onchain-config"}\n' \
        "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$level" "$msg" >&2
}

case "${ONCHAIN_CONFIG_ENABLED:-}" in
    true | 1) ;;
    *)
        log INFO "ONCHAIN_CONFIG_ENABLED not set; leaving $CONFIG_PATH untouched"
        exit 0
        ;;
esac

if [[ ! -f "$CONFIG_PATH" ]]; then
    log ERROR "config file not found: $CONFIG_PATH"
    exit 1
fi

# Read a scalar `key = value` line from a file. The values come from the
# entrypoint heredocs, so plain assignment lines are the only shape we need
# to parse.
file_value() {
    local file="$1" key="$2"
    sed -n "s/^[[:space:]]*${key}[[:space:]]*=[[:space:]]*\"\{0,1\}\([^\"#]*\)\"\{0,1\}.*/\1/p" \
        "$file" | head -1 | tr -d '[:space:]'
}

# Same, from the active config — but the node's loader overlays SNAPCHAIN_*
# env vars over the file, so an env value — when present — is what the node
# will actually run with and takes precedence here too.
config_value() {
    local key="$1" env_name="$2" env_value
    env_value="${!env_name:-}"
    if [[ -n "$env_value" ]]; then
        echo "$env_value"
        return
    fi
    file_value "$CONFIG_PATH" "$key"
}

# Read nodes keep the existing GitHub validators.toml path; the registry
# manages validator config only. read_node is the node's only mode flag and
# defaults to false (validator).
read_node="$(config_value read_node SNAPCHAIN_READ_NODE)"
if [[ "$read_node" == "true" ]]; then
    log INFO "read_node = true; registry manages validator config only — skipping"
    exit 0
fi

# fc cross-checks --network against the config; derive it from the same
# sources the node will use so a mismatch fails inside fc, loudly.
fc_network="$(config_value fc_network SNAPCHAIN_FC_NETWORK)"
network="$(echo "$fc_network" | tr '[:upper:]' '[:lower:]')"
case "$network" in
    mainnet | testnet | devnet) ;;
    *)
        log ERROR "cannot determine network from fc_network='$fc_network' in $CONFIG_PATH"
        exit 1
        ;;
esac

pull_args=(--network "$network" config pull --config "$CONFIG_PATH")
if [[ -n "${ONCHAIN_CONFIG_REGISTRY:-}" ]]; then
    pull_args+=(--registry "$ONCHAIN_CONFIG_REGISTRY")
fi
if [[ -n "${ONCHAIN_CONFIG_RPC_URL:-}" ]]; then
    pull_args+=(--rpc-url "$ONCHAIN_CONFIG_RPC_URL")
fi

check_config() {
    "$SNAPCHAIN_BIN" --config-path "$CONFIG_PATH" --check-config
}

# Hand off to the rollout watcher: a disowned background loop that polls
# configVersion() and triggers a staggered restart when it moves. Spawned from
# here rather than the compose entrypoints so every fleet — Neynar pods and
# public validators alike — gets it by image upgrade alone, with no compose
# changes. The child survives this script's exit (it is reparented to the
# container's init) and dies with the container; each boot starts a fresh one.
# Called only on the success paths: a boot we refused is not a boot to watch.
spawn_watcher() {
    local watch_bin="${ONCHAIN_CONFIG_WATCH_BIN:-$(dirname "$0")/onchain-config-watch.sh}"
    if [[ "${ONCHAIN_CONFIG_POLL_INTERVAL:-300}" == "0" ]]; then
        log "ONCHAIN_CONFIG_POLL_INTERVAL=0; config changes will only apply on the next restart"
        return 0
    fi
    if [[ ! -x "$watch_bin" ]]; then
        log "WARNING: watcher $watch_bin missing or not executable; config changes will only apply on the next restart"
        return 0
    fi
    # The watcher never re-derives the network: this boot's value is what the
    # node actually runs with until the next restart, which replaces the
    # watcher too. stdin closed, stdout/stderr inherited into container logs.
    ONCHAIN_WATCH_NETWORK="$network" "$watch_bin" "$CONFIG_PATH" < /dev/null &
    log "started onchain-config watcher (pid $!)"
}

# Read the mutation counter BEFORE pulling the document: if a registry write
# lands between the two calls, the stored watermark is low — costing one
# harmless extra restart later — never high, which would eat a change. Best
# effort: a boot must not fail because the counter read did, and without a
# cache volume there is nowhere durable to record it (the watcher then keeps
# an in-memory watermark instead).
onchain_version=""
if [[ -n "$CACHE_PATH" ]]; then
    version_args=(--network "$network" config version --config "$CONFIG_PATH")
    if [[ -n "${ONCHAIN_CONFIG_REGISTRY:-}" ]]; then
        version_args+=(--registry "$ONCHAIN_CONFIG_REGISTRY")
    fi
    if [[ -n "${ONCHAIN_CONFIG_RPC_URL:-}" ]]; then
        version_args+=(--rpc-url "$ONCHAIN_CONFIG_RPC_URL")
    fi
    if ! onchain_version="$("$FC_BIN" "${version_args[@]}")" \
        || ! [[ "$onchain_version" =~ ^[0-9]+$ ]]; then
        log "WARNING: could not read configVersion; watermark will not be recorded this boot"
        onchain_version=""
    fi
fi

if "$FC_BIN" "${pull_args[@]}" && check_config; then
    if [[ -n "$CACHE_PATH" ]]; then
        # Rotate the outgoing cache to .prev — the previous known-good that
        # manual rollback restores without an RPC round-trip. The new cache
        # content is exactly $CONFIG_PATH, so compare against that and skip
        # when nothing changed. Best-effort: losing .prev must not stop a
        # boot, and a torn .prev is caught by --check-config at restore time.
        if [[ -f "$CACHE_PATH" ]] && ! cmp -s "$CONFIG_PATH" "$CACHE_PATH"; then
            cp "$CACHE_PATH" "$CACHE_PATH.prev" \
                || log "WARNING: could not rotate previous known-good to $CACHE_PATH.prev"
        fi
        # The merged config contains consensus.private_key: keep the cache
        # non-world-readable (mktemp creates 0600) and publish it with an
        # atomic rename so a crash mid-copy can't leave a truncated
        # last-known-good. The temp name must be unique per invocation:
        # overlapping containers (deploy replacement + autoheal) each have
        # their own PID namespace, so even $$ can collide — with a shared temp
        # name, interleaved writers can publish a spliced file. A cache-write
        # failure (full or read-only volume) must not stop the boot —
        # config.toml is already pulled and validated at this point.
        cache_tmp=""
        trap '[[ -n "$cache_tmp" ]] && rm -f "$cache_tmp"' EXIT
        if { mkdir -p "$(dirname "$CACHE_PATH")" \
            && cache_tmp="$(mktemp "$CACHE_PATH.XXXXXX")" \
            && cp "$CONFIG_PATH" "$cache_tmp" \
            && mv "$cache_tmp" "$CACHE_PATH" && cache_tmp=""; }; then
            log INFO "pull OK; cached last-known-good to $CACHE_PATH"
        else
            log WARN "pull OK but failed to write last-known-good cache to $CACHE_PATH; booting anyway"
        fi
        # Record which counter value this config corresponds to, so the watch
        # loop can gate restarts on the counter moving. Same atomicity rules
        # as the cache; failure costs one no-op restart later, not the boot.
        if [[ -n "$onchain_version" ]]; then
            wm_tmp=""
            trap '[[ -n "$cache_tmp" ]] && rm -f "$cache_tmp"; [[ -n "$wm_tmp" ]] && rm -f "$wm_tmp"' EXIT
            if { wm_tmp="$(mktemp "$CACHE_PATH.version.XXXXXX")" \
                && printf '%s\n' "$onchain_version" > "$wm_tmp" \
                && mv "$wm_tmp" "$CACHE_PATH.version" && wm_tmp=""; }; then
                log "recorded applied configVersion $onchain_version"
            else
                log "WARNING: could not record configVersion watermark; booting anyway"
            fi
        fi
    else
        log INFO "pull OK (no ONCHAIN_CONFIG_CACHE set; skipping last-known-good cache)"
    fi
    spawn_watcher
    exit 0
fi

# Pull or validation failed. Refusing to boot a validator because an RPC
# blipped is worse than running one epoch stale — an L1 outage during a
# rolling restart could otherwise take down several validators at once and
# cost quorum. Boot from the last-known-good instead, loudly.
if [[ -n "$CACHE_PATH" && -f "$CACHE_PATH" ]]; then
    # The cache persists on the host across image rolls, network flips, and
    # key rotations, so it can belong to a different identity than this node
    # — and a stale config for the WRONG node passes --check-config just fine
    # (it is a valid config, just not ours). Restore only when the cache
    # matches the fresh config's network and consensus key; otherwise
    # crash-looping beats booting as somebody else.
    cached_network="$(file_value "$CACHE_PATH" fc_network | tr '[:upper:]' '[:lower:]')"
    if [[ "$cached_network" != "$network" ]]; then
        log ERROR "cache at $CACHE_PATH is for network '$cached_network', this node is '$network'; refusing to boot from it"
        exit 1
    fi
    # Compare, never print: these are consensus signing keys.
    if [[ "$(file_value "$CACHE_PATH" private_key)" != "$(file_value "$CONFIG_PATH" private_key)" ]]; then
        log ERROR "cache at $CACHE_PATH has a different consensus.private_key than the fresh config (key rotated or host repurposed?); refusing to boot from it"
        exit 1
    fi
    log WARN "config pull failed; falling back to last-known-good $CACHE_PATH"
    log WARN "env-derived config changes since that pull will NOT apply this boot"
    cp "$CACHE_PATH" "$CONFIG_PATH"
    if ! check_config; then
        log ERROR "cached config failed --check-config; refusing to boot"
        exit 1
    fi
    log INFO "booting from cached config"
    # The cache's content matches the watermark already on disk (both were
    # written by the last successful pull), so the watcher's inequality check
    # stays correct: once the RPC recovers, a counter that moved since that
    # pull triggers the catch-up restart this boot could not perform.
    spawn_watcher
    exit 0
fi

log ERROR "config pull failed and no last-known-good cache exists; refusing to boot"
exit 1
