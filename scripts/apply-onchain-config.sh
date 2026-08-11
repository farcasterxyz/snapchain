#!/bin/bash

# apply-onchain-config.sh — pull registry-managed validator config before boot.
#
# Invoked from the compose entrypoints after the heredoc writes config.toml and
# before `exec $0 $@` hands off to the node. Runs `fc config pull` to merge the
# onchain-managed keys (consensus.validator_sets, gossip.bootstrap_peers,
# gossip.direct_peers) into the freshly written config and validates the result
# with `snapchain --check-config`. On failure it falls back, loudly, down a
# ladder that always prefers booting over refusing: the last-known-good cache
# first, then the static config the entrypoint just wrote — so neither an L1
# RPC outage nor a first boot with no cache can stop a validator from starting.
#
# Usage: apply-onchain-config.sh [config-path]   (default: config.toml)
#
# Environment:
#   ONCHAIN_CONFIG_ENABLED   On by default (unset or empty runs the pull), so
#                            the fleet converges by image upgrade alone.
#                            "false"/"0" opts out: the script no-ops and a
#                            mounted or inline config wins verbatim — the
#                            operator escape hatch the rollback runbook points
#                            at. "true"/"1" also runs (the old opt-in spelling).
#                            Any other value refuses to boot: a typo'd kill
#                            switch must fail loudly, not silently enable.
#                            Until the registry addresses are baked into fc,
#                            every pull fails and boots fall through to the
#                            static config, so default-on ships dormant and
#                            self-activates when the addresses land.
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
#                            start. Empty disables caching (pull failures then
#                            fall straight through to the static config).
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
#   $CACHE.prev            the previous known-good config, rotated out when a
#                          pull changes the cache — the manual-rollback
#                          artifact that can restore the old config without an
#                          RPC round-trip.
#   $CACHE.fallback-boots  count of consecutive boots that did NOT apply a
#                          fresh pull; cleared by the next successful one.
#                          The monitorable stale-fleet signal: alert on it
#                          existing/climbing, because a node running stale is
#                          otherwise indistinguishable from a healthy one.
#
# The rollout watermark (the configVersion() bound to the applied document,
# reported by `fc config pull --report-version` from block-pinned reads) is
# handed to the spawned watcher through its ENVIRONMENT, never through a
# shared file: overlapping containers (deploy replacement, autoheal) racing
# on a shared watermark file could pair one container's cache with another's
# version and park a stale survivor forever. A fallback boot hands off an
# empty watermark (-> 0), which the watcher self-heals by content comparison
# on its first successful pull.
#
# Exit codes: 0 = config ready to boot (pulled, restored from cache, static
# fallback, or no-op); nonzero = do not boot (entrypoints call this as
# `apply-onchain-config.sh || exit 1`, letting `restart: always` retry rather
# than starting a node on a bad config). With the fallback ladder, nonzero is
# reserved for configs nothing can fix locally: a static config that fails
# --check-config, or a garbage ONCHAIN_CONFIG_ENABLED value.

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
    "" | true | 1) ;;
    false | 0)
        log INFO "ONCHAIN_CONFIG_ENABLED=${ONCHAIN_CONFIG_ENABLED}; leaving $CONFIG_PATH untouched"
        exit 0
        ;;
    *)
        log ERROR "unrecognized ONCHAIN_CONFIG_ENABLED='${ONCHAIN_CONFIG_ENABLED}' (use true/1 or false/0); refusing to boot rather than guess"
        exit 1
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

# stdout dropped ("config OK" is a bare-text line in a JSON log stream);
# stderr kept — it only speaks on failure, where the diagnostics matter.
# Loader-deep only: a config that passes can still fail later in startup
# (e.g. a malformed consensus key panics post-exec), so "validated" here
# means "parses and loads", not "boots".
check_config() {
    "$SNAPCHAIN_BIN" --config-path "$CONFIG_PATH" --check-config > /dev/null
}

# Re-emit captured fc output as JSON log lines at the given level — fc
# writes bare text, and one bare line in the shared docker-logs stream
# breaks JSON-line parsing downstream.
emit_fc_output() {
    local level="$1" line
    [[ -n "$fc_out" ]] || return 0
    while IFS= read -r line; do
        [[ -n "$line" ]] && log "$level" "fc: $line"
    done <<< "$fc_out"
    return 0
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
    # The watcher never re-derives the network or re-reads shared state: this
    # boot's network is what the node runs with until the next restart, and
    # the watermark is the version bound to the document THIS boot applied
    # (empty on fallback boots -> the watcher starts at 0 and self-heals by
    # content comparison). stdin closed, stdout/stderr inherited into
    # container logs.
    ONCHAIN_WATCH_NETWORK="$network" \
        ONCHAIN_WATCH_WATERMARK="$onchain_version" \
        "$watch_bin" "$CONFIG_PATH" < /dev/null &
    log "started onchain-config watcher (pid $!)"
}

# Fallback-boot counter at $CACHE_PATH.fallback-boots: incremented on every
# boot that did not apply a fresh pull, cleared by the next successful one.
# A node running stale is otherwise indistinguishable from a healthy one in
# every dashboard — this file (and its WARN line) is the monitorable signal,
# and a value that keeps climbing is the flap/outage tripwire. Best-effort:
# accounting must never stop a boot.
record_fallback_boot() {
    [[ -n "$CACHE_PATH" ]] || return 0
    local f="$CACHE_PATH.fallback-boots" n
    n="$(tr -d '[:space:]' < "$f" 2> /dev/null || true)"
    [[ "$n" =~ ^[0-9]{1,9}$ ]] || n=0
    n=$((n + 1))
    { mkdir -p "$(dirname "$f")" && printf '%s\n' "$n" > "$f"; } 2> /dev/null \
        || log WARN "could not record the fallback-boot counter at $f"
    log WARN "fallback boot #$n since the last successful pull"
    return 0
}

clear_fallback_counter() {
    [[ -n "$CACHE_PATH" ]] || return 0
    rm -f "$CACHE_PATH.fallback-boots" 2> /dev/null || true
    return 0
}

# The pull reports the configVersion() bound to the very document it merged
# (both reads pinned to one block hash inside fc) — the only value safe to
# hand the watcher as its watermark. Best-effort: a boot must not fail over
# watermark plumbing; an unset watermark just costs the watcher one catch-up
# content comparison.
onchain_version=""
version_report=""
cache_tmp=""
static_tmp=""
# Invoked via the EXIT trap only.
# shellcheck disable=SC2329
cleanup_temps() {
    [[ -n "$cache_tmp" ]] && rm -f "$cache_tmp"
    [[ -n "$version_report" ]] && rm -f "$version_report"
    [[ -n "$static_tmp" ]] && rm -f "$static_tmp"
    return 0
}
trap cleanup_temps EXIT
if version_report="$(mktemp "${TMPDIR:-/tmp}/onchain-config-version.XXXXXX")"; then
    pull_args+=(--report-version "$version_report")
else
    version_report=""
    log "WARNING: cannot create version-report temp file; watcher will start with an unset watermark"
fi
# Sweep snapshots orphaned by hard kills first: SIGKILL (OOM, docker kill)
# skips the EXIT trap, and these carry consensus.private_key — nothing else
# cleans them up. /tmp is container-private, so the glob cannot touch
# another node's files.
rm -f "${TMPDIR:-/tmp}"/onchain-config-static.*

# Snapshot the entrypoint-written config before fc touches it. It must be an
# actual copy — a pull that succeeds but fails --check-config leaves MERGED
# content in $CONFIG_PATH, so "boot what we started with" cannot be
# reconstructed from the file itself. If the snapshot cannot be taken the
# ladder still has its last rung: validating $CONFIG_PATH as it stands.
if ! { static_tmp="$(mktemp "${TMPDIR:-/tmp}/onchain-config-static.XXXXXX")" \
    && cp "$CONFIG_PATH" "$static_tmp"; }; then
    [[ -n "$static_tmp" ]] && rm -f "$static_tmp"
    static_tmp=""
    log WARN "cannot snapshot the static config; will validate config.toml in place if the pull fails"
fi

# Pull and validate as separate steps so the failure messages below can
# tell the truth: "pull failed" sends the operator to RPC health, "merged
# config failed validation" sends them to the registry document — chasing
# the wrong one wastes the incident.
pull_ok=""
fc_out=""
if fc_out="$("$FC_BIN" "${pull_args[@]}" 2>&1)"; then
    pull_ok=1
    emit_fc_output INFO
else
    emit_fc_output WARN
fi

if [[ -n "$pull_ok" ]] && check_config; then
    if [[ -n "$version_report" ]]; then
        onchain_version="$(tr -d '[:space:]' < "$version_report" 2>/dev/null || true)"
        # Width-bounded to 18 digits: the watcher compares this in bash's
        # signed-64-bit arithmetic, where a wider value wraps negative.
        if [[ "$onchain_version" =~ ^[0-9]{1,18}$ ]]; then
            log "applied configVersion $onchain_version"
        else
            log "WARNING: pull reported no usable configVersion; watcher will start with an unset watermark"
            onchain_version=""
        fi
    fi
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
        if { mkdir -p "$(dirname "$CACHE_PATH")" \
            && cache_tmp="$(mktemp "$CACHE_PATH.XXXXXX")" \
            && cp "$CONFIG_PATH" "$cache_tmp" \
            && mv "$cache_tmp" "$CACHE_PATH" && cache_tmp=""; }; then
            log INFO "pull OK; cached last-known-good to $CACHE_PATH"
        else
            log WARN "pull OK but failed to write last-known-good cache to $CACHE_PATH; booting anyway"
        fi
    else
        log INFO "pull OK (no ONCHAIN_CONFIG_CACHE set; skipping last-known-good cache)"
    fi
    clear_fallback_counter
    spawn_watcher
    exit 0
fi

# Classify the failure once; every message below carries it. A pull that
# succeeded but rendered an invalid merge is the registry document's fault
# (or this binary's) — fix it onchain, not in the RPC layer.
if [[ -n "$pull_ok" ]]; then
    failure_reason="merged config failed --check-config"
    log ERROR "pull succeeded but the merged config failed --check-config; the registry document (or this binary) is at fault — falling back"
else
    failure_reason="config pull failed"
fi

# Pull or validation failed. Refusing to boot a validator because an RPC
# blipped is worse than running one epoch stale — an L1 outage during a
# rolling restart could otherwise take down several validators at once and
# cost quorum. Fall back, loudly: first to the last-known-good cache (the
# most recent registry document this node validated), then to the static
# config the entrypoint wrote. Every fallback boot arms the watcher, so a
# node that comes up stale converges on the watcher's first successful pull.

# Restore the last-known-good cache into $CONFIG_PATH, or return nonzero to
# fall through to the static config. The cache persists on the host across
# image rolls, network flips, and key rotations, so it can belong to a
# different identity than this node — and a stale config for the WRONG node
# passes --check-config just fine (it is a valid config, just not ours).
# Restore only when the cache matches the fresh config's network and
# consensus key: the guards exist to stop this node booting as somebody
# else, not to stop it booting as itself from the static fallback.
try_cache_boot() {
    [[ -n "$CACHE_PATH" && -f "$CACHE_PATH" ]] || return 1
    local cached_network
    cached_network="$(file_value "$CACHE_PATH" fc_network | tr '[:upper:]' '[:lower:]')"
    if [[ "$cached_network" != "$network" ]]; then
        log WARN "cache at $CACHE_PATH is for network '$cached_network', this node is '$network'; not booting from it"
        return 1
    fi
    # Compare, never print: these are consensus signing keys. Known limit:
    # this reads the FILE values only, so a deployment supplying the key via
    # the SNAPCHAIN_CONSENSUS__PRIVATE_KEY overlay (no private_key line in
    # either file) passes vacuously — the guard degrades to the network check
    # above. Every compose path in this repo and the deployer writes the key
    # into config.toml; revisit if an env-keyed layout ever runs this script.
    if [[ "$(file_value "$CACHE_PATH" private_key)" != "$(file_value "$CONFIG_PATH" private_key)" ]]; then
        log WARN "cache at $CACHE_PATH has a different consensus.private_key than the fresh config (key rotated or host repurposed?); not booting from it"
        return 1
    fi
    log WARN "$failure_reason; falling back to last-known-good $CACHE_PATH"
    log WARN "env-derived config changes since that pull will NOT apply this boot"
    cp "$CACHE_PATH" "$CONFIG_PATH" || return 1
    if ! check_config; then
        log WARN "cached config failed --check-config; not booting from it"
        return 1
    fi
    return 0
}

if try_cache_boot; then
    log INFO "booting from cached config"
    record_fallback_boot
    # This boot verified no watermark (the pull failed), so spawn_watcher
    # hands off an empty one and the watcher starts at 0: its first
    # successful pull re-derives the truth by content comparison — identical
    # content just advances the watermark, while a counter that moved since
    # the cached pull triggers the catch-up restart this boot could not
    # perform.
    spawn_watcher
    exit 0
fi

# No usable cache — a fresh node's first boot, or a cache that failed its
# guards (a failed guard may have left cache bytes in $CONFIG_PATH, which is
# why this restores from the snapshot rather than trusting the file). Boot
# the static config the entrypoint just wrote: byte-for-byte what this node
# would run without the registry feature. Refusing here would make L1 RPC
# reachability a boot prerequisite for every fresh validator — and would
# crash-loop the whole fleet while fc still lacks baked-in registry
# addresses. The entrypoints still ship complete static
# validator config today, so this boots a working node; revisit this
# fallback (tighten toward refusal) if the static validator sets are ever
# removed from the entrypoints, because at that point the registry is
# load-bearing and a registry-less boot is not a working validator.
if [[ -n "$static_tmp" ]] && cp "$static_tmp" "$CONFIG_PATH" && check_config; then
    log WARN "$failure_reason and no usable last-known-good cache; booting the static config WITHOUT registry-managed keys"
    record_fallback_boot
    spawn_watcher
    exit 0
fi

# Last rung: no snapshot (unwritable TMPDIR — the disk pressure that often
# accompanies the RPC failures that land us here). fc writes config.toml
# atomically, so after a FAILED pull the file is still the pristine static
# config; after a successful pull whose merge failed validation it is the
# merged content, which this same check just rejected and rejects again.
# Validating in place therefore boots exactly the configs the snapshot rung
# would have, without depending on the copy.
if check_config; then
    log WARN "$failure_reason; no snapshot available but $CONFIG_PATH validates as written; booting it WITHOUT registry-managed keys"
    record_fallback_boot
    spawn_watcher
    exit 0
fi

log ERROR "$failure_reason and no fallback validates; refusing to boot"
exit 1
