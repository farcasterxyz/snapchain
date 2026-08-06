#!/bin/bash

# onchain-config-watch.sh — version-gated, staggered restart loop for
# registry-managed validator config.
#
# Spawned by apply-onchain-config.sh after a successful boot-time pull (or
# cache fallback); one instance per container, dying with it. Periodically
# polls the registry's configVersion() counter — a single cheap eth_call —
# and only when it differs from the recorded watermark does any real work
# happen: pull the document onto a COPY of the running config, validate it
# with `snapchain --check-config`, and byte-compare. A document that fails
# validation is refused loudly with NO restart (fix it onchain); a version
# bump that renders the identical document just advances the watermark. Only
# a validated, genuinely different config triggers a restart — and then only
# inside this node's stagger window.
#
# Stagger: node slots come from `fc config slot` — the node's index in the
# fetched document's validator-key union, so slots are distinct across the
# fleet by construction. Node i may only restart during seconds
# [i*W, (i+1)*W) of a repeating wall-clock cycle of (count+1)*W (the +1 is
# the sentinel slot for nodes not in the document). Wall-clock alignment
# (epoch % cycle) means nodes need no coordination and their differing poll
# phases cannot overlap two restarts, as long as one restart completes
# within one window. The wait re-evaluates in a loop: a further registry
# write while sleeping can move this node's slot, so the restart only fires
# from an evaluation that lands inside its own recomputed window. At most
# one validator is down at a time; worst-case propagation of a change is one
# full cycle plus one poll interval — with 8 validators and defaults,
# ~(8+1)*900s + 300s ≈ 2h20m. Two caveats, accepted and documented: the
# sentinel slot is SHARED by every node absent from the document (removing
# two still-active validators in one registry write can restart them
# together — remove validators one write at a time), and the union spans the
# full set history (matching what a booting node must parse), so keys
# retired-but-retained in history each add one idle window to the cycle.
#
# Restart: `kill -TERM 1`. Every compose file in this repo (and the deployer)
# sets `init: true`, so PID 1 is docker-init, which forwards the TERM to the
# node for a graceful shutdown; the container exits and `restart: always`
# re-runs the entrypoint, whose boot-time pull applies the new config and
# records the new watermark. The watcher never writes config.toml itself —
# boot is the only apply path.
#
# Manual rollback: amend or remove the bad entry onchain — configVersion
# moves FORWARD and nodes converge on the next cycle (the watermark is
# compared for inequality, so a revert is just another change). For a local
# emergency: set ONCHAIN_CONFIG_ENABLED=false and restore
# $ONCHAIN_CONFIG_CACHE.prev (the previous known-good kept by the apply
# script) over the cache; run --check-config before trusting either.
#
# Usage: onchain-config-watch.sh [config-path]   (default: config.toml)
#
# Environment (inherited from apply-onchain-config.sh at spawn):
#   ONCHAIN_WATCH_NETWORK    Network derived by the apply script at boot
#                            (required; the value the node actually runs with
#                            until the next restart replaces this watcher).
#   ONCHAIN_CONFIG_REGISTRY, ONCHAIN_CONFIG_RPC_URL, ONCHAIN_CONFIG_CACHE,
#   FC_BIN, SNAPCHAIN_BIN    Same contract as apply-onchain-config.sh. The
#                            watermark file at $ONCHAIN_CONFIG_CACHE.version is
#                            read once at spawn and then kept in memory (see
#                            read_watermark); with no cache volume it starts at
#                            0 and is re-derived (one pull + byte-compare)
#                            after every restart.
#   ONCHAIN_CONFIG_POLL_INTERVAL   Seconds between polls (default 300).
#   ONCHAIN_CONFIG_STAGGER_WINDOW  Per-node restart window W in seconds
#                            (default 900). Must exceed a full stop + boot +
#                            consensus catch-up, or consecutive slots can
#                            overlap two nodes being down.
#   ONCHAIN_CONFIG_RESTART_CMD     Restart trigger (default "kill -TERM 1").
#                            Escape hatch for layouts without docker-init,
#                            and the test seam.
#   ONCHAIN_CONFIG_WATCH_ONCE      Test seam: run one tick, no initial sleep.

log() {
    echo "[onchain-config-watch] $*" >&2
}

# Pure. Seconds from `now` until the start of this node's next restart
# window. 0 means "restart now": the window is opening (or just opened — see
# grace), or the fleet has at most one validator and there is no one to
# stagger against. Triggering only at a window's START guarantees the full
# window is ahead of the restart, at the cost of waiting out one extra cycle
# when the change is detected mid-window. The grace tenth exists because the
# caller re-checks after sleeping to the computed start: scheduler drift or a
# slow RPC call can land the re-check a few seconds past the boundary, and
# without grace that would cost a full extra cycle every time.
seconds_until_window() {
    local now="$1" index="$2" count="$3" window="$4"
    if [[ "$count" -le 1 ]]; then
        echo 0
        return
    fi
    local cycle=$(((count + 1) * window))
    local start=$((index * window))
    local pos=$((now % cycle))
    local grace=$((window / 10))
    if [[ "$pos" -ge "$start" && "$pos" -le $((start + grace)) ]]; then
        echo 0
        return
    fi
    echo $(((start - pos + cycle) % cycle))
}

fc_version() {
    "$FC_BIN" --network "$NETWORK" config version --config "$CONFIG_PATH" \
        ${REGISTRY_ARGS[@]+"${REGISTRY_ARGS[@]}"}
}

# The watermark is read from disk ONCE, at spawn, and lives in memory after
# that: it means "the version THIS container verified its config against".
# Re-reading the shared file every tick would let an overlapping container
# (deploy replacement, autoheal) advance it under us — this watcher would then
# see counter == watermark and idle forever while running stale config. The
# file is only the boot-to-watcher handoff; we still WRITE it on noop
# advances so the next boot starts from the freshest value.
read_watermark() {
    echo "${WATERMARK_MEM:-0}"
}

# Record the counter value whose rendered document the running config now
# matches. Persisting can fail (full or read-only volume) — that only costs
# re-detection work next tick, never correctness, so warn and carry on with
# the in-memory copy.
write_watermark() {
    WATERMARK_MEM="$1"
    if [[ -n "$WATERMARK_PATH" ]]; then
        local tmp=""
        if ! { tmp="$(mktemp "$WATERMARK_PATH.XXXXXX")" \
            && printf '%s\n' "$1" > "$tmp" \
            && mv "$tmp" "$WATERMARK_PATH"; }; then
            [[ -n "$tmp" ]] && rm -f "$tmp"
            log "WARNING: could not persist watermark $1 to $WATERMARK_PATH"
        fi
    fi
}

# Fetch and judge the current onchain document against the running config.
# Sets EVAL to one of:
#   fail     poll/pull/validation failed, or the document must not be applied
#   current  counter equals the watermark — nothing to do
#   noop     counter moved but the rendered document is byte-identical
#   change   a validated, different document is waiting (EVAL_INDEX/EVAL_COUNT
#            carry the stagger slot)
# and EVAL_VERSION to the counter value read (when readable). Called twice on
# the restart path — once to detect, once after the stagger wait — so an
# onchain revert while we slept (which moves the counter FORWARD, possibly
# back to the running content) downgrades the restart to a watermark update.
evaluate() {
    EVAL=fail
    EVAL_VERSION=""
    EVAL_INDEX=""
    EVAL_COUNT=""
    local v tmp slot_out
    if ! v="$(fc_version)" || ! [[ "$v" =~ ^[0-9]+$ ]]; then
        log "WARNING: configVersion poll failed; will retry in ${POLL_INTERVAL}s"
        return 0
    fi
    EVAL_VERSION="$v"
    if [[ "$v" == "$(read_watermark)" ]]; then
        EVAL=current
        return 0
    fi
    log "configVersion moved ($(read_watermark) -> $v); fetching and validating the new document"
    if ! tmp="$(umask 077 && mktemp "${TMPDIR:-/tmp}/onchain-config-watch.XXXXXX")" \
        || ! cp "$CONFIG_PATH" "$tmp"; then
        log "WARNING: cannot stage a config copy for validation; will retry"
        [[ -n "${tmp:-}" ]] && rm -f "$tmp"
        return 0
    fi
    # Validate on the copy: the running config stays untouched until the
    # restart re-runs the boot-time pull, the one and only apply path.
    if ! "$FC_BIN" --network "$NETWORK" config pull --config "$tmp" \
        ${REGISTRY_ARGS[@]+"${REGISTRY_ARGS[@]}"}; then
        log "ERROR: config pull for version $v failed; NOT restarting; will retry"
        rm -f "$tmp"
        return 0
    fi
    if ! "$SNAPCHAIN_BIN" --config-path "$tmp" --check-config; then
        log "ERROR: version $v renders a config that fails --check-config; NOT restarting (fix it onchain); will retry"
        rm -f "$tmp"
        return 0
    fi
    if cmp -s "$tmp" "$CONFIG_PATH"; then
        rm -f "$tmp"
        EVAL=noop
        return 0
    fi
    if ! slot_out="$("$FC_BIN" config slot --config "$tmp")" \
        || ! [[ "$slot_out" =~ ^[0-9]+\ [0-9]+$ ]]; then
        log "ERROR: cannot compute stagger slot; NOT restarting; will retry"
        rm -f "$tmp"
        return 0
    fi
    rm -f "$tmp"
    EVAL_INDEX="${slot_out% *}"
    EVAL_COUNT="${slot_out#* }"
    EVAL=change
}

tick() {
    evaluate
    case "$EVAL" in
        fail | current) return 0 ;;
        noop)
            log "version $EVAL_VERSION does not change the rendered config; recording watermark without restart"
            write_watermark "$EVAL_VERSION"
            return 0
            ;;
    esac
    # Wait for this node's window — and KEEP re-evaluating until an
    # evaluation lands inside its own recomputed window. The wait can be
    # hours long, and the document defines the slots: a further registry
    # write while we sleep can change this node's index or the fleet count,
    # and restarting in the stale window could collide with the node that
    # owns that window under the new document. Bounded in practice by the
    # owner not writing continuously; a revert while sleeping downgrades to
    # a watermark update.
    local delta
    while :; do
        delta="$(seconds_until_window "$(date +%s)" "$EVAL_INDEX" "$EVAL_COUNT" "$WINDOW")"
        [[ "$delta" -gt 0 ]] || break
        log "version $EVAL_VERSION changes the config; waiting ${delta}s for restart window (slot $EVAL_INDEX of $((EVAL_COUNT + 1)))"
        sleep "$delta"
        evaluate
        case "$EVAL" in
            fail | current)
                log "change no longer applies after the stagger wait; skipping restart"
                return 0
                ;;
            noop)
                log "document reverted to the running config while waiting (version $EVAL_VERSION); recording watermark without restart"
                write_watermark "$EVAL_VERSION"
                return 0
                ;;
        esac
    done
    log "restarting node to apply config version $EVAL_VERSION (slot ${EVAL_INDEX:-?})"
    if bash -c "$RESTART_CMD"; then
        # The container is now shutting down; this watcher dies with it.
        exit 0
    fi
    # A failed trigger must not end the watch (the unconditional exit here
    # was silent watcher death): leave the watermark alone and retry the
    # whole pipeline next tick.
    log "ERROR: restart command failed; will retry next tick"
    return 0
}

# Test harness sources this file for the pure helpers; everything below runs
# only when executed.
if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
    return 0
fi

set -euo pipefail

CONFIG_PATH="${1:-config.toml}"
FC_BIN="${FC_BIN:-/app/fc}"
SNAPCHAIN_BIN="${SNAPCHAIN_BIN:-/app/snapchain}"
POLL_INTERVAL="${ONCHAIN_CONFIG_POLL_INTERVAL:-300}"
WINDOW="${ONCHAIN_CONFIG_STAGGER_WINDOW:-900}"
RESTART_CMD="${ONCHAIN_CONFIG_RESTART_CMD:-kill -TERM 1}"
WATERMARK_PATH="${ONCHAIN_CONFIG_CACHE:+${ONCHAIN_CONFIG_CACHE}.version}"
# Snapshot once at spawn; never re-read (see read_watermark for why).
WATERMARK_MEM="0"
if [[ -n "$WATERMARK_PATH" && -f "$WATERMARK_PATH" ]]; then
    WATERMARK_MEM="$(cat "$WATERMARK_PATH" 2>/dev/null || echo 0)"
fi

if [[ -z "${ONCHAIN_WATCH_NETWORK:-}" ]]; then
    log "ERROR: ONCHAIN_WATCH_NETWORK not set (apply-onchain-config.sh sets it); exiting"
    exit 1
fi
NETWORK="$ONCHAIN_WATCH_NETWORK"

if ! [[ "$POLL_INTERVAL" =~ ^[0-9]+$ && "$WINDOW" =~ ^[0-9]+$ ]] \
    || [[ "$POLL_INTERVAL" -eq 0 || "$WINDOW" -eq 0 ]]; then
    log "ERROR: ONCHAIN_CONFIG_POLL_INTERVAL/ONCHAIN_CONFIG_STAGGER_WINDOW must be positive integers; exiting"
    exit 1
fi

REGISTRY_ARGS=()
if [[ -n "${ONCHAIN_CONFIG_REGISTRY:-}" ]]; then
    REGISTRY_ARGS+=(--registry "$ONCHAIN_CONFIG_REGISTRY")
fi
if [[ -n "${ONCHAIN_CONFIG_RPC_URL:-}" ]]; then
    REGISTRY_ARGS+=(--rpc-url "$ONCHAIN_CONFIG_RPC_URL")
fi

if [[ -n "${ONCHAIN_CONFIG_WATCH_ONCE:-}" ]]; then
    tick
    exit 0
fi

log "watching configVersion every ${POLL_INTERVAL}s (stagger window ${WINDOW}s, watermark ${WATERMARK_PATH:-in-memory})"

# One log line an hour proves the loop is alive without flooding the
# container logs; there is no autoheal for a dead watcher, only this.
HEARTBEAT_TICKS=$((3600 / POLL_INTERVAL))
[[ "$HEARTBEAT_TICKS" -lt 1 ]] && HEARTBEAT_TICKS=1
ticks=0

while true; do
    sleep "$POLL_INTERVAL"
    # `|| log` keeps an unexpected tick failure from killing the loop (it
    # also suppresses set -e inside tick; every step handles its own errors).
    tick || log "WARNING: watcher tick failed unexpectedly; continuing"
    ticks=$((ticks + 1))
    if [[ $((ticks % HEARTBEAT_TICKS)) -eq 0 ]]; then
        log "alive; watermark $(read_watermark)"
    fi
done
