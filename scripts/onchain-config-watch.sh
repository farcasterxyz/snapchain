#!/bin/bash

# onchain-config-watch.sh — version-gated, staggered restart loop for
# registry-managed validator config.
#
# Spawned by apply-onchain-config.sh after a successful boot-time pull (or
# cache fallback); one instance per container, dying with it. Periodically
# polls the registry's configVersion() counter — a single cheap eth_call —
# and only when it EXCEEDS the watermark (the counter is strictly monotonic
# onchain, so a lower observation is a stale RPC view, never a change) does
# any real work happen: pull the document onto a COPY of the running config,
# validate it with `snapchain --check-config`, and byte-compare. The pull
# also reports the version bound to the pulled document (block-pinned inside
# fc) — only THAT value ever becomes a watermark, so a load-balanced RPC
# cannot pair a fresh counter with a stale document. A document that fails
# validation is refused loudly with NO restart (fix it onchain); a version
# bump that renders the identical document just advances the watermark. Only
# a validated, genuinely different config triggers a restart — and then only
# inside this node's stagger window.
#
# Stagger: node slots come from `fc config slot` — the node's index in the
# SORTED validator-key union of the fetched document, so slots are distinct
# across the fleet by construction and a write that merely reorders keys
# cannot move anyone's window. Node i may only restart during seconds
# [i*W, (i+1)*W) of a repeating wall-clock cycle of (count+1)*W (the +1 is
# the sentinel slot for nodes not in the document). Wall-clock alignment
# (epoch % cycle) means nodes need no coordination and their differing poll
# phases cannot overlap two restarts, as long as one restart completes
# within one window. The wait re-evaluates in a loop: a further registry
# write while sleeping can move this node's slot, so the restart only fires
# from an evaluation that lands inside its own recomputed window. At most
# one validator is down at a time; worst-case propagation of a change is one
# full cycle plus one poll interval — with 8 validators and defaults,
# ~(8+1)*900s + 300s ≈ 2h20m. Caveats, accepted and documented: the
# sentinel slot is SHARED by every node absent from the document (removing
# two still-active validators in one registry write can restart them
# together — remove validators one write at a time); the union spans the
# full set history (matching what a booting node must parse), so keys
# retired-but-retained in history each add one idle window to the cycle;
# and a write that CHANGES MEMBERSHIP while a validator is mid-restart can
# shift another node's slot into the vacated window — full cross-version
# exclusion needs registry-assigned slots or a registry-level write
# cooldown, so until then: one membership write at a time, and wait a full
# cycle before the next. Two more: wall-clock alignment assumes fleet
# clocks agree to well within the grace tenth (fine under NTP; a validator
# with a drifted clock can restart inside another node's window), and a
# union of exactly one key gives its sole member an immediate restart while
# non-members keep the trailing window (see seconds_until_window).
#
# Restart: `kill -TERM 1`. Every compose file in this repo (and the deployer)
# sets `init: true`, so PID 1 is docker-init, which forwards the TERM to the
# node for a graceful shutdown; the container exits and `restart: always`
# re-runs the entrypoint, whose boot-time pull applies the new config and
# records the new watermark. The watcher never writes config.toml itself —
# boot is the only apply path.
#
# Manual rollback: amend or remove the bad entry onchain — configVersion
# moves FORWARD and nodes converge on the next cycle (a revert is just
# another version above the watermark). For a local
# emergency: set ONCHAIN_CONFIG_ENABLED=false and restore
# $ONCHAIN_CONFIG_CACHE.prev (the previous known-good kept by the apply
# script) over the cache; run --check-config before trusting either.
#
# Usage: onchain-config-watch.sh [config-path]   (default: config.toml)
#
# Tested by the sibling onchain-config-watch-test.sh — self-contained and
# stub-based (fc/snapchain/date/sleep all stubbed), so it runs anywhere bash
# does, but it is NOT wired into CI: run it by hand after any change here.
#
# Environment (set/inherited from apply-onchain-config.sh at spawn):
#   ONCHAIN_WATCH_NETWORK    Network derived by the apply script at boot
#                            (required; the value the node actually runs with
#                            until the next restart replaces this watcher).
#   ONCHAIN_WATCH_WATERMARK  The configVersion bound to the document this
#                            boot applied (from fc's block-pinned
#                            --report-version). Kept in this process only —
#                            never a shared file (see read_watermark). Unset
#                            (fallback boots) -> 0; the first successful pull
#                            re-derives the truth by content comparison.
#   ONCHAIN_CONFIG_REGISTRY, ONCHAIN_CONFIG_RPC_URL,
#   ONCHAIN_CONFIG_ACCEPT_LOCAL_BOOTSTRAP_PEERS,
#   FC_BIN, SNAPCHAIN_BIN    Same contract as apply-onchain-config.sh. The
#                            accept-local-bootstrap-peers knob MUST match the
#                            boot pull's (it is inherited from the spawning
#                            script, so it does): pulling with a different
#                            flag would merge a different document than the
#                            one running, and every peer-only registry write
#                            would look like a change and restart the node
#                            for nothing. Lenient parse here (any non-empty
#                            value other than false/0 enables): the apply
#                            script already refused to boot on garbage.
#   ONCHAIN_CONFIG_POLL_INTERVAL   Seconds between polls (default 300).
#   ONCHAIN_CONFIG_STAGGER_WINDOW  Per-node restart window W in seconds
#                            (default 900). Must exceed a full stop + boot +
#                            consensus catch-up, or consecutive slots can
#                            overlap two nodes being down.
#   ONCHAIN_CONFIG_RESTART_CMD     Restart trigger (default "kill -TERM 1").
#                            Escape hatch for layouts without docker-init,
#                            and the test seam.
#   ONCHAIN_CONFIG_RESTART_GRACE   Seconds to wait after a sent restart
#                            trigger before concluding the node hung in
#                            shutdown and re-arming (default 60).
#   ONCHAIN_CONFIG_WATCH_ONCE      Test seam: run one tick, no initial sleep.

# log LEVEL MESSAGE… — one JSON line per call, shaped like the node's own
# tracing_subscriber .json() output; same rationale and shape as the log()
# in apply-onchain-config.sh (this loop's stderr shares the container's
# docker-logs stream with the node).
log() {
    local level="$1" msg
    shift
    msg="$*"
    msg=${msg//\\/\\\\}
    msg=${msg//\"/\\\"}
    printf '{"timestamp":"%s","level":"%s","fields":{"message":"%s"},"target":"onchain-config-watch"}\n' \
        "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$level" "$msg" >&2
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
    if [[ "$count" -eq 0 ]]; then
        echo 0
        return
    fi
    # The sole member of a one-key union has nobody to stagger against; its
    # sentinels do NOT get the same shortcut — a union shrunk to one key by a
    # registry write must not restart every still-running old validator at
    # once, so non-members keep their trailing window below.
    if [[ "$count" -eq 1 && "$index" -eq 0 ]]; then
        echo 0
        return
    fi
    local cycle=$(((count + 1) * window))
    local start=$((index * window))
    local pos=$((now % cycle))
    # Grace is a tenth of the window, floored at 60s. The post-sleep re-check
    # lands only after real work — up to six 30s-capped RPC round-trips plus
    # two exec'd validations — so a tenth alone can be outrun by a
    # slow-but-alive RPC (e.g. window 120 -> grace 12s vs an 18s evaluation),
    # and every re-check would then miss its window: the change never applies
    # on this node. The floor stays under the 120s window minimum, so a grace
    # zone can never reach into the next slot's window.
    local grace=$((window / 10))
    if [[ "$grace" -lt 60 ]]; then
        grace=60
    fi
    if [[ "$pos" -ge "$start" && "$pos" -le $((start + grace)) ]]; then
        echo 0
        return
    fi
    echo $(((start - pos + cycle) % cycle))
}

# Poll stderr is suppressed: fc's bare-text error lines would break the
# JSON-line log stream every POLL_INTERVAL while the registry is unreachable
# (or not yet deployed). The transition WARN below is the signal; run
# `fc config version` by hand for the underlying error.
fc_version() {
    "$FC_BIN" --network "$NETWORK" config version --config "$CONFIG_PATH" \
        ${REGISTRY_ARGS[@]+"${REGISTRY_ARGS[@]}"} 2> /dev/null
}

# Poll-failure state for transition logging: WARN once when polls start
# failing, INFO once on recovery, quiet in between. A fleet whose registry
# is not deployed yet polls-and-fails forever; logging every tick would
# train operators to ignore the exact WARN that matters after activation.
POLL_FAILING=""

# The watermark means "the version THIS container verified its config
# against". It arrives once, via environment, from the boot that verified it
# (apply-onchain-config.sh reads it from fc's block-bound --report-version)
# and lives only in this process — never in a shared file, which an
# overlapping container (deploy replacement, autoheal) could rewrite under
# us and park a stale survivor forever.
read_watermark() {
    echo "${WATERMARK_MEM:-0}"
}

write_watermark() {
    WATERMARK_MEM="$1"
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
    local v wm tmp bound slot_out cmp_rc
    # Width-bounded: configVersion is a uint256 onchain but bash arithmetic is
    # 64-bit signed, and a wider value would WRAP NEGATIVE in the -le gates
    # below — parking the watcher as "current" forever. 18 digits (< 2^63)
    # keeps every comparison numerically sound; an honest counter increments
    # once per registry write and cannot get near that, so anything wider is a
    # garbage or hostile RPC response, treated as a failed poll.
    if ! v="$(fc_version)" || ! [[ "$v" =~ ^[0-9]{1,18}$ ]]; then
        if [[ -z "$POLL_FAILING" ]]; then
            log WARN "configVersion poll failed; retrying every ${POLL_INTERVAL}s, quiet until it recovers"
            POLL_FAILING=1
        fi
        return 0
    fi
    if [[ -n "$POLL_FAILING" ]]; then
        log INFO "configVersion poll recovered"
        POLL_FAILING=""
    fi
    wm="$(read_watermark)"
    # The counter is strictly monotonic onchain — even a rollback moves it
    # FORWARD — so an observed value at or below the watermark can only be
    # the state we already verified or a stale RPC view (a lagging
    # load-balanced backend), never a change to apply. Numeric comparison
    # keeps such a backend from churning pointless pulls.
    if [[ "$v" -le "$wm" ]]; then
        EVAL=current
        return 0
    fi
    log INFO "configVersion moved ($wm -> $v); fetching and validating the new document"
    if ! tmp="$(umask 077 && mktemp "${TMPDIR:-/tmp}/onchain-config-watch.XXXXXX")" \
        || ! cp "$CONFIG_PATH" "$tmp"; then
        log WARN "cannot stage a config copy for validation; will retry"
        [[ -n "${tmp:-}" ]] && rm -f "$tmp"
        return 0
    fi
    # Validate on the copy: the running config stays untouched until the
    # restart re-runs the boot-time pull, the one and only apply path. The
    # pull reports the version bound (same pinned block, inside fc) to the
    # document it merged — the trigger version above is unbound and must
    # never be recorded as a watermark.
    if ! "$FC_BIN" --network "$NETWORK" config pull --config "$tmp" \
        --report-version "$tmp.version" \
        ${REGISTRY_ARGS[@]+"${REGISTRY_ARGS[@]}"}; then
        log ERROR "config pull for version $v failed; NOT restarting; will retry"
        rm -f "$tmp" "$tmp.version"
        return 0
    fi
    if ! bound="$(tr -d '[:space:]' < "$tmp.version" 2>/dev/null)" \
        || ! [[ "$bound" =~ ^[0-9]{1,18}$ ]]; then
        log ERROR "pull reported no usable configVersion; NOT restarting; will retry"
        rm -f "$tmp" "$tmp.version"
        return 0
    fi
    if [[ "$bound" -le "$wm" ]]; then
        log WARN "pull landed on a backend at version $bound, at or below watermark $wm (stale RPC view); will retry"
        rm -f "$tmp" "$tmp.version"
        return 0
    fi
    EVAL_VERSION="$bound"
    if ! "$SNAPCHAIN_BIN" --config-path "$tmp" --check-config > /dev/null; then
        log ERROR "version $bound renders a config that fails --check-config; NOT restarting (fix it onchain); will retry"
        rm -f "$tmp" "$tmp.version"
        return 0
    fi
    # Three-way compare: identical is a no-op, different is a change, and a
    # comparison ERROR (cmp exit >= 2: I/O failure, vanished file) must not
    # masquerade as "different" and restart a validator on no evidence.
    cmp_rc=0
    cmp -s "$tmp" "$CONFIG_PATH" || cmp_rc=$?
    if [[ "$cmp_rc" -eq 0 ]]; then
        rm -f "$tmp" "$tmp.version"
        EVAL=noop
        return 0
    elif [[ "$cmp_rc" -ne 1 ]]; then
        log WARN "cannot compare the pulled config against the running one (cmp exit $cmp_rc); NOT restarting; will retry"
        rm -f "$tmp" "$tmp.version"
        return 0
    fi
    if ! slot_out="$("$FC_BIN" config slot --config "$tmp")" \
        || ! [[ "$slot_out" =~ ^[0-9]+\ [0-9]+$ ]]; then
        log ERROR "cannot compute stagger slot; NOT restarting; will retry"
        rm -f "$tmp" "$tmp.version"
        return 0
    fi
    rm -f "$tmp" "$tmp.version"
    EVAL_INDEX="${slot_out% *}"
    EVAL_COUNT="${slot_out#* }"
    EVAL=change
}

tick() {
    evaluate
    case "$EVAL" in
        fail | current) return 0 ;;
        noop)
            log INFO "version $EVAL_VERSION does not change the rendered config; recording watermark without restart"
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
    local delta now
    while :; do
        # A failed `date` must not fall through as epoch 0 — that computes
        # slot 0's window as "now" and restarts outside the real window.
        if ! now="$(date +%s)" || ! [[ "$now" =~ ^[0-9]+$ ]]; then
            log WARN "cannot read the clock; NOT restarting; will retry"
            return 0
        fi
        delta="$(seconds_until_window "$now" "$EVAL_INDEX" "$EVAL_COUNT" "$WINDOW")"
        [[ "$delta" -gt 0 ]] || break
        log INFO "version $EVAL_VERSION changes the config; waiting ${delta}s for restart window (slot $EVAL_INDEX of $((EVAL_COUNT + 1)))"
        sleep "$delta"
        evaluate
        case "$EVAL" in
            fail | current)
                log INFO "change no longer applies after the stagger wait; skipping restart"
                return 0
                ;;
            noop)
                log INFO "document reverted to the running config while waiting (version $EVAL_VERSION); recording watermark without restart"
                write_watermark "$EVAL_VERSION"
                return 0
                ;;
        esac
    done
    log INFO "restarting node to apply config version $EVAL_VERSION (slot ${EVAL_INDEX:-?})"
    if bash -c "$RESTART_CMD"; then
        # Success means the signal was SENT, not that the container is dying:
        # a node hung in graceful shutdown leaves PID 1 up, and exiting here
        # would end the watch with the change unapplied — the same silent
        # watcher death the failure branch below guards against. If shutdown
        # proceeds, the container (and this sleep) never gets past this line;
        # if we are still running afterwards, re-arm and retry next tick.
        sleep "$RESTART_GRACE"
        log ERROR "still running ${RESTART_GRACE}s after the restart trigger — node hung in shutdown?; will retry next tick"
        return 0
    fi
    # A failed trigger must not end the watch (the unconditional exit here
    # was silent watcher death): leave the watermark alone and retry the
    # whole pipeline next tick.
    log ERROR "restart command failed; will retry next tick"
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
RESTART_GRACE="${ONCHAIN_CONFIG_RESTART_GRACE:-60}"
if ! [[ "$RESTART_GRACE" =~ ^[0-9]+$ ]] || [[ "$RESTART_GRACE" -eq 0 ]]; then
    log ERROR "ONCHAIN_CONFIG_RESTART_GRACE must be a positive integer; exiting"
    exit 1
fi
# Handed off by the boot that verified it (see read_watermark). Unset or
# garbage -> 0: the first successful pull re-derives the truth by content
# comparison, at the cost of one pull. Width-bounded like every version
# parse: wider than 18 digits wraps bash's signed-64-bit arithmetic.
WATERMARK_MEM="${ONCHAIN_WATCH_WATERMARK:-0}"
[[ "$WATERMARK_MEM" =~ ^[0-9]{1,18}$ ]] || WATERMARK_MEM="0"

if [[ -z "${ONCHAIN_WATCH_NETWORK:-}" ]]; then
    log ERROR "ONCHAIN_WATCH_NETWORK not set (apply-onchain-config.sh sets it); exiting"
    exit 1
fi
NETWORK="$ONCHAIN_WATCH_NETWORK"

if ! [[ "$POLL_INTERVAL" =~ ^[0-9]+$ && "$WINDOW" =~ ^[0-9]+$ ]] \
    || [[ "$POLL_INTERVAL" -eq 0 || "$WINDOW" -eq 0 ]]; then
    log ERROR "ONCHAIN_CONFIG_POLL_INTERVAL/ONCHAIN_CONFIG_STAGGER_WINDOW must be positive integers; exiting"
    exit 1
fi
# A restart fires only when a FRESH evaluation lands inside [start,
# start + window/10], and an evaluation costs an RPC round trip plus
# check-config — seconds. A grace tenth smaller than that latency can never
# be hit: the loop overshoots every cycle, re-pulls, and re-sleeps — a
# rollout that silently never lands. Refuse windows whose grace is below a
# margin over worst-case evaluation latency instead of livelocking.
if [[ "$WINDOW" -lt 120 ]]; then
    log ERROR "ONCHAIN_CONFIG_STAGGER_WINDOW must be >= 120 (the window must outlast a full node stop + boot, and must exceed the 60s grace floor); exiting"
    exit 1
fi

REGISTRY_ARGS=()
if [[ -n "${ONCHAIN_CONFIG_REGISTRY:-}" ]]; then
    REGISTRY_ARGS+=(--registry "$ONCHAIN_CONFIG_REGISTRY")
fi
if [[ -n "${ONCHAIN_CONFIG_RPC_URL:-}" ]]; then
    REGISTRY_ARGS+=(--rpc-url "$ONCHAIN_CONFIG_RPC_URL")
fi
case "${ONCHAIN_CONFIG_ACCEPT_LOCAL_BOOTSTRAP_PEERS:-}" in
    "" | false | 0) ;;
    *) REGISTRY_ARGS+=(--accept-local-bootstrap-peers-config) ;;
esac

if [[ -n "${ONCHAIN_CONFIG_WATCH_ONCE:-}" ]]; then
    # Same error semantics as the production loop below: `|| log` suppresses
    # set -e inside tick, so the harness exercises exactly the failure mode
    # production runs with — an unguarded failing command falls through here
    # just as it would in the loop, instead of aborting only under test.
    tick || log WARN "watcher tick failed unexpectedly; continuing"
    exit 0
fi

log INFO "watching configVersion every ${POLL_INTERVAL}s (stagger window ${WINDOW}s, watermark ${WATERMARK_MEM})"

# One log line an hour proves the loop is alive without flooding the
# container logs; there is no autoheal for a dead watcher, only this.
# Caveat for anyone alerting on heartbeat absence: tick blocks inside the
# stagger wait, which can last a full cycle — the heartbeat goes quiet
# during a rollout, and the last "waiting Ns" line is the liveness signal.
HEARTBEAT_TICKS=$((3600 / POLL_INTERVAL))
[[ "$HEARTBEAT_TICKS" -lt 1 ]] && HEARTBEAT_TICKS=1
ticks=0

while true; do
    sleep "$POLL_INTERVAL"
    # `|| log` keeps an unexpected tick failure from killing the loop (it
    # also suppresses set -e inside tick; every step handles its own errors).
    tick || log WARN "watcher tick failed unexpectedly; continuing"
    ticks=$((ticks + 1))
    if [[ $((ticks % HEARTBEAT_TICKS)) -eq 0 ]]; then
        log INFO "alive; watermark $(read_watermark)"
    fi
done
