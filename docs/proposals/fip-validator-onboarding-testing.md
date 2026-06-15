# FIP: Requirements for Onboarding New Farcaster Validators

> **Note:** Written in the snapchain repo for review; intended for posting to
> [farcasterxyz/protocol Discussions](https://github.com/farcasterxyz/protocol/discussions). Code
> references point at the snapchain repository.

| Field | Value |
|-------|-------|
| **Status** | Draft |
| **Author** | Neynar (manan@neynar.com) |
| **Created** | 2026-06-09 |
| **Discussion** | TBD |

## Summary

Farcaster consensus is BFT (Malachite) with **equal voting power per validator** and a **>2/3
quorum**. On today's ~6-validator shards the fault budget is **1**, so a single faulty, slow, or
**divergent** validator can halt a shard. Adding a validator is a one-line `effective_at` edit to
[`validators.toml`](../../validators.toml) — cheap to do, expensive to get wrong.

The hard case, and the **core of this proposal**, is a validator running a **different codebase**
than snapchain (a fork or an independent reimplementation). Such a client must be **bit-for-bit
deterministic** with the rest of the network or it breaks consensus. This document defines the
**requirements an alternative client must meet**, how they are **verified**, and the **rollout** for
admitting one. Deployment requirements that apply to *any* new validator (including same-binary
expansion into a new geo/datacenter) are covered in one section near the end.

## Motivation

Two things are happening:

1. **Near-term:** expansion of the validator set into a new **geo/datacenter** (latency-sensitive —
   see [Deployment requirements](#deployment-requirements-all-new-validators)).
2. **Structurally:** interest in **client diversity** — a validator built from a different codebase.
   "Client" here means the **consensus node** (the validator/hub binary), **not** an app.

There is no written, testable bar today. This FIP provides one, scaled to risk.

**Current phase.** Validator membership is governed **manually** — there are no native protocol
incentives or permissionless staking yet, so the set is maintained by mutual agreement among
operators via `validators.toml`. Until that changes, admission and removal are partly a **trust and
collaboration** decision, not only a technical one (see
[Operator requirements](#operator-requirements-all-validators)).

### Risk tiers

| Tier | Candidate | Added requirement over the tier above |
|------|-----------|----------------------------------------|
| **A** | Stock snapchain, **new operator / geo** | Deployment, networking, custody, ops only. |
| **B** | **Fork** of snapchain | Everything in A **+** the full determinism contract below (forks drift). |
| **C** | **Independent reimplementation** | Everything in B, with no shared code to fall back on — the determinism contract is the *entire* burden. |

Tiers are cumulative. The bulk of this document is the **B/C determinism contract**; Tier A only
needs the Deployment + Operational sections.

---

## Requirements for alternative client implementations

A non-snapchain validator (Tier B/C) must satisfy **all** of the following before it is added to
`validators.toml` on mainnet. Each is a hard gate, not a guideline.

### R1 — Consensus determinism (the core gate)

Consensus signs over **encoded bytes** and hashes headers with **BLAKE3**
([`snapchain_codec.rs`](../../src/consensus/malachite/snapchain_codec.rs),
[`blocks.proto`](../../proto/definitions/blocks.proto)). Any divergence in encoding, hashing, or
state computation produces different signed bytes or block hashes → votes are rejected → the shard
stalls (or, if a quorum diverges together, forks). The client must produce, for every input in the
shared conformance corpus:

- **byte-exact protobuf encoding** of messages, blocks, votes, and proposals;
- the **same BLAKE3 header hash**;
- a **valid, verifiable Ed25519 signature** over the canonical bytes;
- the **same post-state merkle/state root**.

**Gate:** 100% byte-exact match against the shared conformance vectors (see
[Verification](#how-requirements-are-verified)).

### R2 — Validation parity

The client must make the **identical accept/reject decision** on every message as the reference
validation rules ([`src/core/validations/`](../../src/core/validations/)). One client accepting what
another rejects produces non-deterministic block contents and breaks consensus.

### R3 — Protocol compatibility & versioning

- The client must declare and track a specific **protocol-compatibility version**: the proto
  definitions, the Malachite consensus wire format/version, and the message-validation rule set it
  conforms to.
- Consensus-critical changes upstream (encoding, hashing, validation, timing) must be adopted within
  a defined compatibility window. A client that falls behind is removed until it re-conforms.

### R4 — Byzantine safety & crash recovery

- **No equivocation:** the client must never sign two values at the same height/round, even across a
  crash/restart. Signer design must make double-signing impossible (e.g. height/round high-water
  mark persisted before signing).
- **Crash recovery:** on restart the client must replay its write-ahead state and resume voting
  consistently with its pre-crash self.

### R5 — Networking conformance

The client must speak the existing p2p protocol: **libp2p gossipsub over QUIC**, the consensus /
mempool / decided-values / contact-info topics, and the contact-info exchange used for peer
discovery and mesh formation ([`gossip.rs`](../../src/network/gossip.rs)). It must form and hold the
gossip mesh with existing validators.

### R6 — Operational & custody

- **Key custody:** validator Ed25519 key in an HSM / managed secret store; no key reuse across
  environments; equivocation-safe signer (R4).
- **Monitoring:** block-height lag, round count, message-rejection counts, and peer/mesh health
  exported.
- **Auditability:** because a divergent binary is a consensus-safety dependency for *every*
  operator, the client should be auditable by the existing operators (source available for review).

### R7 — Continuous compliance

Conformance is **not** one-and-done. The client must:

- run the **shared conformance + validation suites in its own CI**, pinned to the declared protocol
  version;
- **re-verify on every release** (its own and on each snapchain protocol bump);
- remain subject to removal via `effective_at` if it drifts.

---

## How requirements are verified

The above requirements map onto four test layers. Snapchain is strong at L2 today but thin at L0/L3
— the concrete gaps are tracked under **[#924](https://github.com/farcasterxyz/snapchain/issues/924)**
and must be closed before a Tier B/C client is admitted.

| Layer | Verifies | Mechanism | Status |
|-------|----------|-----------|--------|
| **L0** Conformance vectors | R1, R2 | Shared, versioned corpus: input → expected bytes/hash/signature/state-root. Today [`client_parity_tests`](../../tests/client_parity_tests/) is one-directional (input validation only); it must become bidirectional with output assertions. | **Gap — [#917](https://github.com/farcasterxyz/snapchain/issues/917)** |
| **L1** Unit / validation | R2, R4 | The client passes the reference validation corpus and a Byzantine/equivocation harness. | Validation: exists. Byzantine harness: **gap — [#918](https://github.com/farcasterxyz/snapchain/issues/918)**, fuzz: **[#923](https://github.com/farcasterxyz/snapchain/issues/923)** |
| **L2** Multi-node | R4, R5 | The [`consensus_test.rs`](../../tests/consensus_test.rs) `TestNetwork` harness with the candidate as a node: consensus, sync, crash/recovery, **validator-add**, cross-shard, partition. | Strong, but **add path is untested — [#919](https://github.com/farcasterxyz/snapchain/issues/919)** |
| **L3** Full-network testnet | R1–R7 end-to-end | Production-like testnet (real config + QUIC, [`setup_local_testnet`](../../src/bin/setup_local_testnet.rs), load via [`src/perf/`](../../src/perf/)) with the candidate validating. | **Gap — [#922](https://github.com/farcasterxyz/snapchain/issues/922)** |

## Deployment requirements (all new validators)

These apply to **every** new validator, Tier A included — most relevant for a validator in a new
geo/datacenter, because snapchain's timing budget is tight (`block_time` 1s; `propose_time` 1s;
`prevote_time`/`precommit_time` 500ms; round timeouts grow by `step_delta` 500ms,
[`consensus.rs`](../../src/consensus/consensus.rs#L130-L150)).

- **Latency budget:** measured RTT from the new location to each existing validator must fit inside
  the propose/prevote windows with margin. A validator that can't get votes out in time degrades
  throughput for everyone. *(No latency-injection test exists yet — [#920](https://github.com/farcasterxyz/snapchain/issues/920).)*
- **Soak:** a multi-day run from the real location to catch diurnal jitter, packet loss, and mesh
  re-formation under churn.
- **Resilience:** partition/failover drills from the new location; NTP/clock-sync verified.
- **Reachability:** bootstrap/direct-peer config and QUIC/firewall reachability confirmed.
- **Bootstrap:** node fully synced (snapshot/replication) and tracking tip **before** its
  `effective_at`.

## Operator requirements (all validators)

While the set is manually governed (see [Current phase](#motivation)), recovering from a
high-priority incident — a stalled shard needing a coordinated validator-set cutover, or a fast
removal of a misbehaving node — depends on operators coordinating in real time. A prospective
operator must commit to:

- a **reachable on-call / incident contact** and an agreed escalation path;
- **participation in coordinated validator-set changes** (cutovers, rollbacks) on short notice;
- **good-faith, professional collaboration** with other operators.

Operators are also **added as maintainers of the snapchain repository**, sharing responsibility for
review, releases, and incident fixes — reinforcing the auditability expectation in R6 and ensuring
every operator can act during an incident.

The same manual process that admits a validator can **remove** one — for technical drift (R3/R7) or
for failing to uphold these collaboration expectations. These criteria are interim and expected to
be superseded once native protocol incentives exist.

## Rollout

Staged and reversible:

1. **Pass L0–L2.** No mainnet scheduling until green.
2. **Testnet read-node.** Candidate syncs (no voting); verify zero state-root divergence over an
   observation window.
3. **Testnet validator.** Add via future `effective_at`; observe the L3 gates for an observation
   window, including a partition drill.
4. **Mainnet.** Schedule `effective_at` to cut into **all shards at around the same time** —
   staggering leaves the validator sets mismatched across shards and risks cross-shard instability.
   Because `effective_at` is a per-shard height and shards advance independently, a near-simultaneous
   cutover needs per-shard height estimates coordinated with existing operators.
5. **Rollback.** Removal is the same mechanism in reverse — a validator-set entry dropping the
   candidate at a future `effective_at`. Operators should know how to execute and observe a removal
   before scheduling the add.

## Acceptance checklist (go / no-go)

Complete before a mainnet `validators.toml` edit. Tier tags in parentheses; unmarked = all tiers.

- [ ] **(B/C)** R1 conformance vectors: 100% byte-exact (encoding, hash, signature, state root).
- [ ] **(B/C)** R2 validation parity across the full corpus.
- [ ] **(B/C)** R3 declared protocol-compat version + adoption process agreed.
- [ ] **(B/C)** R4 equivocation-safe signer + crash-recovery demonstrated.
- [ ] **(B/C)** R7 conformance + validation suites running in the client's own CI.
- [ ] L2 multi-node scenarios green across repeated runs (incl. validator-add).
- [ ] L3 production-like testnet soak passed (block production, round count, commit latency, zero
      candidate-attributable rejections, candidate proposes & is voted in).
- [ ] Latency budget met with margin; multi-day soak + partition drill from the real location; NTP
      verified.
- [ ] Testnet read-node ran clean (zero divergence) over the observation window.
- [ ] Key custody attested; monitoring/alerting wired; client auditable by operators.
- [ ] Operator committed to incident-response collaboration: on-call contact, escalation path, and
      participation in coordinated validator-set changes.
- [ ] Rollback (removal) procedure agreed; near-simultaneous per-shard `effective_at` schedule
      agreed with all operators.

## Open questions

- Should a **Byzantine/equivocation fault-injection harness** ([#918](https://github.com/farcasterxyz/snapchain/issues/918))
  be a hard requirement for Tier C, or is conformance + audit sufficient?
- If a desirable geo can't fit the current timing budget, do we revisit `propose_time` /
  `prevote_time` / `block_time` — a coordinated, all-node change?
- Who **owns and versions** the shared conformance corpus, and where does it live so multiple
  clients can depend on it?

## References

- Testing-gap tracking issue: [#924](https://github.com/farcasterxyz/snapchain/issues/924)
  (sub-issues [#917](https://github.com/farcasterxyz/snapchain/issues/917)–[#923](https://github.com/farcasterxyz/snapchain/issues/923))
- Consensus-critical codec: [`snapchain_codec.rs`](../../src/consensus/malachite/snapchain_codec.rs),
  [`blocks.proto`](../../proto/definitions/blocks.proto)
- Validator config & timing: [`consensus.rs`](../../src/consensus/consensus.rs),
  [`validators.toml`](../../validators.toml)
- Networking: [`gossip.rs`](../../src/network/gossip.rs)
- Test harness: [`consensus_test.rs`](../../tests/consensus_test.rs),
  [`client_parity_tests/`](../../tests/client_parity_tests/),
  [`setup_local_testnet.rs`](../../src/bin/setup_local_testnet.rs), [`src/perf/`](../../src/perf/)
