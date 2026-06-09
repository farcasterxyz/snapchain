# FIP: Testing & Acceptance Criteria for Onboarding New Snapchain Validators

> **Note:** This document is written in the snapchain repo for review, but is intended to be posted
> as an RFC / discussion in [farcasterxyz/protocol Discussions](https://github.com/farcasterxyz/protocol/discussions).
> Code references below point at the snapchain repository.

| Field | Value |
|-------|-------|
| **Title** | Testing & Acceptance Criteria for Onboarding New Snapchain Validators |
| **Status** | Draft |
| **Author** | Neynar (manan@neynar.com) |
| **Created** | 2026-06-09 |
| **Discussion** | TBD |

---

## Summary

Snapchain consensus is Byzantine-fault-tolerant (Malachite BFT) with **equal voting power per
validator** and a **2/3+ quorum** to commit. Adding a validator is a one-line change — appending a
`[[consensus.validator_sets]]` entry to [`validators.toml`](../../validators.toml) at a future
`effective_at` height — but the blast radius is large: a single faulty, slow, or *divergent*
validator on a ~6-node shard sits close to the 1/3 fault budget that halts a shard.

This FIP defines, **before any validator is appended to `validators.toml`**:

1. the **failure modes** a new validator can introduce (consensus/determinism, networking/liveness,
   operational/security);
2. a **layered test taxonomy** — unit → service/multi-node → **full-network integration on a
   production-like testnet** — with explicit pass/fail gates;
3. a **staged rollout** (read-node → testnet → mainnet, one shard at a time, with rollback); and
4. a **copy-pasteable go/no-go acceptance checklist**.

All of the above is scaled to **three escalating risk tiers** so the bar is proportionate to how
different the candidate is from the existing fleet.

## Motivation

Today snapchain consensus is run by two operators — **Neynar (5 active validators)** and **Uno (1
validator)** — all running the **same Rust binary**, with validator sets pinned per-shard in
[`validators.toml`](../../validators.toml).

> Neynar's key 6 replaced key 4 in an earlier per-shard rotation, so although keys 1–6 appear in the
> history, only 5 Neynar validators are active concurrently. The current active set per shard is
> keys 1, 2, 3, 5, 6 + Uno's key 7 = **6 validators**, meaning the fault budget is **1** (⌊(6-1)/3⌋).

Two upcoming changes increase risk and motivate a written, testable bar:

1. **Geographic / datacenter diversity (primary driver).** Adding a validator in a new geo/DC,
   further from the existing (largely us-east) cluster. This directly stresses snapchain's
   latency-sensitive BFT timing (1s block time; 500ms–1s step timeouts). A validator that can't get
   its proposals and votes to the rest of the set inside the timeout budget degrades throughput for
   *everyone* and, if it crosses the 1/3 boundary during a partition, halts a shard.

2. **Node (consensus-client) diversity (secondary driver).** Interest in running a validator built
   from a **different codebase** than snapchain — an independent reimplementation, or a fork.
   *"Node / client" here means the consensus node software — the validator/hub binary that
   participates in BFT — and explicitly **not** a web or mobile app client.* A divergent node
   implementation can silently break consensus determinism (encoding, hashing, signing, validation,
   state roots) in ways a same-binary operator never can.

There is currently **no documented, staged acceptance procedure**. This FIP fills that gap.

## Specification

### Risk tiers (the spine of this proposal)

Every failure mode and test below is tagged by the lowest tier it applies to. **Tiers are
cumulative** — Tier C must pass everything required for A and B as well.

| Tier | Candidate | Dominant risk | Required because |
|------|-----------|---------------|------------------|
| **A** | Stock snapchain, **new operator / new geo-DC** | Networking, latency, key custody, ops | Same binary ⇒ determinism is a given; the network path and operator are new. |
| **B** | **Fork** of snapchain (custom patches, dep drift) | Divergence in encoding / validation / config vs upstream | Code differs ⇒ subtle determinism drift possible; everything in A still applies. |
| **C** | **Independent reimplementation** (from-scratch consensus client) | Full byte-for-byte determinism | Nothing shared ⇒ protobuf bytes, hashes, signatures, validation rules, and state roots must all match exactly; everything in A and B still applies. |

### How a validator participates (background, grounded in the code)

A reviewer should understand these mechanisms, because the test criteria map directly onto them.

- **Consensus:** Malachite BFT (CometBFT/Tendermint-style), pinned to a specific git ref in CI
  ([`.github/workflows/verify-impl.yml`](../../.github/workflows/verify-impl.yml)); engine crates in
  [`Cargo.toml`](../../Cargo.toml). Flow per height: **propose → prevote → precommit → commit**, with
  2/3+ voting power required at each gate. Round timeouts grow by `step_delta` each round.
- **Per-shard consensus:** shard 0 is the block shard; shards 1–2 are message shards. Each shard
  runs an **independent** consensus instance with its **own validator set**
  ([`src/consensus/validator.rs`](../../src/consensus/validator.rs),
  [`src/consensus/consensus.rs`](../../src/consensus/consensus.rs#L39-L44)). A validator can be in some
  shards' sets but not others.
- **Validator identity:** Ed25519. The address is the 32-byte public key; `Ed25519Provider` signs
  and verifies votes, proposals, and commit certificates
  ([`src/core/types.rs`](../../src/core/types.rs)).
- **Validator set config:** `[[consensus.validator_sets]]` entries with `effective_at` (block height
  the set activates) and `shard_ids` ([`validators.toml`](../../validators.toml),
  [`src/consensus/consensus.rs`](../../src/consensus/consensus.rs#L39-L44)). Each validator has voting
  power = 1; 2/3+ to commit.
- **Consensus-critical serialization:** protobuf (`prost`) over votes/proposals/commits. **Signatures
  are computed over `encode_to_vec()` bytes**, and block/shard headers are hashed with **BLAKE3**
  ([`src/consensus/malachite/snapchain_codec.rs`](../../src/consensus/malachite/snapchain_codec.rs),
  [`proto/definitions/blocks.proto`](../../proto/definitions/blocks.proto)). Any encoding or hashing
  difference produces different signed bytes and breaks verification.
- **Networking:** libp2p gossipsub over QUIC (port 3382). Topics: `consensus`, `mempool`,
  `decided-values`, `contact-info`. Bootstrap and direct peers; gossipsub heartbeat 500ms; mesh
  D=10 / Dh=20 ([`src/network/gossip.rs`](../../src/network/gossip.rs)).
- **Latency-sensitive timing** (defaults,
  [`src/consensus/consensus.rs`](../../src/consensus/consensus.rs#L130-L150)):

  | Parameter | Default |
  |-----------|---------|
  | `block_time` | 1000 ms |
  | `propose_time` | 1000 ms |
  | `prevote_time` | 500 ms |
  | `precommit_time` | 500 ms |
  | `step_delta` (per-round growth) | 500 ms |
  | `sync_request_timeout` | 2000 ms |

  These are **the budget a cross-geo validator must fit inside.** Round-trip time between the new DC
  and the existing set must leave proposals and votes arriving well within these windows.

### Failure modes (tagged by tier)

**Consensus / determinism** — primarily Tier C, partially Tier B:

| # | Failure | Mechanism | Tier |
|---|---------|-----------|------|
| C1 | **Encoding divergence** | Different protobuf bytes (field ordering, default/optional handling, timestamp width) ⇒ different signed bytes ⇒ signature verify fails ⇒ votes rejected ⇒ shard stalls | B/C |
| C2 | **Hash divergence** | BLAKE3 over headers differs ⇒ proposers disagree on block hash ⇒ no 2/3 on any value | B/C |
| C3 | **State-root / app-state divergence** | Message-shard validators compute a different post-state ⇒ prevote nil ⇒ liveness loss; if a quorum diverges *together*, a **fork** | B/C |
| C4 | **Validation-rule divergence** | One client accepts a message another rejects ⇒ non-deterministic block contents ⇒ consensus failure | B/C |
| C5 | **Equivocation / double-sign** | Buggy client signs two values at one height/round ⇒ **safety** risk; counts against the fault budget | A/B/C |
| C6 | **Proposer-selection / round-numbering mismatch** | Proposes in the wrong round, or disagrees on whose turn it is ⇒ wasted rounds, degraded liveness | B/C |
| C7 | **WAL / crash-recovery divergence** | Validator replays incorrectly on restart ⇒ votes inconsistently with its pre-crash self | A/B/C |

**Networking / liveness** — all tiers, dominant for Tier A:

| # | Failure | Mechanism | Tier |
|---|---------|-----------|------|
| N1 | **Cross-geo RTT exceeds timing budget** | Proposals/votes arrive after `propose_time`/`prevote_time` ⇒ repeated round timeouts, lower throughput, missed proposer slots | A/B/C |
| N2 | **Gossipsub mesh fails to form** with a distant peer (D=10 mesh, QUIC) ⇒ message loss ⇒ liveness | A/B/C |
| N3 | **Bootstrap / direct-peer misconfig** ⇒ new validator never joins the mesh, or partially connects | A/B/C |
| N4 | **Partition / asymmetric reachability** between DCs ⇒ minority stalls; if the new validator straddles the 1/3 boundary, the **whole shard stalls** | A/B/C |
| N5 | **Clock skew** across DCs ⇒ timestamp / `LIVE_AT` and timeout interactions | A/B/C |
| N6 | **MTU / QUIC / firewall** issues on the new network path; 10 MB max transmit size | A/B/C |

**Operational / security** — all tiers:

| # | Failure | Mechanism | Tier |
|---|---------|-----------|------|
| O1 | **Key custody / accidental key reuse** ⇒ equivocation (C5); HSM/secret-management gaps | A/B/C |
| O2 | **Snapshot / replication bootstrap failure** ⇒ validator never reaches tip before `effective_at` ([`src/storage/db/snapshot.rs`](../../src/storage/db/snapshot.rs); replication peers `rho.farcaster.xyz` / `tau.farcaster.xyz`) | A/B/C |
| O3 | **`effective_at` coordination mistake** ⇒ the set activates before the node is synced | A/B/C |
| O4 | **Resource exhaustion** (mempool capacity 1M/shard, DB growth) under production load | A/B/C |

### Test taxonomy & acceptance gates

The explicit requirement is that testing is **not just unit and service level** — it must include
**full-network integration on a testnet whose configuration is close to production.** Each layer
below reuses existing snapchain infrastructure where possible.

#### L0 — Conformance / determinism vectors *(Tier B/C — the new requirement for non-snapchain clients)*

A shared, versioned corpus mapping each input to its **expected protobuf bytes, expected BLAKE3
hash, expected signature, and expected post-state root.** This is the single most important gate for
Tier C, because it catches C1–C4 *before* the candidate ever touches the network.

- Extend the existing **one-way** parity suite [`tests/client_parity_tests/`](../../tests/client_parity_tests/)
  (TypeScript vectors in `input_messages/`, cases in `test_inputs.json`) into a **bidirectional**
  suite the candidate client must pass in both directions.
- **Gate:** 100% byte-exact match on encoding, hash, signature, and resulting state root across the
  full corpus, including adversarial/edge inputs.

#### L1 — Unit *(all tiers; the parity gate is Tier B/C)*

- Validation rules, encoding round-trips, signing/verify
  ([`src/core/validations/`](../../src/core/validations/), `*_tests.rs`).
- **Gate (B/C):** the candidate passes the **same** validation corpus the Rust suite enforces — same
  accept/reject decision on every message. (Tier A inherits snapchain's existing unit suite by
  definition.)

#### L2 — Service / multi-node integration *(all tiers)*

Use the [`tests/consensus_test.rs`](../../tests/consensus_test.rs) `TestNetwork` harness, which spins
up N validators × M shards and supports crash, restart, pause, **network partition**, gRPC submit,
and block counting. Add a scenario where **≥1 node is the candidate** (for Tier A, an extra
same-binary node in a simulated remote profile), then run:

- basic consensus, sync, crash + recovery;
- **validator-set rotation / add** (`test_validator_set_rotation`) — note the existing test leans on
  the *remove* path because the *add* path is flaky in CI; **hardening the add path is part of this
  work** (see Open Questions);
- cross-shard interactions; partition + heal.
- **Gate:** all scenarios green across repeated runs (flake-free).

#### L3 — Full-network testnet integration *(all tiers — the production-like requirement)*

Stand up a testnet whose configuration **mirrors production**, not a toy local net:

- same `num_shards`, **same consensus timing defaults**, snapshot bootstrap, and **real gossip over
  QUIC across the actual target DC/geo** — not localhost;
- generate configs and keys with [`src/bin/setup_local_testnet.rs`](../../src/bin/setup_local_testnet.rs);
  base the topology on the `docker-compose*.yml` files;
- drive realistic load with the `perftest` binaries ([`src/perf/`](../../src/perf/)).
- **Gate metrics:**
  - sustained block production at the target `block_time`;
  - ~0 extra rounds at steady state (no chronic timeouts);
  - p50/p99 commit latency within budget;
  - **zero signature/hash rejections** attributable to the candidate;
  - the candidate **proposes and is voted in** during its proposer slots;
  - survives a **rolling restart** and a **simulated cross-DC partition** with correct recovery.

### Geo / datacenter-specific tests *(new validator in a different geo — all tiers)*

These target N1–N6 and O3 specifically:

- Measure **real RTT** from the new DC to *each* existing validator; verify it fits within the
  `propose_time` / `prevote_time` budget **with margin**; document headroom and whether timing
  params need revisiting (and that changing timing is itself a coordinated, all-node change).
- **Soak test** for ≥ several days on testnet **from the real DC** to catch diurnal latency/jitter,
  transient packet loss, and mesh re-formation under churn.
- **Partition/failover drills** from the new DC (reuse `partition_validators` / `heal_partition`).
- **Clock-sync (NTP)** verification across DCs.
- Confirm **bootstrap/direct-peer lists** and **firewall/QUIC reachability** from the new network
  path.

## Rollout

Staged, reversible, one shard at a time:

1. **Pass L0–L2 in CI.** No mainnet scheduling until these are green.
2. **Testnet read-node first.** Candidate joins the public testnet as a **read node** (syncs, does
   not vote). Verify it tracks tip with **zero divergence** over an observation window.
3. **Testnet validator.** Add the candidate to the **testnet** validator set via a future
   `effective_at`. Observe the L3 gates for an observation window (e.g. several days), including a
   partition drill.
4. **Mainnet, one shard at a time.** Only then schedule a **mainnet** `effective_at`, **per shard**,
   mirroring the existing staggered per-shard rollout already visible in
   [`validators.toml`](../../validators.toml) (block shard first, then message shards, with an
   observation window between each).
5. **Rollback plan.** Pre-stage a follow-up validator-set entry that **removes** the candidate, so
   recovery is a known-good config change rather than an improvised one. Document the height
   coordination, and note the **"remove is reliable, add is racy"** caveat from
   `test_validator_set_rotation`.

## Acceptance checklist (go / no-go)

A candidate operator must complete this **before** a `validators.toml` mainnet edit. Tier tags in
parentheses; unmarked items apply to **all** tiers.

- [ ] **(B/C)** L0 determinism vectors: 100% byte-exact on encoding, hash, signature, state root.
- [ ] **(B/C)** L1 validation parity: identical accept/reject on the full message corpus.
- [ ] L2 multi-node scenarios green across repeated runs (consensus, sync, crash/recovery,
      rotation, cross-shard, partition+heal).
- [ ] L3 production-like testnet soak passed: block production, round count, commit latency, zero
      candidate-attributable rejections, candidate proposes & is voted in.
- [ ] Cross-DC RTT measured and within timing budget with margin; multi-day soak from the real DC.
- [ ] Partition / failover drill from the new DC passed; NTP/clock-sync verified.
- [ ] Bootstrap/direct-peer config and firewall/QUIC reachability confirmed from the new path.
- [ ] Testnet read-node ran clean (zero divergence) over the observation window.
- [ ] Key custody attested (HSM / secret management; no key reuse; equivocation-safe signer).
- [ ] Monitoring & alerting wired (block height lag, round count, rejection counts, peer/mesh
      health).
- [ ] Rollback validator-set entry pre-staged and reviewed.
- [ ] Per-shard `effective_at` schedule agreed with all existing operators.

## Risks & open questions

- **Flaky validator-add test.** `test_validator_set_rotation` currently relies on the *remove* path
  because the *add* path is racy in CI (new-validator discovery timing). Onboarding fundamentally
  exercises the add path, so hardening it is a prerequisite, not a nicety.
- **No Byzantine / equivocation fault-injection harness** exists today. Should one be **required**
  for Tier C before a from-scratch client can vote on mainnet?
- **Cross-geo latency vs. timing params.** If a desirable geo can't fit the current budget, do we
  revisit `propose_time` / `prevote_time` / `block_time` — a coordinated, all-node change with its
  own testing burden?
- **No formal property-based / fuzz testing of the codec.** A fuzz/property suite over the protobuf
  + signing path would materially strengthen Tier C determinism assurance.

## References (snapchain code)

- Validator set config & timing defaults: [`src/consensus/consensus.rs`](../../src/consensus/consensus.rs)
- Active validator sets & per-shard rollout history: [`validators.toml`](../../validators.toml)
- Consensus-critical codec (protobuf + BLAKE3 + Ed25519): [`src/consensus/malachite/snapchain_codec.rs`](../../src/consensus/malachite/snapchain_codec.rs), [`proto/definitions/blocks.proto`](../../proto/definitions/blocks.proto)
- Validator identity / signing: [`src/core/types.rs`](../../src/core/types.rs)
- Networking / gossip: [`src/network/gossip.rs`](../../src/network/gossip.rs)
- Snapshot / replication bootstrap: [`src/storage/db/snapshot.rs`](../../src/storage/db/snapshot.rs)
- Multi-node test harness: [`tests/consensus_test.rs`](../../tests/consensus_test.rs)
- Client parity vectors: [`tests/client_parity_tests/`](../../tests/client_parity_tests/)
- Testnet config/key generator: [`src/bin/setup_local_testnet.rs`](../../src/bin/setup_local_testnet.rs)
- Load/perf binaries: [`src/perf/`](../../src/perf/)
