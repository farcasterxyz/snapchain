# Hyper Mode Architecture

This document captures the plan for running a **hyper** copy of the network
alongside the canonical path. Hyper peers mimic the public protocol for every
legacy interaction while maintaining a second execution context where pruning
limits do not apply and new rules can be evaluated.

## Goals

- Keep the existing network id and leader set untouched.
- Provide a node-local execution context that keeps all historical data
  (messages, blocks, metadata) without pruning.
- Allow hyper-capable peers to exchange richer blocks while continuing to
  gossip canonical blocks to legacy-only peers.
- Offer tooling to compare canonical vs. hyper state so operators can detect
  divergence quickly.

## State Context & Storage

- Introduce `StateContext::{Legacy, Hyper}` to tag every storage interaction.
- Wrap RocksDB access through a context-aware adapter that namespaces keys via
  `StateContext::namespace_prefix()`.
- Pruning hooks check `ctx.allows_pruning()`; for hyper contexts pruning becomes
  a no-op that logs when retention heuristics request it.
- Hyper and legacy stores can co-exist inside the same DB by prefixing key
  ranges, or in separate column families if desired later.

## Dual Pipelines

- Ingress queue fans out to two execution paths:
  1. **Legacy pipeline** – existing behavior, retains pruning, drives gossip to
     every peer so canonical behavior never changes.
  2. **Hyper pipeline** – clones decoded inputs, substitutes
     `StateContext::Hyper`, and applies new validation rules or metadata
     enrichments without pruning.
- Each canonical block emits an extra `HyperEnvelope` with metadata that tracks
  the same `canonical_block_id` and `parent_hash` to keep ordering aligned.

## Peer Capabilities & Packaging

- Extend the gossip handshake with `CAPABILITY_HYPER` so peers advertise
  support explicitly.
- When both peers support the capability, canonical blocks are followed by a
  `HyperEnvelope { metadata, payload }` over the same stream.
- `HyperBlockMetadata` contains the hyper state root, retained message count,
  and rule-version identifiers so receivers can validate the payload deterministically.
- Legacy peers ignore the hyper extension entirely because it is never sent to
  them.

## Validation & Diff Tooling

- Provide CLI helpers such as `snapchain hyper diff --block <n>` that read both
  contexts and render a `HyperDiffReport` (state roots, retained message deltas,
  human-readable notes).
- Offer `snapchain hyper audit --latest <m>` to walk backwards through recent
  blocks verifying there are no hyper gaps and to highlight when canonical
  pruning removed data that still exists in hyper.
- Emit metrics (`hyper_pipeline_lag`, `hyper_storage_bytes`,
  `hyper_diff_failures`) on the interval described by the config.

## Configuration & Operations

- New `hyper::HyperConfig` controls whether the pipeline spins up, the soft
  retention cap (for alerting), and metrics cadence.
- Defaults keep hyper mode disabled; enabling requires explicit configuration.
- Operators should monitor disk usage and lag between canonical and hyper block
  processing to ensure the unbounded store remains healthy.

## Next Steps

1. Implement the storage adapters so that `StateContext` routing is available to
   the existing stores.
2. Wire the dual pipeline so canonical + hyper execution share the same ingress
   but emit different outputs.
3. Extend gossip handshakes/protobufs with the `CAPABILITY_HYPER` bit and the
   `HyperEnvelope` payload.
4. Add the CLI commands and metrics described above.

## Proto Additions (draft)

```proto
message HyperBlockMetadata {
  uint64 canonical_block_id = 1;
  bytes parent_hash = 2;
  bytes hyper_state_root = 3;
  uint32 extra_rules_version = 4;
  uint64 retained_message_count = 5;
}

message HyperEnvelope {
  HyperBlockMetadata metadata = 1;
  bytes payload = 2;
}

message GossipHandshake {
  repeated string capabilities = 5; // includes "hyper:v1" when supported
}
```

Only peers that negotiate `"hyper:v1"` will ever see `HyperEnvelope` payloads.
Legacy peers keep using the existing handshake slice unmodified.

## CLI Draft Interfaces

- `snapchain hyper diff --block 12345 --context both`
  - Pulls the canonical block plus hyper metadata, compares state roots, and
    prints a `HyperDiffReport`.
- `snapchain hyper audit --latest 500`
  - Walks the past 500 canonical blocks, ensuring each has a matching hyper
    envelope and retained message counts only increase as expected.
- `snapchain hyper metrics --tail`
  - Streams metrics derived from the hyper pipeline (lag, storage usage, diff
    failures) for quick debugging.

These commands reuse the RPC layer so operators can target production hubs or
local development nodes using the same authentication model.

### CLI Scaffolding

A placeholder binary (`cargo run --bin hyper -- --help`) wires up the `diff`,
`audit`, and `metrics` subcommands so the UX is discoverable while the backend
plumbing is built. The current implementation surfaces descriptive messages plus
a stub `HyperDiffReport`, making it safe to experiment before the hyper pipeline
lands.

## Example Execution Flow

1. Node receives a new canonical block from a legacy peer.
2. Legacy pipeline processes and prunes according to the current rules.
3. Hyper pipeline consumes the same block, bypasses pruning, writes to the
   hyper namespace, and builds the `HyperEnvelope`.
4. Gossip layer sends canonical block to every peer. For hyper-capable peers it
   immediately follows up with the envelope, referencing the canonical block id.
5. Operators run `snapchain hyper diff --block <id>` which internally loads both
   contexts and surfaces any retained message deltas or divergent roots.
