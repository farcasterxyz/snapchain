# Per-FID mempool size limit

## Motivation

During the Feb 26 spam incident, FID 2842822 submitted ~90K follow messages/hour. The rate limiter slowed it down but couldn't fully stop it. This adds a second layer of defense: a cap on how many of a single FID's messages can sit in the mempool at once. Once a FID fills its quota, new submissions are rejected immediately — before consuming rate limit tokens, before being gossiped, and before crowding out other FIDs.

This is distinct from rate limiting (which limits submission *rate* over time). The mempool cap limits *concurrent pending messages*.

## Changes

- Added `max_mempool_messages_per_fid: u32` to `Config` (default: 100, 0 = disabled)
- Added `fid_message_counts: HashMap<u64, usize>` to `Mempool` — one entry per FID with pending messages, incremented on insert and decremented on `pull_messages` (proposer path) or `remove_committed_txns` (non-proposer path)
- Added `fid_exceeds_mempool_limit` check in `insert_into_shard`, before `message_is_valid`, so rejected messages never touch the DB or consume rate limit tokens
- Only applies to `UserMessage` variants; validator/onchain/fname/block messages are unaffected
- New metric: `mempool.per_fid_limit_hit` (count, per shard)

## Memory overhead

The map holds one entry per FID with *currently pending* messages — not one per registered FID and not one per message. Between blocks it's nearly empty. Worst case (every mempool slot occupied by a different FID) is ~34 MB, which is unreachable in practice since spam incidents involve very few FIDs.

## Performance

Benchmarked sequential insert throughput with and without the cap, across both a normal workload (100 FIDs × 10 messages) and a single-FID spam scenario (1 FID × 500 messages, cap=50). Throughput was ~950 msg/s in all cases — identical with cap on or off. The cap check (a single HashMap lookup) adds no measurable overhead. The bottleneck is the 1ms poll interval, which yields a theoretical ceiling of ~256,000 msg/s per shard. The Feb 26 incident peaked at ~90K msg/hour (~25 msg/s), well within that headroom.

## Running the perf tests

```
cargo test -p snapchain perf_ -- --ignored --nocapture
```
