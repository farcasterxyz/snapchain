# Snapchain High Message Commit Rate

## Monitor configuration

**Name:** `Snapchain - High message commit rate on {{host.name}} shard {{shard.name}}`

**Metric:** `snaptest.engine.commit.merged_message`
- Evaluate: sum over last 5 minutes
- Group by: host, shard
- Warning threshold: > 3,000
- Alert threshold: > 6,000

**Description:**
Fires when the number of user messages committed to blocks exceeds normal throughput on a single host/shard combination, which may indicate a spam campaign, a misconfigured client flooding the network, or a denial-of-service attempt against the protocol.

- Warning (>3,000 / 5 min): Elevated commit rate — monitor closely, may resolve on its own
- Alert (>6,000 / 5 min): Sustained high throughput — likely active abuse or misconfigured submitter

Note: This monitor fires on ALL message types. To identify which type is responsible, filter `snapchain.engine.commit.merged_message` by `message_type` tag in the Metrics Explorer (type 5 = LinkAdd/follow, type 3 = ReactionAdd, type 1 = CastAdd).

---

## Runbook

### 1. Identify the message type

In Datadog Metrics Explorer, query:

```
snapchain.engine.commit.merged_message grouped by message_type, host, shard
```

Look for a single `message_type` dominating the spike.

### 2. Identify the offending FID(s)

Search logs for pruned messages during the incident window:

```
service:snapchain "Pruned messages" @fields.msg_type:<TYPE>
```

High prune counts for a single FID indicate that FID is saturating its store.

Also search:

```
service:snapchain "rate limit exceeded for FID"
```

FIDs appearing frequently here are being blocked but are still submitting at high volume.

### 3. Check if rate limits are enabled on all validators

Confirm all validator configs have:

```toml
[mempool]
enable_rate_limits = true
```

A node missing this setting will accept unlimited submissions from the offending FID and gossip them to the rest of the network.

### 4. Identify signer keys for the FID

```
GET https://snap.farcaster.xyz:3381/v1/onChainSignersByFid?fid=<FID>
```

Note the signer keys and their `addedAt` timestamps — newly added keys near the incident start indicate intentional abuse with a fresh signer.

### 5. Short-term mitigation

If spam is ongoing and rate limits are not sufficient:

- Add `enable_rate_limits = true` to any validator config missing it and restart that node
- To stop a specific FID, a code change is required: add `blocked_fids: HashSet<u64>` to `mempool::Config` and check it in `message_is_valid()` (src/mempool/mempool.rs) before the rate limit check
- A node restart clears the in-memory mempool, but messages will re-gossip from other nodes unless the source is blocked at submission

### 6. Escalate if

- Block commit rate is consistently near `max_messages_per_block` (1000) — legitimate messages may be getting crowded out of blocks
- The spike persists after `enable_rate_limits = true` is confirmed on all validators
- Multiple FIDs are involved simultaneously (coordinated attack)
