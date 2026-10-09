# Info API

## info

Get the node's version, sync status and per-shard statistics.

**Example**

```bash
curl http://127.0.0.1:3381/v1/info
```

**Response**

```json
{
  "dbStats": {
    "numMessages": 914092103,
    "numFidRegistrations": 3354008,
    "approxSize": 548529692074
  },
  "numShards": 2,
  "shardInfos": [
    {
      "shardId": 0,
      "maxHeight": 47583086,
      "numMessages": 12661169,
      "numFidRegistrations": 0,
      "approxSize": 38012136899,
      "blockDelay": 1,
      "mempoolSize": 0
    },
    {
      "shardId": 2,
      "maxHeight": 48091473,
      "numMessages": 455556375,
      "numFidRegistrations": 1676871,
      "approxSize": 273017268128,
      "blockDelay": 0,
      "mempoolSize": 4294967295
    },
    {
      "shardId": 1,
      "maxHeight": 48256318,
      "numMessages": 458535728,
      "numFidRegistrations": 1677137,
      "approxSize": 275512423946,
      "blockDelay": 0,
      "mempoolSize": 4294967295
    }
  ],
  "version": "0.14.2",
  "peer_id": "12D3KooWCjFRfnj2yrWyADcW3adS45YigbUPwEyGZ6fB9rbxDW2T",
  "nextEngineVersionTimestamp": 0
}
```

**Fields**

| Field | Description |
| --- | --- |
| `dbStats` | Totals across the message shards; shard 0 is not included. `approxSize` is the approximate on-disk size in bytes. |
| `numShards` | Number of message shards. `shardInfos` has one more entry than this, because it also includes shard 0. |
| `shardInfos[].shardId` | `0` is the block shard, which orders the other shards' chunks. It keeps a small state of its own (onchain events are routed to it as well as to the user's shard), so its `numMessages` is not zero. `1` and higher are message shards, which hold user messages. |
| `shardInfos[].maxHeight` | Height of the newest block this node has for the shard. Each shard is its own chain, so heights differ between shards. |
| `shardInfos[].blockDelay` | Seconds between the current time and the timestamp of the shard's newest block. Close to `0` on a synced node; it climbs if the node falls behind or the shard stops producing blocks. |
| `shardInfos[].mempoolSize` | Messages waiting in this node's mempool for the shard. Always `0` for shard 0. Read nodes have no mempool and report `4294967295` (`u32::MAX`) for message shards, meaning "not reported". |
| `version` | Snapchain release the node is running. |
| `peer_id` | The node's libp2p peer ID. Note the snake_case name. |
| `nextEngineVersionTimestamp` | Unix timestamp of the next scheduled protocol upgrade, or `0` if none is scheduled. Nodes must run a release that supports that upgrade before this time. |
