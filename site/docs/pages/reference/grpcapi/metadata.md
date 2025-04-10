# Metadata API

These APIs are used to retrieve node metadata and for synchronization between nodes. Some methods are not meant for use by external applications.

## API

| Method Name             | Request Type            | Response Type            | Description                               |
| ----------------------- | ----------------------- | ------------------------ | ----------------------------------------- |
| GetInfo                 | GetInfoRequest          | GetInfoResponse          | Returns metadata about the node's state   |
| GetTrieMetadataByPrefix | TrieNodeMetadataRequest | TrieNodeMetadataResponse | Get trie metadata for a particular prefix |

## GetInfoRequest

Empty request, no parameters needed.

## GetInfoResponse

| Field       | Type                | Label    | Description                  |
| ----------- | ------------------- | -------- | ---------------------------- |
| db_stats    | [DbStats](#DbStats) |          | Database statistics          |
| num_shards  | [uint32](#uint32)   |          | Number of shards in the node |
| shard_infos | [ShardInfo](#)      | repeated | Information about each shard |

## DbStats

| Field                 | Type              | Label | Description                               |
| --------------------- | ----------------- | ----- | ----------------------------------------- |
| num_messages          | [uint64](#uint64) |       | Total number of messages in the node      |
| num_fid_registrations | [uint64](#uint64) |       | Number of FID registrations in the node   |
| approx_size           | [uint64](#uint64) |       | Approximate size of the database in bytes |

## ShardInfo

| Field                 | Type              | Label | Description                              |
| --------------------- | ----------------- | ----- | ---------------------------------------- |
| shard_id              | [uint32](#uint32) |       | Shard identifier                         |
| max_height            | [uint64](#uint64) |       | Maximum block height in the shard        |
| num_messages          | [uint64](#uint64) |       | Number of messages in the shard          |
| num_fid_registrations | [uint64](#uint64) |       | Number of FID registrations in the shard |
| approx_size           | [uint64](#uint64) |       | Approximate size of the shard in bytes   |
| block_delay           | [uint64](#uint64) |       | Block delay in the shard                 |
| mempool_size          | [uint64](#uint64) |       | Size of the mempool for this shard       |

## TrieNodeMetadataRequest

| Field    | Type              | Label | Description                  |
| -------- | ----------------- | ----- | ---------------------------- |
| shard_id | [uint32](#uint32) |       | Shard ID to get metadata for |
| prefix   | [bytes](#bytes)   |       | Prefix to get metadata for   |

## TrieNodeMetadataResponse

| Field        | Type                                                  | Label    | Description                          |
| ------------ | ----------------------------------------------------- | -------- | ------------------------------------ |
| prefix       | [bytes](#bytes)                                       |          | Prefix of the trie node              |
| num_messages | [uint64](#uint64)                                     |          | Number of messages under this prefix |
| hash         | [string](#string)                                     |          | Hash of the trie node                |
| children     | [TrieNodeMetadataResponse](#TrieNodeMetadataResponse) | repeated | Child nodes of this trie node        |
