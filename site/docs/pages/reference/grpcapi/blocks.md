# Blocks API

Used to retrieve blocks and shard chunks from the chain.

## API

| Method Name     | Request Type         | Response Type        | Description                                     |
| --------------- | -------------------- | -------------------- | ----------------------------------------------- |
| GetBlocks       | BlocksRequest        | stream Block         | Returns a stream of blocks for a given shard    |
| GetShardChunks  | ShardChunksRequest   | ShardChunksResponse  | Returns chunks of serialized block data         |

## BlocksRequest

| Field              | Type              | Label    | Description                              |
| ------------------ | ----------------- | -------- | ---------------------------------------- |
| shard_id           | [uint32](#uint32) |          | ID of the shard to get blocks from       |
| start_block_number | [uint64](#uint64) |          | Block number to start from (inclusive)   |
| stop_block_number  | [uint64](#uint64) | optional | Block number to stop at (inclusive)      |

## ShardChunksRequest

| Field              | Type              | Label    | Description                              |
| ------------------ | ----------------- | -------- | ---------------------------------------- |
| shard_id           | [uint32](#uint32) |          | ID of the shard to get chunks from       |
| start_block_number | [uint64](#uint64) |          | Block number to start from (inclusive)   |
| stop_block_number  | [uint64](#uint64) | optional | Block number to stop at (inclusive)      |

## ShardChunksResponse

| Field        | Type                | Label    | Description             |
| ------------ | ------------------- | -------- | ----------------------- |
| shard_chunks | [ShardChunk](#)     | repeated | Array of shard chunks   |

## Block

| Field              | Type                      | Label    | Description                          |
| ------------------ | ------------------------- | -------- | ------------------------------------ |
| header             | [BlockHeader](#)          |          | Header info for the block            |
| messages           | [Message](#)              | repeated | Array of messages in the block       |