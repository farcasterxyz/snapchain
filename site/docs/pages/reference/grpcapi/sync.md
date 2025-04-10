# Sync API

These APIs are used by Hubs to synchronize their state with other Hubs. Not meant for use by external applications.

## API

| Method Name             | Request Type      | Response Type            | Description                                   |
| ----------------------- | ----------------- | ------------------------ | --------------------------------------------- |
| GetInfo                 | HubInfoRequest    | HubInfoResponse          | Returns metadata about the hub's state.       |
| GetSyncStatus           | SyncStatusRequest | SyncStatusResponse       | Returns the hub's sync status.                |
| GetAllSyncIdsByPrefix   | TrieNodePrefix    | SyncIds                  | Get all the SyncIds for a particular prefix   |
| GetAllMessagesBySyncIds | SyncIds           | MessagesResponse         | Get all messages given corresponding sync ids |
| GetSyncMetadataByPrefix | TrieNodePrefix    | TrieNodeMetadataResponse | Get Sync metadata for a particular prefix     |
| GetSyncSnapshotByPrefix | TrieNodePrefix    | TrieNodeSnapshotResponse | Get sync snapshot for a particular prefix     |

## HubInfoRequest

| Field    | Type          | Label | Description |
| -------- | ------------- | ----- | ----------- |
| db_stats | [bool](#bool) |       |             |

## HubInfoResponse

Response Types for the Sync RPC Methods

| Field      | Type                | Label | Description |
| ---------- | ------------------- | ----- | ----------- |
| version    | [string](#string)   |       |             |
| is_syncing | [bool](#bool)       |       |             |
| nickname   | [string](#string)   |       |             |
| root_hash  | [string](#string)   |       |             |
| db_stats   | [DbStats](#DbStats) |       |             |

## SyncStatusRequest

| Field  | Type              | Label    | Description |
| ------ | ----------------- | -------- | ----------- |
| peerId | [string](#string) | optional |             |

## SyncStatusResponse

| Field       | Type                      | Label    | Description |
| ----------- | ------------------------- | -------- | ----------- |
| is_syncing  | [bool](#bool)             |          |             |
| sync_status | [SyncStatus](#SyncStatus) | repeated |             |

## SyncStatus

| Field                | Type              | Label | Description |
| -------------------- | ----------------- | ----- | ----------- |
| peerId               | [string](#string) |       |             |
| inSync               | [string](#string) |       |             |
| shouldSync           | [bool](#bool)     |       |             |
| divergencePrefix     | [string](#string) |       |             |
| divergenceSecondsAgo | [int32](#int32)   |       |             |
| theirMessages        | [uint64](#uint64) |       |             |
| ourMessages          | [uint64](#uint64) |       |             |
| lastBadSync          | [int64](#int64)   |       |             |

## TrieNodePrefix

| Field  | Type            | Label | Description |
| ------ | --------------- | ----- | ----------- |
| prefix | [bytes](#bytes) |       |             |

## SyncIds

| Field    | Type            | Label    | Description |
| -------- | --------------- | -------- | ----------- |
| sync_ids | [bytes](#bytes) | repeated |             |

## TrieNodeMetadataResponse

| Field        | Type                                                  | Label    | Description |
| ------------ | ----------------------------------------------------- | -------- | ----------- |
| prefix       | [bytes](#bytes)                                       |          |             |
| num_messages | [uint64](#uint64)                                     |          |             |
| hash         | [string](#string)                                     |          |             |
| children     | [TrieNodeMetadataResponse](#TrieNodeMetadataResponse) | repeated |             |

## TrieNodeSnapshotResponse

| Field           | Type              | Label    | Description |
| --------------- | ----------------- | -------- | ----------- |
| prefix          | [bytes](#bytes)   |          |             |
| excluded_hashes | [string](#string) | repeated |             |
| num_messages    | [uint64](#uint64) |          |             |
| root_hash       | [string](#string) |          |             |

## DbStats

| Field            | Type              | Label | Description |
| ---------------- | ----------------- | ----- | ----------- |
| num_messages     | [uint64](#uint64) |       |             |
| num_fid_events   | [uint64](#uint64) |       |             |
| num_fname_events | [uint64](#uint64) |       |             |
