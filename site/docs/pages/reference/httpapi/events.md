# Events API

The events API returns events as they are merged into the Hub, which can be used to listen to Hub activity.

## eventById

Get an event by its Id

**Query Parameters**
| Parameter   | Description                   | Example                    |
| ----------- | ----------------------------- | -------------------------- |
| event_id    | The Hub Id of the event       | `event_id=350909155450880` |
| shard_index | The shard index for the event | `shard_index=1`            |

**Example**

```bash
curl http://127.0.0.1:3381/v1/eventById?event_id=151622205440&shard_index=1

```

**Response**

```json
{
  "type": "HUB_EVENT_TYPE_BLOCK_CONFIRMED",
  "id": 151622205440,
  "blockConfirmedBody": {
    "blockNumber": 9254285,
    "shardIndex": 1,
    "timestamp": 142732801,
    "blockHash": "0x95659381b61ac3cd9fc06e61d9d9c256f8274aaab526cb23ce72856c2017f721",
    "totalEvents": 10
  },
  "blockNumber": 9254285,
  "shardIndex": 1
}
```

## events

Get a page of Hub events

**Query Parameters**
| Parameter     | Description                                                                                  | Example                         |
| ------------- | -------------------------------------------------------------------------------------------- | ------------------------------- |
| from_event_id | An optional Hub Id to start getting events from. Set it to `0` to start from the first event | `from_event_id=350909155450880` |
| shard_index   | Optional shard index to query                                                                | `shard_index=1`                 |
| stop_id       | Optional stop event ID                                                                       | `stop_id=350909170294785`       |
| pageSize      | Optional page size (default: 1000)                                                           | `pageSize=100`                  |
| pageToken     | Optional page token for pagination                                                           | `pageToken=DAEDAAAGlQ...`       |
| reverse       | Optional reverse order flag                                                                  | `reverse=true`                  |

**Note**
Hubs prune events older than 3 days, so not all historical events can be fetched via this API

**Example**

```bash
curl http://127.0.0.1:3381/v1/events?from_event_id=0

```

**Response**

```json
{
  "events": [
    {
      "type": "HUB_EVENT_TYPE_BLOCK_CONFIRMED",
      "id": 151622205440,
      "blockConfirmedBody": {
        "blockNumber": 9254285,
        "shardIndex": 1,
        "timestamp": 142732801,
        "blockHash": "0x95659381b61ac3cd9fc06e61d9d9c256f8274aaab526cb23ce72856c2017f721",
        "totalEvents": 10
      },
      "blockNumber": 9254285,
      "shardIndex": 1
    },
    {
      "type": "HUB_EVENT_TYPE_MERGE_MESSAGE",
      "id": 151622205441,
      "mergeMessageBody": {
        "message": {
          "data": {
            "type": "MESSAGE_TYPE_REACTION_ADD",
            "fid": 310826,
            "timestamp": 142732800,
            "network": "FARCASTER_NETWORK_MAINNET",
            "reactionBody": {
              "type": "REACTION_TYPE_LIKE",
              "targetCastId": {
                "fid": 1026688,
                "hash": "0xa1162b5d59281733daee1bcd3b810c5259f66ee1"
              }
            }
          },
          "hash": "0xfd4e55bb235fec5cad679182a2c926948d95b7cb",
          "hashScheme": "HASH_SCHEME_BLAKE3",
          "signature": "3N0jZHh46/gXa6uZS+jCbw/9eiOti3MyHNODn7cw5xqo7DBa45rixbzG2QNJtnDmF5XJb+q4GNv/eZF+19qQBw==",
          "signatureScheme": "SIGNATURE_SCHEME_ED25519",
          "signer": "0x217a69e523fbcc51643021d78f9a0fc98ac4e56c7418a2825f0870c81a5d18aa"
        },
        "deletedMessages": []
      },
      "blockNumber": 9254285,
      "shardIndex": 1
    }
  ]
}
```
