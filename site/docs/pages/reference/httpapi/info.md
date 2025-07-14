# Info API

## info

Get the Hub's info including database stats and shard information

**Query Parameters**
None

**Example**

```bash
curl http://127.0.0.1:3381/v1/info

```

**Response**

```json
{
  "version": "0.4.0",
  "dbStats": {
    "numMessages": 656823920,
    "numFidRegistrations": 1049519,
    "approxSize": 324437755099
  },
  "peerId": "12D3KooWNr294AH1fviDQxRmQ4K79iFSGoRCWzGspVxPprJUKN47",
  "numShards": 2,
  "shardInfos": [
    {
      "shardId": 0,
      "maxHeight": 1932723,
      "numMessages": 0,
      "numFidRegistrations": 0,
      "approxSize": 1114006028,
      "blockDelay": 5,
      "mempoolSize": 0
    },
    {
      "shardId": 2,
      "maxHeight": 1945363,
      "numMessages": 326936052,
      "numFidRegistrations": 523905,
      "approxSize": 161319650355,
      "blockDelay": 6,
      "mempoolSize": 4294967295
    },
    {
      "shardId": 1,
      "maxHeight": 1966901,
      "numMessages": 329887868,
      "numFidRegistrations": 525614,
      "approxSize": 163118104744,
      "blockDelay": 5,
      "mempoolSize": 4294967295
    }
  ]
}
```
