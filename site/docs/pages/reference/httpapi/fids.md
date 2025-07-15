# Fids API

## fids

Get a list of all the FIDs

**Query Parameters**
| Parameter | Description                        | Example                   |
| --------- | ---------------------------------- | ------------------------- |
| shard_id  | Required shard ID to query         | `shard_id=1`              |
| pageSize  | Optional page size (default: 1000) | `pageSize=100`            |
| pageToken | Optional page token for pagination | `pageToken=DAEDAAAGlQ...` |
| reverse   | Optional reverse order flag        | `reverse=true`            |

**Example**

```bash
curl http://127.0.0.1:3381/v1/fids?shard_id=1
```

**Response**

```json
{
  "fids": [1, 3, 5, 7, 11, 12, 15, 17, 18, 19, 21, 22, 23, 30, 31, 33, 36, 37, 41, 43, 47, 48, 52, 54, 55, 57, 58, 60, 62, 65, 67, 68, 70, 77, 79, 81, 85, 86, 87, 88, 89, 91, 92, 93, 94, 95, 96, 97, 99, 106],
  "nextPageToken": "DAEDAAAGlQarXegAAACK"
}
```
