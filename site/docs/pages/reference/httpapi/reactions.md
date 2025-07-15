# Reactions API

The Reactions API will accept the following values for the `reaction_type` field.

| String | Description                              |
| ------ | ---------------------------------------- |
| Like   | Like the target cast                     |
| Recast | Share target cast to the user's audience |

## reactionById

Get a reaction by its created FID and target Cast.

**Query Parameters**
| Parameter     | Description                                     | Example                                                  |
| ------------- | ----------------------------------------------- | -------------------------------------------------------- |
| fid           | The FID of the reaction's creator               | `fid=6833`                                               |
| target_fid    | The FID of the cast's creator                   | `target_fid=2`                                           |
| target_hash   | The cast's hash                                 | `target_hash=0xa48dd46161d8e57725f5e26e34ec19c13ff7f3b9` |
| reaction_type | The type of reaction, use string representation | `reaction_type=Like` OR `reaction_type=Recast`           |

**Example**

```bash
curl http://127.0.0.1:3381/v1/reactionById?fid=2&reaction_type=Like&target_fid=1795&target_hash=0x7363f449bfb0e7f01c5a1cc0054768ed5146abc0
```

**Response**

```json
{
  "data": {
    "type": "MESSAGE_TYPE_REACTION_ADD",
    "fid": 2,
    "timestamp": 72752656,
    "network": "FARCASTER_NETWORK_MAINNET",
    "reactionBody": {
      "type": "REACTION_TYPE_LIKE",
      "targetCastId": {
        "fid": 1795,
        "hash": "0x7363f449bfb0e7f01c5a1cc0054768ed5146abc0"
      }
    }
  },
  "hash": "0x9fc9c51f6ea3acb84184efa88ba4f02e7d161766",
  "hashScheme": "HASH_SCHEME_BLAKE3",
  "signature": "F2OzKsn6Wj...gtyORbyCQ==",
  "signatureScheme": "SIGNATURE_SCHEME_ED25519",
  "signer": "0x78ff9a7...647b6d62558c"
}
```

## reactionsByFid

Get all reactions by an FID

**Query Parameters**
| Parameter     | Description                                     | Example                                        |
| ------------- | ----------------------------------------------- | ---------------------------------------------- |
| fid           | The FID of the reaction's creator               | `fid=6833`                                     |
| reaction_type | The type of reaction, use string representation | `reaction_type=Like` OR `reaction_type=Recast` |
| pageSize      | Optional page size (default: 1000)              | `pageSize=100`                                 |
| pageToken     | Optional page token for pagination              | `pageToken=DAEDAAAGlQ...`                      |
| reverse       | Optional reverse order flag                     | `reverse=true`                                 |

**Example**

```bash
curl http://127.0.0.1:3381/v1/reactionsByFid?fid=2&reaction_type=Like
```

**Response**

```json
{
  "messages": [
    {
      "data": {
        "type": "MESSAGE_TYPE_REACTION_ADD",
        "fid": 2,
        "timestamp": 72752656,
        "network": "FARCASTER_NETWORK_MAINNET",
        "reactionBody": {
          "type": "REACTION_TYPE_LIKE",
          "targetCastId": {
            "fid": 1795,
            "hash": "0x7363f449bfb0e7f01c5a1cc0054768ed5146abc0"
          }
        }
      },
      "hash": "0x9fc9c51f6ea3acb84184efa88ba4f02e7d161766",
      "hashScheme": "HASH_SCHEME_BLAKE3",
      "signature": "F2OzKsn6WjP8MTw...hqUbrAvp6mggtyORbyCQ==",
      "signatureScheme": "SIGNATURE_SCHEME_ED25519",
      "signer": "0x78ff9a768...62558c"
    }
  ],
  "nextPageToken": ""
}
```

## reactionsByCast

Get all reactions to a cast

**Query Parameters**
| Parameter     | Description                                     | Example                                                  |
| ------------- | ----------------------------------------------- | -------------------------------------------------------- |
| target_fid    | The FID of the cast's creator                   | `target_fid=6833`                                        |
| target_hash   | The hash of the cast                            | `target_hash=0x7363f449bfb0e7f01c5a1cc0054768ed5146abc0` |
| reaction_type | The type of reaction, use string representation | `reaction_type=Like` OR `reaction_type=Recast`           |
| pageSize      | Optional page size (default: 1000)              | `pageSize=100`                                           |
| pageToken     | Optional page token for pagination              | `pageToken=DAEDAAAGlQ...`                                |
| reverse       | Optional reverse order flag                     | `reverse=true`                                           |

**Example**

```bash
curl http://127.0.0.1:3381/v1/reactionsByCast?target_fid=2&reaction_type=Like&target_hash=0x7363f449bfb0e7f01c5a1cc0054768ed5146abc0
```

**Response**

```json
{
  "messages": [
    {
      "data": {
        "type": "MESSAGE_TYPE_REACTION_ADD",
        "fid": 426,
        "timestamp": 72750141,
        "network": "FARCASTER_NETWORK_MAINNET",
        "reactionBody": {
          "type": "REACTION_TYPE_LIKE",
          "targetCastId": {
            "fid": 1795,
            "hash": "0x7363f449bfb0e7f01c5a1cc0054768ed5146abc0"
          }
        }
      },
      "hash": "0x7662fba1be3166fc75acc0914a7b0e53468d5e7a",
      "hashScheme": "HASH_SCHEME_BLAKE3",
      "signature": "tmAUEYlt/+...R7IO3CA==",
      "signatureScheme": "SIGNATURE_SCHEME_ED25519",
      "signer": "0x13dd2...204e57bc2a"
    }
  ],
  "nextPageToken": ""
}
```

## reactionsByTarget

Get all reactions to cast's target URL

**Query Parameters**
| Parameter     | Description                                     | Example                                                                  |
| ------------- | ----------------------------------------------- | ------------------------------------------------------------------------ |
| url           | The URL of the parent cast                      | `url=chain://eip155:1/erc721:0x39d89b649ffa044383333d297e325d42d31329b2` |
| reaction_type | The type of reaction, use string representation | `reaction_type=Like` OR `reaction_type=Recast`                           |

**Example**

```bash
curl http://127.0.0.1:3381/v1/reactionsByTarget?url=chain://eip155:1/erc721:0x39d89b649ffa044383333d297e325d42d31329b2
```

**Response**

```json
{
  "messages": [
    {
      "data": {
        "type": "MESSAGE_TYPE_REACTION_ADD",
        "fid": 1134,
        "timestamp": 79752856,
        "network": "FARCASTER_NETWORK_MAINNET",
        "reactionBody": {
          "type": "REACTION_TYPE_LIKE",
          "targetUrl": "chain://eip155:1/erc721:0x39d89b649ffa044383333d297e325d42d31329b2"
        }
      },
      "hash": "0x94a0309cf11a07b95ace71c62837a8e61f17adfd",
      "hashScheme": "HASH_SCHEME_BLAKE3",
      "signature": "+f/+M...0Uqzd0Ag==",
      "signatureScheme": "SIGNATURE_SCHEME_ED25519",
      "signer": "0xf6...3769198d4c"
    }
  ],
  "nextPageToken": ""
}
```
