# Signers API

Account keys (signers) for an FID, from both sources in one list: keys added onchain through the KeyRegistry, and gasless keys added with a `KEY_ADD` message. Use these endpoints instead of `onChainSignersByFid`, which only returns onchain keys and is deprecated.

Every signer has a `source`: `SIGNER_SOURCE_ONCHAIN` or `SIGNER_SOURCE_OFFCHAIN` (gasless). Fields that don't apply to a source are left out rather than zeroed. Timestamps are Unix epoch seconds for both sources.

## signersByFid

Get the active signers of an FID, plus the nonces you need to add or revoke a gasless key.

**Query Parameters**
| Parameter     | Description                                                                                         | Example                       |
| ------------- | --------------------------------------------------------------------------------------------------- | ----------------------------- |
| fid           | The FID being requested                                                                             | `fid=6833`                    |
| pageSize      | Optional page size                                                                                  | `pageSize=100`                |
| pageToken     | Optional page token for pagination                                                                  | `pageToken=DAEDAAAGlQ...`     |
| reverse       | Optional reverse order flag                                                                         | `reverse=true`                |
| requesterFids | Optional. FIDs whose current app nonce should be returned in `requesterFidNonces`. Repeat for more. | `requesterFids=9152`          |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/signersByFid?fid=6833&requesterFids=9152"
```

**Response**

```json
{
  "signers": [
    {
      "source": "SIGNER_SOURCE_ONCHAIN",
      "key": "0x0852c07b5695ff94138b025e3f9b4788e06133f04e254f0ea0eb85a06e999cdd",
      "keyType": 1,
      "fid": 6833,
      "addedAt": 1693350485,
      "onChainEvent": {
        "type": "EVENT_TYPE_SIGNER",
        "chainId": 10,
        "blockNumber": 108875854,
        "blockHash": "0xceb1cdc21ee319b06f0455f1cedc0cd4669b471d283a5b2550b65aba0e0c1af0",
        "blockTimestamp": 1693350485,
        "transactionHash": "0x76e20cf2f7c3db4b78f00f6bb9a7b78b0acfb1eca4348c1f4b5819da66eb2bee",
        "logIndex": 2,
        "fid": 6833,
        "signerEventBody": {
          "key": "0x0852c07b5695ff94138b025e3f9b4788e06133f04e254f0ea0eb85a06e999cdd",
          "keyType": 1,
          "eventType": "SIGNER_EVENT_TYPE_ADD",
          "metadata": "AAAAAAAAAAAA...AAAAAAAA",
          "metadataType": 1
        },
        "txIndex": 0
      }
    },
    {
      "source": "SIGNER_SOURCE_OFFCHAIN",
      "key": "0x8e6bafe5f0d67f0253e9e7b9d0fe10748e37b29216f7d633c15eedbd4be84da8",
      "keyType": 1,
      "fid": 6833,
      "addedAt": 1792141960,
      "lastUsedAt": 1792605791,
      "ttl": 7776000,
      "expiresAt": 1800381791,
      "scopes": [
        "MESSAGE_TYPE_CAST_ADD",
        "MESSAGE_TYPE_CAST_REMOVE",
        "MESSAGE_TYPE_REACTION_ADD",
        "MESSAGE_TYPE_REACTION_REMOVE"
      ],
      "requestFid": 9152,
      "nonce": 3
    }
  ],
  "gaslessSignerCount": 1,
  "gaslessSignerLimit": 1000,
  "currentUserNonce": 3,
  "requesterFidNonces": {
    "9152": 0
  }
}
```

| Field                | Description                                                                                                                                                   |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `gaslessSignerCount` | Active gasless keys for this FID, independent of pagination                                                                                                   |
| `gaslessSignerLimit` | Maximum active gasless keys per FID                                                                                                                           |
| `currentUserNonce`   | Highest nonce accepted on a `KEY_ADD` or custody-signed `KEY_REMOVE` for this FID. The next one must be strictly greater. `0` if the FID has no gasless keys yet |
| `requesterFidNonces` | Current app nonce for each FID in `requesterFids`, keyed by FID as a string. A key's self-signed `KEY_REMOVE` must use a nonce strictly greater than its `requestFid`'s value |
| `nextPageToken`      | Present when there are more results                                                                                                                           |

For a gasless signer, `expiresAt` is `lastUsedAt + ttl`: the expiry slides forward every time the key signs a message. `scopes` lists the message types the key may sign, and `requestFid` is the app that requested it.

## signer

Get one signer of an FID by its public key.

**Query Parameters**
| Parameter | Description              | Example                                                                     |
| --------- | ------------------------ | --------------------------------------------------------------------------- |
| fid       | The FID being requested  | `fid=6833`                                                                  |
| signer    | The signer's public key  | `signer=0x8e6bafe5f0d67f0253e9e7b9d0fe10748e37b29216f7d633c15eedbd4be84da8` |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/signer?fid=6833&signer=0x8e6bafe5f0d67f0253e9e7b9d0fe10748e37b29216f7d633c15eedbd4be84da8"
```

**Response**

```json
{
  "signer": {
    "source": "SIGNER_SOURCE_OFFCHAIN",
    "key": "0x8e6bafe5f0d67f0253e9e7b9d0fe10748e37b29216f7d633c15eedbd4be84da8",
    "keyType": 1,
    "fid": 6833,
    "addedAt": 1792141960,
    "lastUsedAt": 1792605791,
    "ttl": 7776000,
    "expiresAt": 1800381791,
    "scopes": [
      "MESSAGE_TYPE_CAST_ADD",
      "MESSAGE_TYPE_CAST_REMOVE",
      "MESSAGE_TYPE_REACTION_ADD",
      "MESSAGE_TYPE_REACTION_REMOVE"
    ],
    "requestFid": 9152,
    "nonce": 3
  }
}
```
