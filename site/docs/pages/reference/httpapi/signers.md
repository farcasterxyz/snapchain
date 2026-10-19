# Signers API

## signersByFid

Get every signer registered for an FID, onchain and gasless, in one list.

This differs from [`onChainSignersByFid`](/reference/httpapi/onchain), which returns
only onchain registration events. `signersByFid` also reports the nonce and signer-limit
state needed to add or remove a gasless key.

**Query Parameters**
| Parameter     | Description                                             | Example                |
| ------------- | ------------------------------------------------------- | ---------------------- |
| fid           | The FID being requested                                 | `fid=2`                |
| requesterFids | Optional app FIDs whose app-nonces should be returned   | `requesterFids=9152`   |
| pageSize      | Optional page size                                      | `pageSize=100`         |
| pageToken     | Optional page token for pagination                      | `pageToken=DAEDAAAGlQ...` |
| reverse       | Optional reverse order flag                             | `reverse=true`         |

**Example**

```bash
curl http://127.0.0.1:3381/v1/signersByFid?fid=3&pageSize=1
```

**Response**

```json
{
  "signers": [
    {
      "source": "SIGNER_SOURCE_ONCHAIN",
      "key": "0xc887f5bf385a4718eaee166481f1832198938cf33e98a82dc81a0b4b81ffe33d",
      "keyType": 1,
      "fid": 3,
      "addedAt": 1699395643,
      "onChainEvent": {
        "type": "EVENT_TYPE_SIGNER",
        "chainId": 10,
        "blockNumber": 111898433,
        "fid": 3,
        "signerEventBody": {
          "key": "0xc887f5bf385a4718eaee166481f1832198938cf33e98a82dc81a0b4b81ffe33d",
          "keyType": 1,
          "eventType": "SIGNER_EVENT_TYPE_ADD",
          "metadata": "AAAAAAAAAAAA...AAAAAAAA",
          "metadataType": 1
        }
      }
    }
  ],
  "gaslessSignerCount": 0,
  "gaslessSignerLimit": 1000,
  "currentUserNonce": 0
}
```

**Fields**

| Field               | Description                                                                                                     |
| ------------------- | --------------------------------------------------------------------------------------------------------------- |
| `source`            | How the key was registered. `SIGNER_SOURCE_ONCHAIN` for keys added by a KeyRegistry transaction.                  |
| `currentUserNonce`  | The fid's user nonce. A custody-authorized `KEY_ADD` or `KEY_REMOVE` must use a nonce strictly greater than this. |
| `gaslessSignerCount`| Active gasless keys for this fid.                                                                                 |
| `gaslessSignerLimit`| Per-fid cap on active gasless keys.                                                                               |
| `requesterFidNonces`| App-nonces keyed by requester fid, for each fid passed in `requesterFids`. Missing counters are reported as `0`.   |

`metadata` is base64-encoded, unlike the hex used elsewhere in this API. It decodes to
an ABI-encoded `SignedKeyRequestMetadata`, whose `requestFid` identifies the app that
requested the key.

See [writing messages](/guides/writing-messages) for how these fields are used when
registering a gasless key.

## signer

Get a single signer for an FID, when the key is already known.

**Query Parameters**
| Parameter | Description                 | Example                                                                     |
| --------- | --------------------------- | --------------------------------------------------------------------------- |
| fid       | The FID being requested     | `fid=2`                                                                     |
| signer    | The key of the signer       | `signer=0x0852c07b5695ff94138b025e3f9b4788e06133f04e254f0ea0eb85a06e999cdd` |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/signer?fid=3&signer=0xc887f5bf385a4718eaee166481f1832198938cf33e98a82dc81a0b4b81ffe33d"
```
