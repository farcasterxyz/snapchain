# On Chain API

## onChainSignersByFid

Get a list of account keys (signers) provided by an FID

**Query Parameters**
| Parameter | Description                        | Example                                                                     |
| --------- | ---------------------------------- | --------------------------------------------------------------------------- |
| fid       | The FID being requested            | `fid=2`                                                                     |
| signer    | The optional key of signer         | `signer=0x0852c07b5695ff94138b025e3f9b4788e06133f04e254f0ea0eb85a06e999cdd` |
| pageSize  | Optional page size (default: 1000) | `pageSize=100`                                                              |
| pageToken | Optional page token for pagination | `pageToken=DAEDAAAGlQ...`                                                   |
| reverse   | Optional reverse order flag        | `reverse=true`                                                              |

**Example**

```bash
curl http://127.0.0.1:3381/v1/onChainSignersByFid?fid=6833
```

**Response**

```json
{
  "events": [
    {
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
  ]
}
```

## onChainEventsByFid

Get a list of account keys provided by an FID

**Query Parameters**
| Parameter  | Description                                                                    | Example                                                                |
| ---------- | ------------------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| fid        | The FID being requested                                                        | `fid=2`                                                                |
| event_type | The string value of the event type being requested. This parameter is required | `event_type=EVENT_TYPE_SIGNER` OR `event_type=EVENT_TYPE_STORAGE_RENT` |
| pageSize   | Optional page size (default: 1000)                                             | `pageSize=100`                                                         |
| pageToken  | Optional page token for pagination                                             | `pageToken=DAEDAAAGlQ...`                                              |
| reverse    | Optional reverse order flag                                                    | `reverse=true`                                                         |

The onChainEventsByFid API will accept the following values for the `event_type` field.

| String                     |
| -------------------------- |
| EVENT_TYPE_NONE            |
| EVENT_TYPE_SIGNER          |
| EVENT_TYPE_SIGNER_MIGRATED |
| EVENT_TYPE_ID_REGISTER     |
| EVENT_TYPE_STORAGE_RENT    |
| EVENT_TYPE_TIER_PURCHASE   |

**Example**

```bash
curl http://127.0.0.1:3381/v1/onChainEventsByFid?fid=3&event_type=EVENT_TYPE_SIGNER
```

**Response**

```json
{
  "events": [
    {
      "type": "EVENT_TYPE_SIGNER",
      "chainId": 10,
      "blockNumber": 108875456,
      "blockHash": "0x75fbbb8b2a4ede67ac350e1b0503c6a152c0091bd8e3ef4a6927d58e088eae28",
      "blockTimestamp": 1693349689,
      "transactionHash": "0x36ef79e6c460e6ae251908be13116ff0065960adb1ae032b4cc65a8352f28952",
      "logIndex": 2,
      "fid": 3,
      "signerEventBody": {
        "key": "0xc887f5bf385a4718eaee166481f1832198938cf33e98a82dc81a0b4b81ffe33d",
        "keyType": 1,
        "eventType": "SIGNER_EVENT_TYPE_ADD",
        "metadata": "AAAAAAAAA...AAAAA",
        "metadataType": 1
      },
      "txIndex": 0
    }
  ]
}
```

## onChainIdRegistryEventByAddress

Get a list of on chain events for a given Address

**Query Parameters**
| Parameter | Description                     | Example                                              |
| --------- | ------------------------------- | ---------------------------------------------------- |
| address   | The ETH address being requested | `address=0x74232bf61e994655592747e20bdf6fa9b9476f79` |

**Example**

```bash
curl http://127.0.0.1:3381/v1/onChainIdRegistryEventByAddress?address=0x74232bf61e994655592747e20bdf6fa9b9476f79
```

**Response**

```json
{
  "type": "EVENT_TYPE_ID_REGISTER",
  "chainId": 10,
  "blockNumber": 108874508,
  "blockHash": "0x20d83804a26247ad8c26d672f2212b28268d145b8c1cefaa4126f7768f46682e",
  "blockTimestamp": 1693347793,
  "transactionHash": "0xf3481fc32227fbd982b5f30a87be32a2de1fc5736293cae7c3f169da48c3e764",
  "logIndex": 7,
  "fid": 3,
  "idRegisterEventBody": {
    "to": "0x74232bf61e994655592747e20bdf6fa9b9476f79",
    "eventType": "ID_REGISTER_EVENT_TYPE_REGISTER",
    "from": "0x",
    "recoveryAddress": "0x00000000fcd5a8e45785c8a4b9a718c9348e4f18"
  },
  "txIndex": 0
}
```

## fidAddressType

Get the address type information for a given FID and address

**Query Parameters**
| Parameter | Description              | Example                                              |
| --------- | ------------------------ | ---------------------------------------------------- |
| fid       | The FID being requested  | `fid=2`                                              |
| address   | The ETH address to check | `address=0x91031dcfdea024b4d51e775486111d2b2a715871` |

**Example**

```bash
curl http://127.0.0.1:3381/v1/fidAddressType?fid=2&address=0x91031dcfdea024b4d51e775486111d2b2a715871
```

**Response**

```json
{
  "is_custody": false,
  "is_auth": false,
  "is_verified": true
}
```
