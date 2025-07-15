# UserData API

The UserData API will accept the following values for the `user_data_type` field.

| String                  | Numerical value | Description                   |
| ----------------------- | --------------- | ----------------------------- |
| USER_DATA_TYPE_PFP      | 1               | Profile Picture for the user  |
| USER_DATA_TYPE_DISPLAY  | 2               | Display Name for the user     |
| USER_DATA_TYPE_BIO      | 3               | Bio for the user              |
| USER_DATA_TYPE_URL      | 5               | URL of the user               |
| USER_DATA_TYPE_USERNAME | 6               | Preferred Name for the user   |
| USER_DATA_TYPE_LOCATION | 7               | Location for the user         |
| USER_DATA_TYPE_TWITTER  | 8               | Twitter username for the user |
| USER_DATA_TYPE_GITHUB   | 9               | GitHub username for the user  |

See [FIP-196](https://github.com/farcasterxyz/protocol/discussions/196) for more information on Location.
See [FIP-19](https://github.com/farcasterxyz/protocol/discussions/199) for more information on Twitter/X and Github usernames.

## userDataByFid

Get UserData for a FID.

**Query Parameters**
| Parameter      | Description                                                                                                                  | Example                                                       |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| fid            | The FID that's being requested                                                                                               | `fid=6833`                                                    |
| user_data_type | The type of user data, either as a numerical value or type string. If this is omitted, all user data for the FID is returned | `user_data_type=1` OR `user_data_type=USER_DATA_TYPE_DISPLAY` |
| pageSize       | Optional page size (default: 1000)                                                                                           | `pageSize=100`                                                |
| pageToken      | Optional page token for pagination                                                                                           | `pageToken=DAEDAAAGlQ...`                                     |
| reverse        | Optional reverse order flag                                                                                                  | `reverse=true`                                                |

**Example**

```bash
curl http://127.0.0.1:3381/v1/userDataByFid?fid=6833&user_data_type=1
```

**Response**

```json
{
  "data": {
    "type": "MESSAGE_TYPE_USER_DATA_ADD",
    "fid": 6833,
    "timestamp": 83433831,
    "network": "FARCASTER_NETWORK_MAINNET",
    "userDataBody": {
      "type": "USER_DATA_TYPE_PFP",
      "value": "https://i.imgur.com/HG54Hq6.png"
    }
  },
  "hash": "0x327b8f47218c369ae01cc453cc23efc79f10181f",
  "hashScheme": "HASH_SCHEME_BLAKE3",
  "signature": "XITQZD7q...LdAlJ9Cg==",
  "signatureScheme": "SIGNATURE_SCHEME_ED25519",
  "signer": "0x0852...6e999cdd"
}
```
