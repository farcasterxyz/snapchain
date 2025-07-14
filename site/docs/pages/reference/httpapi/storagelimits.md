# Storage API

## storageLimitsByFid

Get an FID's storage limits.

**Query Parameters**
| Parameter | Description                    | Example    |
| --------- | ------------------------------ | ---------- |
| fid       | The FID that's being requested | `fid=6833` |

**Example**

```bash
curl http://127.0.0.1:3381/v1/storageLimitsByFid?fid=6833
```

**Response**

```json
{
  "limits": [
    {
      "storeType": "Casts",
      "name": "CASTS",
      "limit": 77000,
      "used": 10510,
      "earliestTimestamp": 0,
      "earliestHash": []
    },
    {
      "storeType": "Links",
      "name": "LINKS",
      "limit": 38500,
      "used": 1742,
      "earliestTimestamp": 0,
      "earliestHash": []
    },
    {
      "storeType": "Reactions",
      "name": "REACTIONS",
      "limit": 38500,
      "used": 19578,
      "earliestTimestamp": 0,
      "earliestHash": []
    },
    {
      "storeType": "UserData",
      "name": "USER_DATA",
      "limit": 800,
      "used": 8,
      "earliestTimestamp": 0,
      "earliestHash": []
    },
    {
      "storeType": "Verifications",
      "name": "VERIFICATIONS",
      "limit": 400,
      "used": 7,
      "earliestTimestamp": 0,
      "earliestHash": []
    },
    {
      "storeType": "UsernameProofs",
      "name": "USERNAME_PROOFS",
      "limit": 80,
      "used": 1,
      "earliestTimestamp": 0,
      "earliestHash": []
    }
  ],
  "units": 515,
  "unit_details": [
    {
      "unitType": "UnitTypeLegacy",
      "unitSize": 15
    },
    {
      "unitType": "UnitType2024",
      "unitSize": 1
    },
    {
      "unitType": "UnitType2025",
      "unitSize": 0
    }
  ],
  "tier_subscriptions": [
    {
      "tier_type": "Pro",
      "expires_at": 1781630485
    }
  ]
}
```
