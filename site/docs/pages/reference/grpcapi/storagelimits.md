# Storage API

Get an FID's storage limits.

## API

| Method Name                  | Request Type | Response Type         | Description                                              |
| ---------------------------- | ------------ | --------------------- | -------------------------------------------------------- |
| GetCurrentStorageLimitsByFid | FidRequest   | StorageLimitsResponse | Returns current storage limits for all stores for an Fid |

#### StorageLimitsResponse

| Field             | Type                      | Label    | Description                   |
| ----------------- | ------------------------- | -------- | ----------------------------- |
| limits            | [StorageLimit](#)         | repeated | Storage limits per store type |
| units             | [uint32](#)               |          | Number of units               |
| unit_details      | [StorageUnitDetails](#)   | repeated | Details about storage units   |
| tier_subscriptions| [TierDetails](#)          | repeated | Tier subscription details     |

#### StorageLimit

| Field             | Type           | Label | Description                                            |
| ----------------- | -------------- | ----- | ------------------------------------------------------ |
| store_type        | [StoreType](#) |       | The specific type being managed by the store           |
| name              | [string](#)    |       | Name of the store type                                 |
| limit             | [uint64](#)    |       | The limit of the store type, scaled by the user's rent |
| used              | [uint64](#)    |       | Current usage of the store type                        |
| earliestTimestamp | [uint64](#)    |       | Timestamp of earliest message                          |
| earliestHash      | [bytes](#)     |       | Hash of earliest message                               |

#### StorageUnitDetails

| Field      | Type                      | Label | Description                    |
| ---------- | ------------------------- | ----- | ------------------------------ |
| unit_type  | [StorageUnitType](#)      |       | Type of storage unit           |
| unit_size  | [uint32](#)               |       | Size of the storage unit       |

#### TierDetails

| Field      | Type           | Label | Description                    |
| ---------- | -------------- | ----- | ------------------------------ |
| tier_type  | [TierType](#)  |       | Type of tier                   |
| expires_at | [uint64](#)    |       | Expiration timestamp           |
