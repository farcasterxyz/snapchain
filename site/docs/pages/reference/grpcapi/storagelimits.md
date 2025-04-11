# Storage API

Get an FID's storage limits.

## API

| Method Name                  | Request Type | Response Type         | Description                                              |
| ---------------------------- | ------------ | --------------------- | -------------------------------------------------------- |
| GetCurrentStorageLimitsByFid | FidRequest   | StorageLimitsResponse | Returns current storage limits for all stores for an Fid |

#### StorageLimitsResponse

| Field  | Type              | Label    | Description                   |
| ------ | ----------------- | -------- | ----------------------------- |
| limits | [StorageLimit](#) | repeated | Storage limits per store type |

#### StorageLimit

| Field      | Type           | Label | Description                                            |
| ---------- | -------------- | ----- | ------------------------------------------------------ |
| store_type | [StoreType](#) |       | The specific type being managed by the store           |
| limit      | [uint64](#)    |       | The limit of the store type, scaled by the user's rent |
