# Signers API

Account keys (signers) for an FID, from both sources in one list: keys added onchain through the KeyRegistry, and gasless keys added with a `KEY_ADD` message. These replace `GetOnChainSigner` and `GetOnChainSignersByFid`, which only return onchain keys and are deprecated.

## API

| Method Name     | Request Type        | Response Type        | Description                                                                          |
| --------------- | ------------------- | -------------------- | ------------------------------------------------------------------------------------ |
| GetSigner       | SignerRequest       | SignerResponse       | Returns one active signer of an Fid, onchain or gasless                              |
| GetSignersByFid | SignersByFidRequest | SignersByFidResponse | Returns all active signers of an Fid, with the nonces needed to add or revoke a key |

## SignerRequest

| Field  | Type        | Label | Description                   |
| ------ | ----------- | ----- | ----------------------------- |
| fid    | [uint64](#) |       | Farcaster ID of the user      |
| signer | [bytes](#)  |       | Public key of the signer      |

## SignersByFidRequest

| Field          | Type        | Label    | Description                                                                 |
| -------------- | ----------- | -------- | --------------------------------------------------------------------------- |
| fid            | [uint64](#) |          | Farcaster ID of the user                                                    |
| page_size      | uint32      | optional | Page size                                                                   |
| page_token     | bytes       | optional | Page token for pagination                                                   |
| reverse        | boolean     | optional | Ordering of the response                                                    |
| requester_fids | uint64      | repeated | FIDs whose current app nonce should be returned in `requester_fid_nonces` |

## SignerResponse

| Field  | Type              | Label | Description |
| ------ | ----------------- | ----- | ----------- |
| signer | [Signer](#signer) |       |             |

## SignersByFidResponse

| Field                | Type                  | Label    | Description                                                                                                         |
| -------------------- | --------------------- | -------- | ------------------------------------------------------------------------------------------------------------------- |
| signers              | [Signer](#signer)     | repeated |                                                                                                                     |
| next_page_token      | bytes                 | optional |                                                                                                                     |
| gasless_signer_count | uint32                |          | Active gasless keys for this Fid, independent of pagination                                                        |
| gasless_signer_limit | uint32                |          | Maximum active gasless keys per Fid                                                                                 |
| current_user_nonce   | uint32                |          | Highest nonce accepted on a `KEY_ADD` or custody-signed `KEY_REMOVE`. The next one must be strictly greater         |
| requester_fid_nonces | `map<uint64, uint32>` |          | Current app nonce for each Fid in `requester_fids`. A key's self-signed `KEY_REMOVE` must use a greater nonce       |

## Signer

Fields that don't apply to a signer's source are left unset. Timestamps are Unix epoch seconds for both sources.

| Field         | Type                          | Label    | Description                                                              |
| ------------- | ----------------------------- | -------- | ------------------------------------------------------------------------ |
| source        | SignerSource                  |          | `SIGNER_SOURCE_ONCHAIN` or `SIGNER_SOURCE_OFFCHAIN` (gasless)            |
| key           | bytes                         |          | 32-byte Ed25519 public key                                               |
| key_type      | uint32                        |          | 1 = Ed25519                                                              |
| fid           | uint64                        |          | Farcaster ID of the user                                                 |
| added_at      | uint64                        | optional | When the key was added (block timestamp for onchain keys)                |
| last_used_at  | uint64                        | optional | Gasless only. When the key last signed a message                         |
| ttl           | uint32                        | optional | Gasless only. Expiry window in seconds                                   |
| expires_at    | uint64                        | optional | Gasless only. `last_used_at + ttl` when `ttl > 0`                        |
| scopes        | int32                         | repeated | Gasless only. Message types the key may sign                             |
| request_fid   | uint64                        | optional | Gasless only. The app Fid that requested the key                         |
| nonce         | uint32                        | optional | Gasless only. User nonce of the `KEY_ADD` that added the key             |
| onchain_event | [OnChainEvent](#onchainevent) | optional | Onchain only. The original KeyRegistry event                             |
