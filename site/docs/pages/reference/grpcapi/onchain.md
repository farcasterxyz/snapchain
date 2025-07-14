# OnChainEvents API

Used to retrieve on chain events (id registry, keys, storage rent)

## API

| Method Name                        | Request Type                    | Response Type            | Description                                                                                              |
| ---------------------------------- | ------------------------------- | ------------------------ | -------------------------------------------------------------------------------------------------------- |
| GetOnChainSigner                   | SignerRequest                   | OnChainEvent             | Returns the onchain event for an active signer for an Fid                                                |
| GetOnChainSignersByFid             | FidRequest                      | OnChainEventResponse     | Returns all active account keys (signers) add events for an Fid                                          |
| GetIdRegistryOnChainEvent          | FidRequest                      | OnChainEvent             | Returns the most recent register/transfer on chain event for an fid                                      |
| GetIdRegistryOnChainEventByAddress | IdRegistryEventByAddressRequest | OnChainEvent             | Returns the registration/transfer event by address if it exists (allows looking up fid by address)       |
| GetOnChainEvents                   | OnChainEventRequest             | OnChainEventResponse     | Returns all on chain events filtered by type for an Fid (includes inactive keys and expired rent events) |
| GetFidAddressType                  | FidAddressTypeRequest           | FidAddressTypeResponse   | Returns address type information for a given fid and address                                             |

## Signer Request

| Field  | Type        | Label | Description                                       |
| ------ | ----------- | ----- | ------------------------------------------------- |
| fid    | [uint64](#) |       | Farcaster ID of the user who generated the Signer |
| signer | [bytes](#)  |       | Public Key of the Signer                          |

## Fid Request

| Field      | Type        | Label | Description                                 |
| ---------- | ----------- | ----- | ------------------------------------------- |
| fid        | [uint64](#) |       | Farcaster ID of the user                    |
| page_size  | uint32      |       | (optional) Type of the Link being requested |
| page_token | bytes       |       | (optional)Type of the Link being requested  |
| reverse    | boolean     |       | (optional) Ordering of the response         |

#### IdRegistryEventByAddressRequest

| Field   | Type            | Label | Description |
| ------- | --------------- | ----- | ----------- |
| address | [bytes](#bytes) |       |             |

#### OnChainEventResponse

| Field           | Type                          | Label    | Description |
| --------------- | ----------------------------- | -------- | ----------- |
| events          | [OnChainEvent](#onchainevent) | repeated |             |
| next_page_token | [bytes](#bytes)               | optional |             |

#### FidAddressTypeRequest

| Field   | Type            | Label | Description |
| ------- | --------------- | ----- | ----------- |
| fid     | [uint64](#)     |       | Farcaster ID |
| address | [bytes](#bytes) |       | Address to check |

#### FidAddressTypeResponse

| Field       | Type         | Label | Description |
| ----------- | ------------ | ----- | ----------- |
| is_custody  | [bool](#)    |       | Whether the address is a custody address |
| is_auth     | [bool](#)    |       | Whether the address is an auth address |
| is_verified | [bool](#)    |       | Whether the address is verified |
