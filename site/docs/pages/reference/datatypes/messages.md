# Messages

A message is the fundamental data type in the Farcaster network.

When an account takes an action like casting a public message, change its profile or verifying an Ethereum account, it
generates a new messages

## 1. Message

The message is a protobuf that contains the data, its hash and a signature from the author.

| Field            | Type                                | Label | Description                                                                                                                                                                                                                                                      |
| ---------------- | ----------------------------------- | ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| data             | [MessageData](#MessageData)         |       | Contents of the message. Alternatively, you can use the data_bytes to serialize the `MessageData`                                                                                                                                                                |
| hash             | bytes                               |       | Hash digest of data                                                                                                                                                                                                                                              |
| hash_scheme      | [HashScheme](#HashScheme)           |       | Hash scheme that produced the hash digest                                                                                                                                                                                                                        |
| signature        | bytes                               |       | Signature of the hash digest                                                                                                                                                                                                                                     |
| signature_scheme | [SignatureScheme](#SignatureScheme) |       | Signature scheme that produced the signature                                                                                                                                                                                                                     |
| signer           | bytes                               |       | Public key or address of the key pair that produced the signature                                                                                                                                                                                                |
| data_bytes       | bytes                               |       | Alternate to the "data" field. If you are constructing the [MessageData](#MessageData) in a programming language other than Typescript, you can use this field to serialize the `MessageData` and calculate the `hash` and `signature` on these bytes. Optional. |

### 1.1 MessageData

MessageData is a generic envelope that contains a type, fid, timestamp and network which must be present in all
Farcaster messages. It also contains a body whose type is determined by the MessageType.

| Field     | Type                                                                                                                                                                                                                                                                                                                                    | Label | Description                                    |
| --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- | ---------------------------------------------- |
| type      | [MessageType](#MessageType)                                                                                                                                                                                                                                                                                                             |       | Type of Message contained in the body          |
| fid       | uint64                                                                                                                                                                                                                                                                                                                                  |       | Farcaster ID of the user producing the message |
| timestamp | uint32                                                                                                                                                                                                                                                                                                                                  |       | Farcaster epoch timestamp in seconds           |
| network   | [FarcasterNetwork](#FarcasterNetwork)                                                                                                                                                                                                                                                                                                   |       | Farcaster network the message is intended for  |
| body      | [CastAddBody](#CastAddBody), <br /> [CastRemoveBody](#CastRemoveBody), <br /> [ReactionBody](#ReactionBody), <br />[VerificationAddEthAddressBody](#VerificationAddEthAddressBody), <br />[VerificationRemoveBody](#VerificationRemoveBody), <br /> [UserDataBody](#UserDataBody),<br /> [LinkBody](#LinkBody),<br /> [UserNameProof](#UserNameProof) | oneOf | Properties specific to the MessageType         |

### 1.2 HashScheme

Type of hashing scheme used to produce a digest of MessageData

| Name               | Number | Description                            |
| ------------------ | ------ | -------------------------------------- |
| HASH_SCHEME_NONE   | 0      |                                        |
| HASH_SCHEME_BLAKE3 | 1      | Default scheme for hashing MessageData |

### 1.3 Signature Scheme

Type of signature scheme used to sign the Message hash

| Name                     | Number | Description                          |
| ------------------------ | ------ | ------------------------------------ |
| SIGNATURE_SCHEME_NONE    | 0      |                                      |
| SIGNATURE_SCHEME_ED25519 | 1      | Ed25519 signature (default)          |
| SIGNATURE_SCHEME_EIP712  | 2      | ECDSA signature using EIP-712 scheme |

### 1.4 Message Type

Type of the MessageBody

| Name                                      | Number | Description                               |
| ----------------------------------------- | ------ | ----------------------------------------- |
| MESSAGE_TYPE_NONE                         | 0      | Invalid default value                     |
| MESSAGE_TYPE_CAST_ADD                     | 1      | Add a new Cast                            |
| MESSAGE_TYPE_CAST_REMOVE                  | 2      | Remove an existing Cast                   |
| MESSAGE_TYPE_REACTION_ADD                 | 3      | Add a Reaction to a Cast                  |
| MESSAGE_TYPE_REACTION_REMOVE              | 4      | Remove a Reaction from a Cast             |
| MESSAGE_TYPE_LINK_ADD                     | 5      | Add a Link to a target                    |
| MESSAGE_TYPE_LINK_REMOVE                  | 6      | Remove a Link from a target               |
| MESSAGE_TYPE_VERIFICATION_ADD_ETH_ADDRESS | 7      | Add a Verification of an Ethereum Address |
| MESSAGE_TYPE_VERIFICATION_REMOVE          | 8      | Remove a Verification                     |
| MESSAGE_TYPE_USER_DATA_ADD                | 11     | Add metadata about a user                 |
| MESSAGE_TYPE_USERNAME_PROOF               | 12     | Add or replace a username proof           |

### 1.5 Farcaster Network

Farcaster network the message is intended for

| Name                      | Number | Description            |
| ------------------------- | ------ | ---------------------- |
| FARCASTER_NETWORK_NONE    | 0      |                        |
| FARCASTER_NETWORK_MAINNET | 1      | Public primary network |
| FARCASTER_NETWORK_TESTNET | 2      | Public test network    |
| FARCASTER_NETWORK_DEVNET  | 3      | Private test network   |

## 2. UserData

A UserData message represents user metadata (e.g. a profile picture url) .

### 2.1 UserDataBody

Body of a UserData message

| Field | Type                          | Label | Description           |
| ----- | ----------------------------- | ----- | --------------------- |
| type  | [UserDataType](#UserDataType) |       | Type of metadata      |
| value | string                        |       | Value of the metadata |

### 2.2 UserDataType

Type of UserData message

| Name                    | Number | Description                           |
| ----------------------- | ------ | ------------------------------------- |
| USER_DATA_TYPE_NONE     | 0      | Invalid default value                 |
| USER_DATA_TYPE_PFP      | 1      | Profile Picture for the user          |
| USER_DATA_TYPE_DISPLAY  | 2      | Display Name for the user             |
| USER_DATA_TYPE_BIO      | 3      | Bio for the user                      |
| USER_DATA_TYPE_URL      | 5      | URL of the user                       |
| USER_DATA_TYPE_USERNAME | 6      | Preferred Farcaster Name for the user |
| USER_DATA_TYPE_LOCATION | 7      | Location for the user                 |
| USER_DATA_TYPE_TWITTER  | 8      | Twitter username for the user         |
| USER_DATA_TYPE_GITHUB   | 9      | GitHub username for the user          |

See [FIP-196](https://github.com/farcasterxyz/protocol/discussions/196) for more information on Location.
See [FIP-19](https://github.com/farcasterxyz/protocol/discussions/199) for more information on Twitter/X and Github usernames.

## 3. Cast

A Cast message is a public post from a user.

### 3.1 CastAddBody

Adds a new Cast message.

| Field              | Type              | Label    | Description                                 |
| ------------------ | ----------------- | -------- | ------------------------------------------- |
| embeds_deprecated  | string            | repeated | URLs to be embedded in the cast             |
| mentions           | uint64            | repeated | Fids mentioned in the cast                  |
| parent_cast_id     | [CastId](#CastId) |          | Parent cast of the cast                     |
| parent_url         | string            |          | Parent URL of the cast                      |
| text               | string            |          | Text of the cast                            |
| mentions_positions | uint32            | repeated | Positions of the mentions in the text       |
| embeds             | [Embed](#Embed)   | repeated | URLs or cast ids to be embedded in the cast |

#### Embed

| Field   | Type              | Label | Description |
| ------- | ----------------- | ----- | ----------- |
| url     | [string](#string) |       |             |
| cast_id | [CastId](#CastId) |       |             |

### 3.2 CastRemoveBody

Removes an existing Cast message.

| Field       | Type  | Label | Description                |
| ----------- | ----- | ----- | -------------------------- |
| target_hash | bytes |       | Hash of the cast to remove |

### 3.3 CastId

Identifier used to look up a Cast

| Field | Type   | Label | Description                          |
| ----- | ------ | ----- | ------------------------------------ |
| fid   | uint64 |       | Fid of the user who created the cast |
| hash  | bytes  |       | Hash of the cast                     |

## 4. Reaction

A Reaction message creates a relationship between an account and a cast. (e.g. like)

### 4.1 ReactionBody

Adds or removes a Reaction from a Cast

| Field          | Type                          | Label | Description                    |
| -------------- | ----------------------------- | ----- | ------------------------------ |
| type           | [ReactionType](#ReactionType) |       | Type of reaction               |
| target_cast_id | [CastId](#CastId)             |       | CastId of the Cast to react to |
| target_url     | [string](#string)             |       | URL to react to                |

### 4.2 ReactionType

Type of Reaction

| Name                 | Number | Description                                  |
| -------------------- | ------ | -------------------------------------------- |
| REACTION_TYPE_NONE   | 0      | Invalid default value                        |
| REACTION_TYPE_LIKE   | 1      | Like the target cast                         |
| REACTION_TYPE_RECAST | 2      | Share target cast to the user&#39;s audience |

## 5. Link

A Link message creates a relationship between two users (e.g. follow)

### 5.1 LinkBody

Adds or removes a Link

| Field            | Type              | Label    | Description                                                                                                             |
| ---------------- | ----------------- | -------- | ----------------------------------------------------------------------------------------------------------------------- |
| type             | [string](#string) |          | Type of link, &lt;= 8 characters                                                                                        |
| displayTimestamp | [uint32](#uint32) | optional | User-defined timestamp that preserves original timestamp when message.data.timestamp needs to be updated for compaction |
| target_fid       | [uint64](#uint64) |          | The fid the link relates to                                                                                             |

## 6. Verification

A Verification message is a proof of ownership of something.

### 6.1 VerificationAddEthAddressBody

Adds a bi-directional signature proving that an fid has control over an Ethereum address.

| Field         | Type  | Label | Description                                                   |
| ------------- | ----- | ----- | ------------------------------------------------------------- |
| address       | bytes |       | Ethereum address being verified                               |
| eth_signature | bytes |       | Signature produced by the user&#39;s Ethereum address         |
| block_hash    | bytes |       | Hash of the latest Ethereum block when the claim was produced |

### 6.2 VerificationRemoveBody

Removes a Verification of any type

| Field   | Type  | Label | Description                           |
| ------- | ----- | ----- | ------------------------------------- |
| address | bytes |       | Address of the Verification to remove |

## 7. Frame Action

Represents an action performed by a user on a frame. This message is not stored on the nodes and cannot be submitted,
only validated.

### 7.1 FrameActionBody

A user action on a frame

| Field        | Type              | Label | Description                                           |
| ------------ | ----------------- | ----- | ----------------------------------------------------- |
| url          | bytes             |       | The original url of the frame as embedded in the cast |
| button_index | uint32            |       | The button that was pressed (indexed from 1)          |
| cast_id      | [CastId](#CastId) |       | The cast id that hosted the frame                     |
| input_text   | bytes             |       | Any text the user input as part of the action         |
| state        | bytes             |       | Serialized state passed from frame to server          |
