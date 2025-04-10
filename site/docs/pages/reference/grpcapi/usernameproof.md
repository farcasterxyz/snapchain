# Username Proofs API

## API

| Method Name            | Request Type                                  | Response Type                                     | Description    |
| ---------------------- | --------------------------------------------- | ------------------------------------------------- | -------------- |
| GetUsernameProof       | [UsernameProofRequest](#UsernameProofRequest) | [UserNameProof](#UserNameProof)                   | Username Proof |
| GetUserNameProofsByFid | [FidRequest](#FidRequest)                     | [UsernameProofsResponse](#UsernameProofsResponse) |                |

## UsernameProofRequest

| Field | Type            | Label | Description |
| ----- | --------------- | ----- | ----------- |
| name  | [bytes](#bytes) |       |             |

## UsernameProofsResponse

| Field  | Type                            | Label    | Description |
| ------ | ------------------------------- | -------- | ----------- |
| proofs | [UserNameProof](#UserNameProof) | repeated |             |

## UserNameProof

| Field     | Type                          | Label | Description |
| --------- | ----------------------------- | ----- | ----------- |
| timestamp | [uint64](#uint64)             |       |             |
| name      | [bytes](#bytes)               |       |             |
| owner     | [bytes](#bytes)               |       |             |
| signature | [bytes](#bytes)               |       |             |
| fid       | [uint64](#uint64)             |       |             |
| type      | [UserNameType](#UserNameType) |       |             |

## UserNameProof

| Field     | Type                          | Label | Description |
| --------- | ----------------------------- | ----- | ----------- |
| timestamp | [uint64](#uint64)             |       |             |
| name      | [bytes](#bytes)               |       |             |
| owner     | [bytes](#bytes)               |       |             |
| signature | [bytes](#bytes)               |       |             |
| fid       | [uint64](#uint64)             |       |             |
| type      | [UserNameType](#UserNameType) |       |             |
