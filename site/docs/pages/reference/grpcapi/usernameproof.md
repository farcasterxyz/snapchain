# Username Proofs API

Used to retrieve proofs of username ownership.

## API

| Method Name            | Request Type                                  | Response Type                                     | Description                         |
| ---------------------- | --------------------------------------------- | ------------------------------------------------- | ----------------------------------- |
| GetUsernameProof       | [UsernameProofRequest](#UsernameProofRequest) | [UserNameProof](#UserNameProof)                   | Gets username proof by name         |
| GetUserNameProofsByFid | [FidRequest](#FidRequest)                     | [UsernameProofsResponse](#UsernameProofsResponse) | Gets all username proofs for an FID |

## UsernameProofRequest

| Field | Type            | Label | Description               |
| ----- | --------------- | ----- | ------------------------- |
| name  | [bytes](#bytes) |       | Username to get proof for |

## UsernameProofsResponse

| Field  | Type                            | Label    | Description              |
| ------ | ------------------------------- | -------- | ------------------------ |
| proofs | [UserNameProof](#UserNameProof) | repeated | Array of username proofs |

## UserNameProof

| Field     | Type                          | Label | Description                            |
| --------- | ----------------------------- | ----- | -------------------------------------- |
| timestamp | [uint64](#uint64)             |       | Timestamp of the proof                 |
| name      | [bytes](#bytes)               |       | Username being proved                  |
| owner     | [bytes](#bytes)               |       | Owner address                          |
| signature | [bytes](#bytes)               |       | Cryptographic signature                |
| fid       | [uint64](#uint64)             |       | Farcaster ID associated with the proof |
| type      | [UserNameType](#UserNameType) |       | Type of username proof                 |
