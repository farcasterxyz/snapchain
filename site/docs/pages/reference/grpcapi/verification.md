# Verifications API

Used to retrieve valid or revoked proof of ownership of an Ethereum Address.

## API

| Method Name                     | Request Type        | Response Type    | Description                                       |
| ------------------------------- | ------------------- | ---------------- | ------------------------------------------------- |
| GetVerification                 | VerificationRequest | Message          | Returns a VerificationAdd for an Ethereum Address |
| GetVerificationsByFid           | FidRequest          | MessagesResponse | Returns all VerificationAdds made by an Fid       |
| GetAllVerificationMessagesByFid | FidRequest          | MessagesResponse | Returns all Verifications made by an Fid          |

## Verification Request

| Field   | Type        | Label | Description                                             |
| ------- | ----------- | ----- | ------------------------------------------------------- |
| fid     | [uint64](#) |       | Farcaster ID of the user who generated the Verification |
| address | [bytes](#)  |       | Ethereum Address being verified                         |
