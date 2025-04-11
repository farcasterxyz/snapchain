# Verifications API

Used to retrieve valid or revoked proof of ownership of an Ethereum Address.

## API

| Method Name                     | Request Type        | Response Type    | Description                                                 |
| ------------------------------- | ------------------- | ---------------- | ----------------------------------------------------------- |
| GetVerification                 | VerificationRequest | Message          | Returns a VerificationAdd for an Ethereum Address           |
| GetVerificationsByFid           | FidRequest          | MessagesResponse | Returns all VerificationAdds made by an Fid                 |
| GetAllVerificationMessagesByFid | FidTimestampRequest | MessagesResponse | Returns all Verifications made by an Fid with time filtering |

## Verification Request

| Field   | Type        | Label | Description                                             |
| ------- | ----------- | ----- | ------------------------------------------------------- |
| fid     | [uint64](#) |       | Farcaster ID of the user who generated the Verification |
| address | [bytes](#)  |       | Ethereum Address being verified                         |

## FidTimestampRequest

| Field            | Type              | Label    | Description                                    |
| ---------------- | ----------------- | -------- | ---------------------------------------------- |
| fid              | [uint64](#uint64) |          | Farcaster ID                                   |
| page_size        | [uint32](#uint32) | optional | Number of results to return per page           |
| page_token       | [bytes](#bytes)   | optional | Token for pagination                           |
| reverse          | [bool](#bool)     | optional | Whether to return results in reverse order     |
| start_timestamp  | [uint64](#uint64) | optional | Optional timestamp to start filtering from     |
| stop_timestamp   | [uint64](#uint64) | optional | Optional timestamp to stop filtering at        |
