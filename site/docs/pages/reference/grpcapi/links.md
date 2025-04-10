# Links API

A Link represents a relationship between two users (e.g. follow)

## API

| Method Name                | Request Type         | Response Type    | Description                                                |
| -------------------------- | -------------------- | ---------------- | ---------------------------------------------------------- |
| GetLink                    | LinkRequest          | Message          | Returns a specific Link                                    |
| GetLinksByFid              | LinksByFidRequest    | MessagesResponse | Returns Links made by an fid in reverse chron order        |
| GetLinksByTarget           | LinksByTargetRequest | MessagesResponse | Returns LinkAdds for a given target in reverse chron order |
| GetLinkCompactStateMessageByFid | FidRequest      | MessagesResponse | Returns compact state messages for Links by an fid        |
| GetAllLinkMessagesByFid    | FidTimestampRequest  | MessagesResponse | Returns Links made by an fid with optional timestamp filtering |

## Link Request

| Field      | Type        | Label | Description                                     |
| ---------- | ----------- | ----- | ----------------------------------------------- |
| fid        | [uint64](#) |       | Farcaster ID of the user who generated the Link |
| link_type  | [string](#) |       | Type of the Link being requested                |
| target_fid | [uint64](#) |       | Fid of the target                               |

## LinksByFid Request

| Field      | Type        | Label    | Description                                     |
| ---------- | ----------- | -------- | ----------------------------------------------- |
| fid        | [uint64](#) |          | Farcaster ID of the user who generated the Link |
| link_type  | string      | optional | Type of the Link being requested                |
| page_size  | uint32      | optional | Number of results to return per page            |
| page_token | bytes       | optional | Token for pagination                            |
| reverse    | boolean     | optional | Whether to return results in reverse order      |

## LinksByTarget Request

| Field      | Type        | Label    | Description                                     |
| ---------- | ----------- | -------- | ----------------------------------------------- |
| target_fid | [uint64](#) |          | Target Farcaster ID to find links for           |
| link_type  | string      | optional | Type of the Link being requested                |
| page_size  | uint32      | optional | Number of results to return per page            |
| page_token | bytes       | optional | Token for pagination                            |
| reverse    | boolean     | optional | Whether to return results in reverse order      |

## FidTimestampRequest

| Field            | Type              | Label    | Description                                    |
| ---------------- | ----------------- | -------- | ---------------------------------------------- |
| fid              | [uint64](#uint64) |          | Farcaster ID                                   |
| page_size        | [uint32](#uint32) | optional | Number of results to return per page           |
| page_token       | [bytes](#bytes)   | optional | Token for pagination                           |
| reverse          | [bool](#bool)     | optional | Whether to return results in reverse order     |
| start_timestamp  | [uint64](#uint64) | optional | Optional timestamp to start filtering from     |
| stop_timestamp   | [uint64](#uint64) | optional | Optional timestamp to stop filtering at        |
