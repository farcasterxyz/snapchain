# Links API

A Link represents a relationship between two users (e.g. follow)

## API

| Method Name             | Request Type         | Response Type    | Description                                                |
| ----------------------- | -------------------- | ---------------- | ---------------------------------------------------------- |
| GetLink                 | LinkRequest          | Message          | Returns a specific Link                                    |
| GetLinksByFid           | LinksByFidRequest    | MessagesResponse | Returns Links made by an fid in reverse chron order        |
| GetLinksByTarget        | LinksByTargetRequest | MessagesResponse | Returns LinkAdds for a given target in reverse chron order |
| GetAllLinkMessagesByFid | FidRequest           | MessagesResponse | Returns Links made by an fid in reverse chron order        |

## Link Request

| Field      | Type        | Label | Description                                     |
| ---------- | ----------- | ----- | ----------------------------------------------- |
| fid        | [uint64](#) |       | Farcaster ID of the user who generated the Link |
| link_type  | [string](#) |       | Type of the Link being requested                |
| target_fid | [uint64](#) |       | Fid of the target                               |

## LinksByFid Request

| Field      | Type        | Label | Description                                     |
| ---------- | ----------- | ----- | ----------------------------------------------- |
| fid        | [uint64](#) |       | Farcaster ID of the user who generated the Link |
| link_type  | string      |       | Type of the Link being requested                |
| page_size  | uint32      |       | (optional) Type of the Link being requested     |
| page_token | bytes       |       | (optional)Type of the Link being requested      |
| reverse    | boolean     |       | (optional) Ordering of the response             |

## LinksByTarget Request

| Field      | Type        | Label | Description                                     |
| ---------- | ----------- | ----- | ----------------------------------------------- |
| target_fid | [uint64](#) |       | Farcaster ID of the user who generated the Link |
| link_type  | string      |       | (optional) Type of the Link being requested     |
| page_size  | uint32      |       | (optional) Type of the Link being requested     |
| page_token | bytes       |       | (optional)Type of the Link being requested      |
| reverse    | boolean     |       | (optional) Ordering of the response             |
