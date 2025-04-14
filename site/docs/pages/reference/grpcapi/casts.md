# Casts API

Used to retrieve valid casts or tombstones for deleted casts

## API

| Method Name             | Request Type         | Response Type    | Description                                                    |
| ----------------------- | -------------------- | ---------------- | -------------------------------------------------------------- |
| GetCast                 | CastId               | Message          | Returns a specific Cast                                        |
| GetCastsByFid           | FidRequest           | MessagesResponse | Returns CastAdds for an Fid in reverse chron order             |
| GetCastsByParent        | CastsByParentRequest | MessagesResponse | Returns CastAdd replies to a given Cast in reverse chron order |
| GetCastsByMention       | FidRequest           | MessagesResponse | Returns CastAdds that mention an Fid in reverse chron order    |
| GetAllCastMessagesByFid | FidTimestampRequest  | MessagesResponse | Returns Casts for an Fid with optional timestamp filtering     |

## CastsByParentRequest

| Field          | Type              | Label    | Description                                    |
| -------------- | ----------------- | -------- | ---------------------------------------------- |
| parent_cast_id | [CastId](#CastId) |          | Parent cast ID to find replies for (optional)  |
| parent_url     | [string](#string) |          | Parent URL to find replies for (optional)      |
| page_size      | [uint32](#uint32) | optional | Number of results to return per page           |
| page_token     | [bytes](#bytes)   | optional | Token for pagination                           |
| reverse        | [bool](#bool)     | optional | Whether to return results in reverse order     |

## FidTimestampRequest

| Field            | Type              | Label    | Description                                    |
| ---------------- | ----------------- | -------- | ---------------------------------------------- |
| fid              | [uint64](#uint64) |          | Farcaster ID                                   |
| page_size        | [uint32](#uint32) | optional | Number of results to return per page           |
| page_token       | [bytes](#bytes)   | optional | Token for pagination                           |
| reverse          | [bool](#bool)     | optional | Whether to return results in reverse order     |
| start_timestamp  | [uint64](#uint64) | optional | Optional timestamp to start filtering from     |
| stop_timestamp   | [uint64](#uint64) | optional | Optional timestamp to stop filtering at        |
