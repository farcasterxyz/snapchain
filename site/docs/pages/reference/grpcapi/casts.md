# Casts API

Used to retrieve valid casts or tombstones for deleted casts

## API

| Method Name             | Request Type | Response Type    | Description                                                    |
| ----------------------- | ------------ | ---------------- | -------------------------------------------------------------- |
| GetCast                 | CastId       | Message          | Returns a specific Cast                                        |
| GetCastsByFid           | FidRequest   | MessagesResponse | Returns CastAdds for an Fid in reverse chron order             |
| GetCastsByParent        | CastId       | MessagesResponse | Returns CastAdd replies to a given Cast in reverse chron order |
| GetCastsByMention       | FidRequest   | MessagesResponse | Returns CastAdds that mention an Fid in reverse chron order    |
| GetAllCastMessagesByFid | FidRequest   | MessagesResponse | Returns Casts for an Fid in reverse chron order                |

## CastsByParentRequest

| Field          | Type              | Label    | Description |
| -------------- | ----------------- | -------- | ----------- |
| parent_cast_id | [CastId](#CastId) |          |             |
| parent_url     | [string](#string) |          |             |
| page_size      | [uint32](#uint32) | optional |             |
| page_token     | [bytes](#bytes)   | optional |             |
| reverse        | [bool](#bool)     | optional |             |
