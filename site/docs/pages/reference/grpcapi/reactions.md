# Reactions API

## API

| Method Name                 | Request Type             | Response Type    | Description                                                                  |
| --------------------------- | ------------------------ | ---------------- | ---------------------------------------------------------------------------- |
| GetReaction                 | ReactionRequest          | Message          | Returns a specific Reaction                                                  |
| GetReactionsByFid           | ReactionsByFidRequest    | MessagesResponse | Returns Reactions made by an Fid in reverse chron order                      |
| GetReactionsByCast          | ReactionsByTargetRequest | MessagesResponse | Returns ReactionAdds for a given Cast in reverse chron order (To be deprecated) |
| GetReactionsByTarget        | ReactionsByTargetRequest | MessagesResponse | Returns ReactionAdds for a given target (cast or URL) in reverse chron order |
| GetAllReactionMessagesByFid | FidTimestampRequest      | MessagesResponse | Returns Reactions made by an Fid with optional timestamp filtering           |

## Reaction Request

Used to retrieve valid or revoked reactions

| Field          | Type              | Label | Description                                                           |
| -------------- | ----------------- | ----- | --------------------------------------------------------------------- |
| fid            | [uint64](#)       |       | Farcaster ID of the user who generated the Reaction                   |
| reaction_type  | [ReactionType](#) |       | Type of the Reaction being requested                                  |
| target_cast_id | [CastId](#)       |       | (optional) Identifier of the Cast whose reactions are being requested |
| target_url     | [string](#)       |       | (optional) Identifier of the Url whose reactions are being requested  |

## ReactionsByFid Request

| Field         | Type              | Label    | Description                                         |
| ------------- | ----------------- | -------- | --------------------------------------------------- |
| fid           | [uint64](#)       |          | Farcaster ID of the user who generated the Reaction |
| reaction_type | [ReactionType](#) | optional | Type of the Reaction being requested                |
| page_size     | uint32            | optional | Number of results to return per page                |
| page_token    | bytes             | optional | Token for pagination                                |
| reverse       | boolean           | optional | Whether to return results in reverse order          |

## ReactionsByTargetRequest

| Field          | Type                          | Label    | Description                                            |
| -------------- | ----------------------------- | -------- | ------------------------------------------------------ |
| target_cast_id | [CastId](#CastId)             |          | Target cast ID to find reactions for (optional)        |
| target_url     | [string](#string)             |          | Target URL to find reactions for (optional)            |
| reaction_type  | [ReactionType](#ReactionType) | optional | Type of reaction to filter by                          |
| page_size      | [uint32](#uint32)             | optional | Number of results to return per page                   |
| page_token     | [bytes](#bytes)               | optional | Token for pagination                                   |
| reverse        | [bool](#bool)                 | optional | Whether to return results in reverse order             |

## FidTimestampRequest

| Field            | Type              | Label    | Description                                    |
| ---------------- | ----------------- | -------- | ---------------------------------------------- |
| fid              | [uint64](#uint64) |          | Farcaster ID                                   |
| page_size        | [uint32](#uint32) | optional | Number of results to return per page           |
| page_token       | [bytes](#bytes)   | optional | Token for pagination                           |
| reverse          | [bool](#bool)     | optional | Whether to return results in reverse order     |
| start_timestamp  | [uint64](#uint64) | optional | Optional timestamp to start filtering from     |
| stop_timestamp   | [uint64](#uint64) | optional | Optional timestamp to stop filtering at        |
