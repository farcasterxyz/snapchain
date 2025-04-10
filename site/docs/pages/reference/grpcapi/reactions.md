# Reactions API

## API

| Method Name                 | Request Type             | Response Type    | Description                                                  |
| --------------------------- | ------------------------ | ---------------- | ------------------------------------------------------------ |
| GetReaction                 | ReactionRequest          | Message          | Returns a specific Reaction                                  |
| GetReactionsByFid           | ReactionsByFidRequest    | MessagesResponse | Returns Reactions made by an Fid in reverse chron order      |
| GetReactionsByCast          | ReactionsByCastRequest   | MessagesResponse | Returns ReactionAdds for a given Cast in reverse chron order |
| GetReactionsByTarget        | ReactionsByTargetRequest | MessagesResponse | Returns ReactionAdds for a given Cast in reverse chron order |
| GetAllReactionMessagesByFid | FidRequest               | MessagesResponse | Returns Reactions made by an Fid in reverse chron order      |

## Reaction Request

Used to retrieve valid or revoked reactions

| Field          | Type              | Label | Description                                                           |
| -------------- | ----------------- | ----- | --------------------------------------------------------------------- |
| fid            | [uint64](#)       |       | Farcaster ID of the user who generated the Reaction                   |
| reaction_type  | [ReactionType](#) |       | Type of the Reaction being requested                                  |
| target_cast_id | [CastId](#)       |       | (optional) Identifier of the Cast whose reactions are being requested |
| target_url     | [string](#)       |       | (optional) Identifier of the Url whose reactions are being requested  |

## ReactionsByFid Request

| Field         | Type              | Label | Description                                         |
| ------------- | ----------------- | ----- | --------------------------------------------------- |
| fid           | [uint64](#)       |       | Farcaster ID of the user who generated the Reaction |
| reaction_type | [ReactionType](#) |       | Type of the Reaction being requested                |
| page_size     | uint32            |       | (optional) Type of the Link being requested         |
| page_token    | bytes             |       | (optional)Type of the Link being requested          |
| reverse       | boolean           |       | (optional) Ordering of the response                 |

## ReactionsByCast Request

| Field         | Type              | Label | Description                                                |
| ------------- | ----------------- | ----- | ---------------------------------------------------------- |
| cast_id       | [CastId](#)       |       | Identifier of the Cast whose reactions are being requested |
| reaction_type | [ReactionType](#) |       | Type of the Reaction being requested                       |
| page_size     | uint32            |       | (optional) Type of the Link being requested                |
| page_token    | bytes             |       | (optional)Type of the Link being requested                 |
| reverse       | boolean           |       | (optional) Ordering of the response                        |

## ReactionsByTargetRequest

| Field          | Type                          | Label    | Description |
| -------------- | ----------------------------- | -------- | ----------- |
| target_cast_id | [CastId](#CastId)             |          |             |
| target_url     | [string](#string)             |          |             |
| reaction_type  | [ReactionType](#ReactionType) | optional |             |
| page_size      | [uint32](#uint32)             | optional |             |
| page_token     | [bytes](#bytes)               | optional |             |
| reverse        | [bool](#bool)                 | optional |             |
