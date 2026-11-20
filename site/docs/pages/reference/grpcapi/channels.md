# Channels API

Used to read channel registrations, channel metadata, members, pins, moderation actions and
follows. See the [HTTP Channels API](/reference/httpapi/channels) for identifiers, follow targets,
pagination rules and examples.

:::warning
Channels activate with engine version V21, which is only scheduled on devnet so far. On mainnet
these methods already exist, but the registration-checked ones return `NOT_FOUND` and the others
return empty results.
:::

All `channel_id` values are the 32-byte `keccak256` of the channel key; any other length is
`INVALID_ARGUMENT`. Methods keyed by `channel_id` return `NOT_FOUND` for a channel that was never
registered, except the follow methods and `GetChannelMembershipsByFid`, which do no registration
check. `page_size: 0` is `INVALID_ARGUMENT` on the member, membership, moderation and follow
methods; omit it for the default.

## API

| Method Name                | Request Type                   | Response Type                | Description                                                                 |
| -------------------------- | ------------------------------ | ---------------------------- | --------------------------------------------------------------------------- |
| GetChannelOwner            | ChannelOwnerRequest            | ChannelOwnerResponse         | Returns the owner address, owner fid and expiry of a channel                |
| GetChannelsByAddress       | ChannelsByAddressRequest       | ChannelsResponse             | Returns the channels registered to an address, including lapsed ones        |
| GetChannelsByFid           | ChannelsByFidRequest           | ChannelsResponse             | Returns the channels owned by an fid through its verified addresses         |
| GetChannelMetadata         | ChannelRequest                 | ChannelMetadataResponse      | Returns a channel's metadata and settings from its latest ChannelUpdate     |
| GetChannelMember           | ChannelMemberRequest           | ChannelMemberResponse        | Returns one fid's membership state in a channel                             |
| GetChannelMembers          | ChannelMembersRequest          | ChannelMembersResponse       | Returns the fids with a membership record in a channel                      |
| GetChannelMembershipsByFid | ChannelMembershipsByFidRequest | ChannelMembershipsResponse   | Returns the channels an fid has a membership record in                      |
| GetChannelPin              | ChannelRequest                 | ChannelPinResponse           | Returns a channel's pinned cast                                             |
| GetChannelModerations      | ChannelModerationsRequest      | ChannelModerationsResponse   | Returns moderation actions on casts in a channel                            |
| GetChannelFollowers        | ChannelFollowersRequest        | ChannelFollowersResponse     | Returns the fids following a channel (reads every shard)                    |
| GetChannelFollowerCount    | ChannelFollowerCountRequest    | ChannelFollowerCountResponse | Returns the number of fids following a channel (reads every shard)          |
| GetChannelFollows          | ChannelFollowsRequest          | ChannelFollowsResponse       | Returns the channels an fid follows                                         |
| IsFollowingChannel         | IsFollowingChannelRequest      | IsFollowingChannelResponse   | Returns whether an fid follows a channel                                    |

`GetChannelFollowers` and `GetChannelFollowerCount` return `FAILED_PRECONDITION` on a node that
does not host every shard, and `page_size` on `GetChannelFollowers` applies per shard.

## ChannelOwnerRequest

| Field       | Type   | Label | Description                   |
| ----------- | ------ | ----- | ----------------------------- |
| channel_key | string |       | The channel's registered name |

## ChannelsByAddressRequest

| Field         | Type   | Label    | Description                                |
| ------------- | ------ | -------- | ------------------------------------------ |
| owner_address | bytes  |          | Owner address, 20 bytes                    |
| page_size     | uint32 | optional | Number of results to return per page       |
| page_token    | bytes  | optional | Token for pagination                       |
| reverse       | bool   | optional | Whether to return results in reverse order |

## ChannelsByFidRequest

| Field      | Type   | Label    | Description                          |
| ---------- | ------ | -------- | ------------------------------------ |
| fid        | uint64 |          | Farcaster ID                         |
| page_size  | uint32 | optional | Number of results to return per page |
| page_token | bytes  | optional | Token for pagination                 |

## ChannelRequest

| Field      | Type  | Label | Description          |
| ---------- | ----- | ----- | -------------------- |
| channel_id | bytes |       | Channel ID, 32 bytes |

## ChannelMemberRequest

| Field      | Type   | Label | Description          |
| ---------- | ------ | ----- | -------------------- |
| channel_id | bytes  |       | Channel ID, 32 bytes |
| fid        | uint64 |       | Farcaster ID         |

## ChannelMembersRequest

| Field        | Type               | Label    | Description                                               |
| ------------ | ------------------ | -------- | --------------------------------------------------------- |
| channel_id   | bytes              |          | Channel ID, 32 bytes                                      |
| state_filter | ChannelMemberState | optional | Only return this state. Absent or NONE returns all states |
| page_size    | uint32             | optional | Number of results to return per page                      |
| page_token   | bytes              | optional | Token for pagination                                      |
| reverse      | bool               | optional | Whether to return results in reverse order                |

## ChannelMembershipsByFidRequest, ChannelFollowsRequest

| Field      | Type   | Label    | Description                                |
| ---------- | ------ | -------- | ------------------------------------------ |
| fid        | uint64 |          | Farcaster ID                               |
| page_size  | uint32 | optional | Number of results to return per page       |
| page_token | bytes  | optional | Token for pagination                       |
| reverse    | bool   | optional | Whether to return results in reverse order |

## ChannelModerationsRequest, ChannelFollowersRequest

| Field      | Type   | Label    | Description                                                                  |
| ---------- | ------ | -------- | ---------------------------------------------------------------------------- |
| channel_id | bytes  |          | Channel ID, 32 bytes                                                         |
| page_size  | uint32 | optional | Number of results to return per page (per shard for ChannelFollowersRequest) |
| page_token | bytes  | optional | Token for pagination                                                         |
| reverse    | bool   | optional | Whether to return results in reverse order                                   |

## ChannelFollowerCountRequest

| Field      | Type  | Label | Description          |
| ---------- | ----- | ----- | -------------------- |
| channel_id | bytes |       | Channel ID, 32 bytes |

## IsFollowingChannelRequest

| Field      | Type   | Label | Description          |
| ---------- | ------ | ----- | -------------------- |
| fid        | uint64 |       | Farcaster ID         |
| channel_id | bytes  |       | Channel ID, 32 bytes |

## ChannelOwnerResponse

| Field         | Type   | Label | Description                                                                              |
| ------------- | ------ | ----- | ---------------------------------------------------------------------------------------- |
| fid           | uint64 |       | Fid that has verified the owner address on shard 0. 0 if none has yet                    |
| owner_address | bytes  |       | Owner address, 20 bytes                                                                  |
| expiry        | uint64 |       | Registration expiry, Unix seconds. Can be in the past for a lapsed registration          |

## ChannelsResponse

| Field           | Type        | Label    | Description                                    |
| --------------- | ----------- | -------- | ---------------------------------------------- |
| channels        | ChannelInfo | repeated | Channels, with the same fields as ChannelOwnerResponse plus `channel_key` |
| next_page_token | bytes       | optional | Token for the next page; absent on the last page |

## ChannelMetadataResponse

| Field           | Type           | Label    | Description                                                    |
| --------------- | -------------- | -------- | -------------------------------------------------------------- |
| name            | string         | optional | Absent if the latest ChannelUpdate did not set it              |
| description     | string         | optional | Absent if the latest ChannelUpdate did not set it              |
| image_url       | string         | optional | Absent if the latest ChannelUpdate did not set it              |
| header          | string         | optional | Absent if the latest ChannelUpdate did not set it              |
| rules           | string         | optional | Absent if the latest ChannelUpdate did not set it              |
| casting_mode    | CastingMode    |          | Advisory. MEMBERS_ONLY if the channel has no ChannelUpdate     |
| membership_mode | MembershipMode |          | Enforced. APPROVAL if the channel has no ChannelUpdate         |

## ChannelMemberResponse

| Field          | Type               | Label    | Description                                                    |
| -------------- | ------------------ | -------- | -------------------------------------------------------------- |
| state          | ChannelMemberState |          | Membership state                                               |
| last_action_ts | uint32             | optional | Farcaster timestamp of the ChannelMember message; absent for NONE |

## ChannelMembersResponse, ChannelMembershipsResponse

| Field           | Type                                 | Label    | Description                                                         |
| --------------- | ------------------------------------ | -------- | ------------------------------------------------------------------- |
| members         | ChannelMember (`fid`, `state`)       | repeated | ChannelMembersResponse only                                         |
| memberships     | ChannelMembership (`channel_id`, `state`) | repeated | ChannelMembershipsResponse only                                |
| next_page_token | bytes                                | optional | Token for the next page; absent on the last page                    |

## ChannelPinResponse

| Field | Type                                   | Label    | Description                                                                         |
| ----- | -------------------------------------- | -------- | ----------------------------------------------------------------------------------- |
| pin   | ChannelPin (`cast_hash`, `author_fid`) | optional | Absent if the channel has no pin. `author_fid` is the fid that set the pin, not the cast's author |

## ChannelModerationsResponse

| Field           | Type                                                  | Label    | Description                                                              |
| --------------- | ----------------------------------------------------- | -------- | ------------------------------------------------------------------------ |
| moderations     | ChannelModeration (`cast_hash`, `action`, `author_fid`) | repeated | `author_fid` is the fid that sent the ChannelModerate message          |
| next_page_token | bytes                                                 | optional | Token for the next page; absent on the last page                         |

## ChannelFollowersResponse, ChannelFollowsResponse

| Field           | Type                                       | Label    | Description                                                  |
| --------------- | ------------------------------------------ | -------- | ------------------------------------------------------------ |
| followers       | ChannelFollower (`fid`, `followed_at`)     | repeated | ChannelFollowersResponse only                                |
| follows         | ChannelFollow (`channel_id`, `followed_at`) | repeated | ChannelFollowsResponse only                                 |
| next_page_token | bytes                                      | optional | Token for the next page; absent on the last page             |

`followed_at` is the Farcaster timestamp of the follow reaction.

## ChannelFollowerCountResponse

| Field | Type   | Label | Description                                     |
| ----- | ------ | ----- | ----------------------------------------------- |
| count | uint64 |       | Number of followers; 0 for an unknown channel   |

## IsFollowingChannelResponse

| Field       | Type   | Label    | Description                         |
| ----------- | ------ | -------- | ----------------------------------- |
| following   | bool   |          | Whether the fid follows the channel |
| followed_at | uint32 | optional | Present only when `following` is true |

## ChannelMemberState

| Name                           | Number | Description                                |
| ------------------------------ | ------ | ------------------------------------------ |
| CHANNEL_MEMBER_STATE_NONE      | 0      | No membership record                       |
| CHANNEL_MEMBER_STATE_MEMBER    | 1      | Member                                     |
| CHANNEL_MEMBER_STATE_MODERATOR | 2      | Moderator                                  |
| CHANNEL_MEMBER_STATE_REMOVED   | 3      | Removed                                    |
| CHANNEL_MEMBER_STATE_BANNED    | 4      | Banned                                     |
