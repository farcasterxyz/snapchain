# Channels API

A channel is a name registered in the Farcaster channel registry, an ERC-721 contract. Snapchain
indexes three things about it:

- the registration (owner address and expiry), read from the registry contract
- channel messages: `ChannelUpdate`, `ChannelMember`, `ChannelPin` and `ChannelModerate`, stored on shard 0
- follows, which are ordinary `REACTION_TYPE_LIKE` reactions to the channel's asset ID, stored on the follower's own shard

:::warning
Channels activate with engine version V21, which is only scheduled on devnet so far. The mainnet
registry is not deployed yet. On mainnet nodes these endpoints already exist, but the
registration-checked ones return `channel not registered` and the others return empty results
(`{"channels":[]}`, `{"count":0}`, `{"following":false}`). The examples on this page are from a
local devnet.
:::

**Identifiers**

| Name          | Format                                                                                          | Example                                                              |
| ------------- | ----------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `channel_key` | The registered name, as a string                                                                | `builders`                                                           |
| `channelId`   | `keccak256(channel_key)`, 32 bytes as hex, with or without `0x`. It is also the ERC-721 tokenId | `0x71245f42091fcf3b250687c50ad1335799f0000d9efad3b2accc3e216cd7ca62` |

A `channelId` of any other length is rejected with `InvalidArgument`. Note the parameter names:
`channelOwner` takes `channel_key`, and the other endpoints take `channelId`.

To follow a channel, add a like reaction whose `targetUrl` is the channel's CAIP-19 asset ID:
`eip155:<chain id>/erc721:<registry address>/<channelId as a decimal number>`. Only one spelling
counts: lowercase namespaces, no leading zeros, and an EIP-55 checksummed address. On devnet the
registry is `eip155:11155111/erc721:0x7Dd80C661dED9bFC8D4440224A0b39b345a91BE4` (testnet
provisionally uses the same one).
To unfollow, remove the reaction. Follows count against the follower's reaction storage, so they
can be pruned like any other reaction.

`lastActionTs` and `followedAt` are Farcaster timestamps (seconds since 2021-01-01 UTC).
`expiry` is a Unix timestamp in seconds.

**Pagination**

List endpoints take `pageSize`, `pageToken` and, where noted, `reverse`. Keep requesting with the
returned `nextPageToken` until the response has none. A page that ends exactly on the last
item can return a token that leads to one final empty page. Omit `pageSize` to use the default:
`pageSize=0` is rejected with `InvalidArgument` on the member, membership, moderation and follow
lists, and returns an empty list on `channelsByAddress` and `channelsByFid`. A token from
`channelMembers`, `channelModerations` or `channelMembershipsByFid` only works with the channel or
FID that issued it, and a `channelFollowers` token only works on a node with the same shards.

**Errors**

Errors return HTTP 400 with the gRPC status in `error_detail`: `NotFound` for a channel that was
never registered, and `InvalidArgument` for a malformed `channelId` or `pageSize=0`.

```json
{
  "error": "Failed to get channel pin",
  "error_detail": "status: NotFound, message: \"channel not registered\", details: [], metadata: MetadataMap { headers: {} }"
}
```

**Member states**

| State                            | Description                                             |
| -------------------------------- | ------------------------------------------------------- |
| `CHANNEL_MEMBER_STATE_NONE`      | No membership record. The owner is not a member by default |
| `CHANNEL_MEMBER_STATE_MEMBER`    | Member                                                  |
| `CHANNEL_MEMBER_STATE_MODERATOR` | Moderator                                               |
| `CHANNEL_MEMBER_STATE_REMOVED`   | Removed (`CHANNEL_MEMBER_ACTION_REMOVE_MEMBER`)         |
| `CHANNEL_MEMBER_STATE_BANNED`    | Banned                                                  |

## channelOwner

Get the owner of a registered channel.

`fid` is the FID that has verified the owner address on shard 0. `0` means no FID has verified it
yet. That is a normal state, not an error: verifications made before V21 activated are not carried
over, so an owner has to verify the address again before they can manage the channel. `expiry`
can be in the past, because a lapsed registration is returned until a later one replaces it.

**Query Parameters**
| Parameter   | Description                  | Example                |
| ----------- | ---------------------------- | ---------------------- |
| channel_key | The channel's registered name | `channel_key=builders` |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/channelOwner?channel_key=builders"
```

**Response**

```json
{
  "fid": 584652,
  "ownerAddress": "0xb595998b0c072aae8056e2e7071f7ec6d28718f8",
  "expiry": 1824113353
}
```

## channelsByAddress

Get the channels registered to an Ethereum address, including lapsed registrations (an `expiry` in
the past).

**Query Parameters**
| Parameter | Description                         | Example                                              |
| --------- | ----------------------------------- | ---------------------------------------------------- |
| address   | The owner address, as hex           | `address=0xb595998b0c072aae8056e2e7071f7ec6d28718f8` |
| pageSize  | Optional page size                  | `pageSize=100`                                       |
| pageToken | Optional page token for pagination  | `pageToken=DDm1lZmLDAcqroBW...`                      |
| reverse   | Optional reverse order flag         | `reverse=true`                                       |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/channelsByAddress?address=0xb595998b0c072aae8056e2e7071f7ec6d28718f8"
```

**Response**

```json
{
  "channels": [
    {
      "channelKey": "builders",
      "fid": 584652,
      "ownerAddress": "0xb595998b0c072aae8056e2e7071f7ec6d28718f8",
      "expiry": 1824113353
    },
    {
      "channelKey": "gardening",
      "fid": 584652,
      "ownerAddress": "0xb595998b0c072aae8056e2e7071f7ec6d28718f8",
      "expiry": 1789985353
    }
  ]
}
```

## channelsByFid

Get the channels owned by an FID, through the Ethereum addresses it has verified. Includes lapsed
registrations. The node must host the FID's shard. There is no `reverse`.

**Query Parameters**
| Parameter | Description                        | Example                         |
| --------- | ---------------------------------- | ------------------------------- |
| fid       | The FID                            | `fid=584652`                    |
| pageSize  | Optional page size                 | `pageSize=100`                  |
| pageToken | Optional page token for pagination | `pageToken=DDm1lZmLDAcqroBW...` |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/channelsByFid?fid=584652"
```

**Response**

The same shape as [channelsByAddress](#channelsbyaddress).

## channelMetadata

Get a channel's name, description and settings, as set by its latest `ChannelUpdate`.

Each `ChannelUpdate` replaces the whole record, so a field the latest update left out is `null`,
not the earlier value. A registered channel with no update yet returns all fields as `null`
with `CASTING_MODE_MEMBERS_ONLY` and `MEMBERSHIP_MODE_APPROVAL`. `castingMode` is advice for
clients; the node does not enforce it. `membershipMode` is enforced: under
`MEMBERSHIP_MODE_OPEN` an FID can add itself as a member, and under `MEMBERSHIP_MODE_APPROVAL`
only the owner or a moderator can add members.

**Query Parameters**
| Parameter | Description    | Example                    |
| --------- | -------------- | -------------------------- |
| channelId | The channel ID | `channelId=0x71245f42...` |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/channelMetadata?channelId=0x71245f42091fcf3b250687c50ad1335799f0000d9efad3b2accc3e216cd7ca62"
```

**Response**

```json
{
  "name": "Builders",
  "description": "People shipping open-source tools",
  "imageUrl": "https://example.com/builders.png",
  "header": "https://example.com/builders-header.png",
  "rules": "Be kind. Show your work.",
  "castingMode": "CASTING_MODE_EVERYONE",
  "membershipMode": "MEMBERSHIP_MODE_OPEN"
}
```

A registered channel with no `ChannelUpdate`:

```json
{
  "name": null,
  "description": null,
  "imageUrl": null,
  "header": null,
  "rules": null,
  "castingMode": "CASTING_MODE_MEMBERS_ONLY",
  "membershipMode": "MEMBERSHIP_MODE_APPROVAL"
}
```

## channelMember

Get one FID's membership state in a channel. `lastActionTs` is the timestamp of the
`ChannelMember` message that set the state, and is absent for `CHANNEL_MEMBER_STATE_NONE`.

**Query Parameters**
| Parameter | Description    | Example                    |
| --------- | -------------- | -------------------------- |
| channelId | The channel ID | `channelId=0x71245f42...` |
| fid       | The FID        | `fid=584653`               |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/channelMember?channelId=0x71245f42091fcf3b250687c50ad1335799f0000d9efad3b2accc3e216cd7ca62&fid=584653"
```

**Response**

```json
{
  "state": "CHANNEL_MEMBER_STATE_MODERATOR",
  "lastActionTs": 183118157
}
```

An FID with no membership record:

```json
{
  "state": "CHANNEL_MEMBER_STATE_NONE"
}
```

## channelMembers

Get the FIDs with a membership record in a channel.

**Query Parameters**
| Parameter   | Description                                                    | Example                                        |
| ----------- | -------------------------------------------------------------- | ---------------------------------------------- |
| channelId   | The channel ID                                                 | `channelId=0x71245f42...`                     |
| stateFilter | Optional. Only return this state (see Member states above)     | `stateFilter=CHANNEL_MEMBER_STATE_MODERATOR` |
| pageSize    | Optional page size                                             | `pageSize=100`                                 |
| pageToken   | Optional page token for pagination                             | `pageToken=GAJxJF9CCR...`                     |
| reverse     | Optional reverse order flag                                    | `reverse=true`                                 |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/channelMembers?channelId=0x71245f42091fcf3b250687c50ad1335799f0000d9efad3b2accc3e216cd7ca62"
```

**Response**

```json
{
  "members": [
    {
      "fid": 584653,
      "state": "CHANNEL_MEMBER_STATE_MODERATOR"
    },
    {
      "fid": 584654,
      "state": "CHANNEL_MEMBER_STATE_MEMBER"
    },
    {
      "fid": 584655,
      "state": "CHANNEL_MEMBER_STATE_MEMBER"
    },
    {
      "fid": 584656,
      "state": "CHANNEL_MEMBER_STATE_BANNED"
    }
  ]
}
```

## channelMembershipsByFid

Get the channels an FID has a membership record in. This endpoint does not check that the channels
are registered, so it never returns `NotFound`.

**Query Parameters**
| Parameter | Description                        | Example                   |
| --------- | ---------------------------------- | ------------------------- |
| fid       | The FID                            | `fid=584654`              |
| pageSize  | Optional page size                 | `pageSize=100`            |
| pageToken | Optional page token for pagination | `pageToken=GAJxJF9CCR...` |
| reverse   | Optional reverse order flag        | `reverse=true`            |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/channelMembershipsByFid?fid=584654"
```

**Response**

```json
{
  "memberships": [
    {
      "channelId": "0x71245f42091fcf3b250687c50ad1335799f0000d9efad3b2accc3e216cd7ca62",
      "state": "CHANNEL_MEMBER_STATE_MEMBER"
    }
  ]
}
```

## channelPin

Get a channel's pinned cast. `authorFid` is the FID that set the pin, not the author of the
cast. `"pin": null` means the channel has no pin: it was never set, or it
was cleared with an empty `cast_hash`.

**Query Parameters**
| Parameter | Description    | Example                    |
| --------- | -------------- | -------------------------- |
| channelId | The channel ID | `channelId=0x71245f42...` |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/channelPin?channelId=0x71245f42091fcf3b250687c50ad1335799f0000d9efad3b2accc3e216cd7ca62"
```

**Response**

```json
{
  "pin": {
    "castHash": "0xf48dcbd8dede6ba69b8b1f2844df838f28528d86",
    "authorFid": 584652
  }
}
```

## channelModerations

Get the moderation actions (`CHANNEL_MODERATE_ACTION_HIDE` or `CHANNEL_MODERATE_ACTION_UNHIDE`)
on casts in a channel. `authorFid` is the FID that sent the `ChannelModerate` message, not the
author of the cast.

**Query Parameters**
| Parameter | Description                        | Example                    |
| --------- | ---------------------------------- | -------------------------- |
| channelId | The channel ID                     | `channelId=0x71245f42...` |
| pageSize  | Optional page size                 | `pageSize=100`             |
| pageToken | Optional page token for pagination | `pageToken=...`            |
| reverse   | Optional reverse order flag        | `reverse=true`             |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/channelModerations?channelId=0x71245f42091fcf3b250687c50ad1335799f0000d9efad3b2accc3e216cd7ca62"
```

**Response**

```json
{
  "moderations": [
    {
      "castHash": "0x403eeb4a5f6a149bf5d46a089726104668b22068",
      "action": "CHANNEL_MODERATE_ACTION_HIDE",
      "authorFid": 584653
    }
  ]
}
```

## channelFollowers

Get the FIDs following a channel. Followers live on their own shards, so this endpoint reads
every shard:

- `pageSize` applies per shard, so a page can hold up to `pageSize` times the number of shards.
- A node that does not host every shard returns `FailedPrecondition` instead of a partial list.
- There is no registration check. An unknown channel returns an empty list.

**Query Parameters**
| Parameter | Description                        | Example                   |
| --------- | ---------------------------------- | ------------------------- |
| channelId | The channel ID                     | `channelId=0x71245f42...` |
| pageSize  | Optional page size, per shard      | `pageSize=100`            |
| pageToken | Optional page token for pagination | `pageToken=W3sic2hhcm...` |
| reverse   | Optional reverse order flag        | `reverse=true`            |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/channelFollowers?channelId=0x71245f42091fcf3b250687c50ad1335799f0000d9efad3b2accc3e216cd7ca62"
```

**Response**

```json
{
  "followers": [
    {
      "fid": 584654,
      "followedAt": 183118161
    },
    {
      "fid": 584656,
      "followedAt": 183118160
    },
    {
      "fid": 584655,
      "followedAt": 183118160
    }
  ]
}
```

## channelFollowerCount

Get the number of FIDs following a channel. It reads every shard, like
[channelFollowers](#channelfollowers), and returns `0` for an unknown channel. Under concurrent
writes the count and the follower list can differ by a few entries.

**Query Parameters**
| Parameter | Description    | Example                    |
| --------- | -------------- | -------------------------- |
| channelId | The channel ID | `channelId=0x71245f42...` |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/channelFollowerCount?channelId=0x71245f42091fcf3b250687c50ad1335799f0000d9efad3b2accc3e216cd7ca62"
```

**Response**

```json
{
  "count": 3
}
```

## channelFollows

Get the channels an FID follows.

**Query Parameters**
| Parameter | Description                        | Example         |
| --------- | ---------------------------------- | --------------- |
| fid       | The FID                            | `fid=584655`    |
| pageSize  | Optional page size                 | `pageSize=100`  |
| pageToken | Optional page token for pagination | `pageToken=...` |
| reverse   | Optional reverse order flag        | `reverse=true`  |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/channelFollows?fid=584655"
```

**Response**

```json
{
  "follows": [
    {
      "channelId": "0x71245f42091fcf3b250687c50ad1335799f0000d9efad3b2accc3e216cd7ca62",
      "followedAt": 183118160
    }
  ]
}
```

## isFollowingChannel

Check whether an FID follows a channel. `followedAt` is present only when `following` is `true`.

**Query Parameters**
| Parameter | Description    | Example                    |
| --------- | -------------- | -------------------------- |
| fid       | The FID        | `fid=584655`               |
| channelId | The channel ID | `channelId=0x71245f42...` |

**Example**

```bash
curl "http://127.0.0.1:3381/v1/isFollowingChannel?fid=584655&channelId=0x71245f42091fcf3b250687c50ad1335799f0000d9efad3b2accc3e216cd7ca62"
```

**Response**

```json
{
  "following": true,
  "followedAt": 183118160
}
```
