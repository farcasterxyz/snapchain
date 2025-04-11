# UserData API

Used to retrieve the current metadata associated with a user

## API

| Method Name                 | Request Type        | Response Type    | Description                                              |
| --------------------------- | ------------------- | ---------------- | -------------------------------------------------------- |
| GetUserData                 | UserDataRequest     | Message          | Returns a specific UserData for an Fid                   |
| GetUserDataByFid            | FidRequest          | MessagesResponse | Returns all UserData for an Fid                          |
| GetAllUserDataMessagesByFid | FidTimestampRequest | MessagesResponse | Returns all UserData for an Fid with timestamp filtering |

## UserData Request

| Field          | Type              | Label | Description                                         |
| -------------- | ----------------- | ----- | --------------------------------------------------- |
| fid            | [uint64](#)       |       | Farcaster ID of the user who generated the UserData |
| user_data_type | [UserDataType](#) |       | Type of UserData being requested                    |

## Messages Response

| Field           | Type            | Label    | Description             |
| --------------- | --------------- | -------- | ----------------------- |
| messages        | [Message](#)    | repeated | Farcaster Message array |
| next_page_token | [bytes](#bytes) | optional | Token for pagination    |

## FidTimestampRequest

| Field            | Type              | Label    | Description                                    |
| ---------------- | ----------------- | -------- | ---------------------------------------------- |
| fid              | [uint64](#uint64) |          | Farcaster ID                                   |
| page_size        | [uint32](#uint32) | optional | Number of results to return per page           |
| page_token       | [bytes](#bytes)   | optional | Token for pagination                           |
| reverse          | [bool](#bool)     | optional | Whether to return results in reverse order     |
| start_timestamp  | [uint64](#uint64) | optional | Optional timestamp to start filtering from     |
| stop_timestamp   | [uint64](#uint64) | optional | Optional timestamp to stop filtering at        |
