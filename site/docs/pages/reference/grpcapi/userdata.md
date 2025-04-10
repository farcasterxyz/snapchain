# UserData API

Used to retrieve the current metadata associated with a user

## API

| Method Name                 | Request Type    | Response Type    | Description                            |
| --------------------------- | --------------- | ---------------- | -------------------------------------- |
| GetUserData                 | UserDataRequest | Message          | Returns a specific UserData for an Fid |
| GetUserDataByFid            | FidRequest      | MessagesResponse | Returns all UserData for an Fid        |
| GetAllUserDataMessagesByFid | FidRequest      | MessagesResponse | Returns all UserData for an Fid        |

## UserData Request

| Field          | Type              | Label | Description                                         |
| -------------- | ----------------- | ----- | --------------------------------------------------- |
| fid            | [uint64](#)       |       | Farcaster ID of the user who generated the UserData |
| user_data_type | [UserDataType](#) |       | Type of UserData being requested                    |

## Messages Response

| Field           | Type            | Label    | Description       |
| --------------- | --------------- | -------- | ----------------- |
| messages        | [Message](#)    | repeated | Farcaster Message |
| next_page_token | [bytes](#bytes) | optional |                   |
