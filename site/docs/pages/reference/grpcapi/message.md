# Message API

Used to validate and send a message to the Farcaster Hub. Valid messages are accepted and gossiped to other hubs in the
network.

## API

| Method Name     | Request Type | Response Type      | Description                                                  |
| --------------- | ------------ | ------------------ | ------------------------------------------------------------ |
| SubmitMessage   | Message      | Message            | Submits a Message to the Hub                                 |
| ValidateMessage | Message      | ValidationResponse | Validates a Message on the Hub without merging and gossiping |

## ValidationResponse

| Field   | Type    | Label | Description                                   |
| ------- | ------- | ----- | --------------------------------------------- |
| valid   | boolean |       | Whether the message is valid or not           |
| message | Message |       | The message being validated (same as request) |
