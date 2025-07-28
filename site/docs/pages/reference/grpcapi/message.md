# Message API

Used to validate and send a message to the Snapchain node. Valid messages are accepted and gossiped to other nodes in the
network.

## API

| Method Name        | Request Type | Response Type      | Description                                                   |
| ------------------ | ------------ | ------------------ | ------------------------------------------------------------- |
| SubmitMessage      | Message      | Message            | Submits a Message to the node                                 |
| SubmitBulkMessages | SubmitBulkMessagesRequest | SubmitBulkMessagesResponse | Submits several Messages to the node |
| ValidateMessage    | Message      | ValidationResponse | Validates a Message on the node without merging and gossiping |

## SubmitBulkMessagesRequest
| Field   | Type    | Label | Description                                   |
| ------- | ------- | ----- | --------------------------------------------- |
| messages |  Message | repeated    | An array of Messages to submit. All messages will submitted, even if earlier ones fail      |

## SubmitBulkMessagesResponse
| Field   | Type    | Label | Description                                   |
| ------- | ------- | ----- | --------------------------------------------- |
| messages | BulkMessageResponse | repeated | An array of BulkMessageResponse, one for each submitted message indicating success or failure |

## BulkMessageResponse
| Field   | Type    | Label | Description                                   |
| ------- | ------- | ----- | --------------------------------------------- |
| message | Message | oneOf | The message if it was submitted successfully  |
| message_error | MessageError | oneOf | Failure reason if the message was not submitted successfully |

# MessageError
| Field   | Type    | Label | Description                                   |
| ------- | ------- | ----- | --------------------------------------------- |
| hash    | bytes   |       | Message hash                                  |
| errCode | string  |       | Failure error code                            |
| message | string  |       | Description of the failure reason             |


## ValidationResponse

| Field   | Type    | Label | Description                                   |
| ------- | ------- | ----- | --------------------------------------------- |
| valid   | boolean |       | Whether the message is valid or not           |
| message | Message |       | The message being validated (same as request) |
