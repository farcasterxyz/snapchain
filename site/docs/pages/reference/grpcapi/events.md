# Events API

Used to subscribe to real-time event updates from the Farcaster Hub

## API

| Method Name | Request Type     | Response Type   | Description                      |
| ----------- | ---------------- | --------------- | -------------------------------- |
| Subscribe   | SubscribeRequest | stream HubEvent | Streams new Events as they occur |
| GetEvent    | EventRequest     | HubEvent        | Streams new Events as they occur |

## SubscribeRequest

| Field       | Type           | Label    | Description                      |
| ----------- | -------------- | -------- | -------------------------------- |
| event_types | [EventType](#) | repeated | Types of events to subscribe to  |
| from_id     | uint64         | optional | Event ID to start streaming from |

## EventRequest

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| id    | [uint64](#uint64) |       |             |
