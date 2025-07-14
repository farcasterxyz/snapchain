# Events API

Used to subscribe to real-time event updates from the Snapchain node

## API

| Method Name | Request Type     | Response Type   | Description                      |
| ----------- | ---------------- | --------------- | -------------------------------- |
| Subscribe   | SubscribeRequest | stream HubEvent | Streams new Events as they occur |
| GetEvent    | EventRequest     | HubEvent        | Returns a single event by ID     |
| GetEvents   | EventsRequest    | EventsResponse  | Returns a paginated list of events |

## SubscribeRequest

| Field       | Type                 | Label    | Description                                |
| ----------- | -------------------- | -------- | ------------------------------------------ |
| event_types | [HubEventType](#)    | repeated | Types of events to subscribe to            |
| from_id     | uint64               | optional | Event ID to start streaming from           |
| shard_index | uint32               | optional | Shard index to subscribe to                |

## EventRequest

| Field       | Type        | Label | Description                  |
| ----------- | ----------- | ----- | ---------------------------- |
| id          | [uint64](#) |       | Event ID to retrieve         |
| shard_index | [uint32](#) |       | Shard index for the event    |

## EventsRequest

| Field       | Type            | Label    | Description                         |
| ----------- | --------------- | -------- | ----------------------------------- |
| start_id    | [uint64](#)     |          | Starting event ID                   |
| shard_index | [uint32](#)     | optional | Shard index to query                |
| stop_id     | [uint64](#)     | optional | Stopping event ID                   |
| page_size   | [uint32](#)     | optional | Number of events to return per page |
| page_token  | [bytes](#)      | optional | Page token for pagination           |
| reverse     | [bool](#)       | optional | Whether to return events in reverse order |

## EventsResponse

| Field           | Type            | Label    | Description                     |
| --------------- | --------------- | -------- | ------------------------------- |
| events          | [HubEvent](#)   | repeated | List of events                  |
| next_page_token | [bytes](#)      | optional | Token for next page of results  |
