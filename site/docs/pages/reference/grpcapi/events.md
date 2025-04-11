# Events API

Used to subscribe to real-time event updates from the Snapchain node

## API

| Method Name | Request Type     | Response Type   | Description                      |
| ----------- | ---------------- | --------------- | -------------------------------- |
| Subscribe   | SubscribeRequest | stream HubEvent | Streams new Events as they occur |

## SubscribeRequest

| Field               | Type           | Label    | Description                                |
| ------------------- | -------------- | -------- | ------------------------------------------ |
| event_types         | [EventType](#) | repeated | Types of events to subscribe to            |
| from_id             | uint64         | optional | Event ID to start streaming from           |
| fid_partitions      | uint64         | optional | Number of FID partitions                   |
| fid_partition_index | uint64         | optional | Index of FID partition to subscribe to     |
| shard_index         | uint32         | optional | Shard index to subscribe to                |
