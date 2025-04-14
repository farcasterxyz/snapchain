# Fids API

Used to retrieve a list of all fids

## API

| Method Name | Request Type | Response Type | Description                          |
| ----------- | ------------ | ------------- | ------------------------------------ |
| GetFids     | FidsRequest  | FidsResponse  | Returns a paginated list of all fids |

## FidsRequest

| Field      | Type              | Label    | Description |
| ---------- | ----------------- | -------- | ----------- |
| page_size  | [uint32](#uint32) | optional |             |
| page_token | [bytes](#bytes)   | optional |             |
| reverse    | [bool](#bool)     | optional |             |

## Fids Response

| Field           | Type            | Label    | Description   |
| --------------- | --------------- | -------- | ------------- |
| fids            | [uint64](#)     | repeated | Array of fids |
| next_page_token | [bytes](#bytes) | optional |               |
