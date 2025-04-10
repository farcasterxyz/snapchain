# GRPC API

Snapchain nodes serve a gRPC API on port 3383 by default.

## Using the API

We recommend using a library
like [hub-nodejs](https://github.com/farcasterxyz/hub-monorepo/tree/main/packages/hub-nodejs) to interact with the
gRPC APIs. Please refer
to its [documentation](https://github.com/farcasterxyz/hub-monorepo/tree/main/packages/hub-nodejs/docs) for how to use
it.

## Other languages

The protobufs for the gRPC API are available within
the [hub-monorepo](https://github.com/farcasterxyz/hub-monorepo/tree/main/protobufs). They can be used to generate
bindings for other clients built using other languages. Note that by default, nodes rely on the
javascript [ts-proto](https://www.npmjs.com/package/ts-proto) library's serialization byte order to verify messages
hashes. If you are using a different client, you may need to use the `data_bytes` field with the raw serialized bytes
when calling `SubmitMessage` in order for the message to be considered valid. Refer to
the [SubmitMessage HTTP API docs](/reference/httpapi/message#using-with-rust-go-or-other-programing-languages)
for more details.
