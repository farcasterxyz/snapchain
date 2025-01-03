# Snapchain

The open-source, canonical implementation of Farcaster's [Snapchain](https://github.com/farcasterxyz/protocol/discussions/207) network. 

![snapchain](https://github.com/user-attachments/assets/e5a041db-e3ae-4250-ad6b-7043ad648d34)


<!-- TODO:  links to installation, user docs, contributor docs -->

## What is Snapchain?

Snapchain is a data storage layer for the Farcaster network. It is a blockchain-like decentralized p2p network that stores data created by users of Farcaster's social network. Learn more about Snapchain's design from the [whitepaper](https://github.com/farcasterxyz/protocol/discussions/207).

The main goals of this implementation are:

1. **High Throughput**: Written in Rust and aims to process at least 10,000 transactions per second. 

2. **Data Availability**: Can be run in the cloud for under $1,000/month and provide real-time access to the entire network. 

3. **Canonical Implementation**: Is the most accurate reference for how Snapchain and Farcaster should work. 

## Status 

Snapchain is currently in early alpha. Please check the release section of the [specification](https://github.com/farcasterxyz/protocol/discussions/207) for the latest timelines.

## Contributing 

We welcome contributions from developers of all skill levels. Please look for issues labeled with "help wanted" to find good tickets to work on. If you are working on something that is not explicitly a ticket, we may or may not accept it. We encourage checking with someone on the team before spending a lot of time on something. 

We will ban and report accounts that appear to engage in reputating farming by using LLMs or automated tools to generate PRs. 

## Installation

### Prerequisites

Before you begin, ensure you have the following installed:
- Rust (latest stable version)
- Cargo (comes with Rust)
- Protocol Buffers compiler (protoc)

### Installation

Clone the snapchain repo and build it:
```
git clone https://github.com/farcasterxyz/snapchain-v0.git
cd snapchain-v0
cargo build
```

### Testing

After setting up your Rust toolchain above, you can run tests with:

```
cargo test
```

### Running the Application

For development, you can run multiple nodes by running:
```
make dev
```

These will be configured to communicate with each other.

To query a node, you can run `grpcurl` from within the container:

```
docker compose exec node1 grpcurl -import-path proto -proto proto/rpc.proto list
```

### Clean up

You can remove any cached items by running:

```
make clean
```
