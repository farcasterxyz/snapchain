# Snapchain

The open-source, canonical implementation of Farcaster's [Snapchain](https://github.com/farcasterxyz/protocol/discussions/207) network. 

![snapchain](https://github.com/user-attachments/assets/e5a041db-e3ae-4250-ad6b-7043ad648d34)


<!-- TODO:  links to installation, user docs, contributor docs -->

## What is Snapchain?

Snapchain is a data storage layer for the Farcaster social protocol. It is a blockchain-like decentralized p2p network that stores data created by Farcaster users. Learn more about Snapchain's design from the [whitepaper](https://github.com/farcasterxyz/protocol/discussions/207).

The main goals of this implementation are:

1. **High Throughput**: Written in Rust and will process at least 10,000 transactions per second. 

2. **Data Availability**: Can be run for < $1,000/month and provide real-time access to user data. 

3. **Canonical Implementation**: Is the most accurate reference for how Snapchain and Farcaster should work. 

## Status 

Snapchain is in the migration phase. Please check the [release docs](https://www.notion.so/warpcast/Snapchain-Mainnet-Public-1b96a6c0c101809493cfda3998a65c7a) for more details on timelines. 

## Running a Node

A snapchain node lets you read and write messages to the network. You will need a machine with the following system requirements to get started: 

- 16 GB of RAM
- 4 CPU cores or vCPUs
- 1TB of free storage
- A public IP address
- Ports 3381 - 3383 exposed on both TCP and UDP. 

You can start a new node or upgrade an existing node with the following commands: 

```bash
mkdir snapchain
cd snapchain
docker compose down # If you have a previous version running
wget https://raw.githubusercontent.com/farcasterxyz/snapchain/refs/heads/main/docker-compose.read.yml -O docker-compose.yml
docker compose up  # append -d to run in the background
```

A brand new node will download historical snapshots to catchup to the latest state before it begins sync. This can take up to 2 hours. Check the node's status by running `curl http://localhost:3381/v1/info`. You should see `maxHeight` increasing and `blockDelay` decreasing until it approaches zero. 

## Contributing 

We welcome contributions from developers of all skill levels. Please look for issues labeled with "help wanted" to find good tickets to work on. If you are working on something that is not explicitly a ticket, we may or may not accept it. We encourage checking with someone on the team before spending a lot of time on something. 

We will ban and report accounts that appear to engage in reputating farming by using LLMs or automated tools to generate PRs. 

## Installation

### Prerequisites

Before you begin, ensure you have the following installed:
- Rust (latest stable version)
- Cargo (comes with Rust)
- Protocol Buffers compiler (`brew install protobuf`)
- cmake (`brew install cmake`) 

### Installation

Clone the snapchain and dependent repos and build snapchain:
```
git clone git@github.com:CassOnMars/eth-signature-verifier.git
git clone git@github.com:informalsystems/malachite.git
cd malachite
git checkout 13bca14cd209d985c3adf101a02924acde8723a5
cd ..
git clone git@github.com:farcasterxyz/snapchain.git
cd snapchain
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

If you need fresh keypairs for your node, you can generate them with:

```
cargo run --bin generate_keys
```

### Clean up

You can remove any cached items by running:

```
make clean
```

## Publishing

1. Update `package.version` in `Cargo.toml`
2. Commit the change.
3. Tag the commit using `snapchain_version=$(awk -F '"' '/^version =/ {print $2}' Cargo.toml) git tag -s -m "$snapchain_version" "v$snapchain_version"`
4. Push the commit and tag to trigger an automated build.
5. Once automated build is complete, confirm the Docker image was [published](https://hub.docker.com/r/farcasterxyz/snapchain)
