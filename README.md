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
- 1.5TB of free storage
- A public IP address
- Ports 3381 - 3383 exposed on both TCP and UDP.

You can start a new node or upgrade an existing node with the following command:

```bash
curl -sSL https://raw.githubusercontent.com/farcasterxyz/snapchain/refs/heads/main/scripts/snapchain-bootstrap.sh | bash
```
You can manage your node using the snapchain.sh script. It uses docker compose to run the node in a container. The script provides commands to start, stop, and check the logs of your node.
A brand new node will download historical snapshots to catchup to the latest state before it begins sync. This can take up to 2 hours. Check the node's status by running `curl http://localhost:3381/v1/info`. You should see `maxHeight` increasing and `blockDelay` decreasing until it approaches zero.

## `fc` CLI

The snapchain Docker image ships a small CLI, `fc`, for submitting messages and streaming
HubEvents against a node. See [`cli/README.md`](cli/README.md) for usage. Run it via
`docker run --rm farcasterxyz/snapchain:<version> fc --help`.

## Mesh & topology endpoint

Each node exposes an admin-gated view of its gossip mesh at `GET /v1/mesh` on the HTTP port
(`3381` by default): who it is connected to, how each peer is linked per topic — emergent gossip
mesh or a configured direct/explicit peer (a validator that is *neither* on the `consensus` topic is
a consensus-partition risk) — and per-peer/per-topic gossip rates. With `?crawl=true` the node queries every connected validator and
assembles a **network-wide topology**. Validators are identified cryptographically — a libp2p
`PeerId` is derived from the validator signing key and matched against the validator set — not by a
heuristic.

**Admin gate.** The endpoint requires the credentials configured in `admin_rpc_auth`
(`username:password`), sent as HTTP Basic auth. If `admin_rpc_auth` is empty the endpoint is open
(convenient for devnet/tests). Requests without valid credentials return `401`. The crawl talks
to other nodes over the gossip port (`3382`); a node only answers crawl requests from validator
peers.

**Query parameters:**

| Parameter         | Values                  | Default             | Description                                                                 |
| ----------------- | ----------------------- | ------------------- | --------------------------------------------------------------------------- |
| `format`          | `json`, `ascii`         | `json`              | `ascii` returns a `text/plain` table / matrix                               |
| `crawl`           | `true`, `false`         | `false`             | When `true`, crawl all connected validators into a network-wide topology    |
| `validators_only` | `true`, `false` (`0`)   | `true`              | When `false`, include non-validator peers (read nodes) in the output        |
| `topics`          | comma list, or `all`    | `consensus,mempool` | Which topics' gossip mesh to show in `ascii` (validated; `all` = every one) |

```bash
# Local view, ASCII table + graph (renders straight to the terminal)
curl -u user:pass "http://127.0.0.1:3381/v1/mesh?format=ascii"

# Network-wide topology crawl
curl -u user:pass "http://127.0.0.1:3381/v1/mesh?crawl=true&format=ascii"

# Structured JSON (default), all topics, including read-node peers
curl -u user:pass "http://127.0.0.1:3381/v1/mesh?validators_only=false&topics=all"
```

Local view (`?format=ascii`) — one column per selected topic. A peer is `● in-mesh` (emergent
gossipsub mesh), `◆ direct/explicit` (a configured `direct_peers` entry — see below), `○ sub-only`
(subscribed but neither), or `· none`. The header's `consensus-mesh N/M` counts validators with an
effective consensus link (mesh **or** direct) out of all validator peers:

```
MESH VIEW  self=…5Wf6xpR  validator  height 109  net=DEVNET  consensus-mesh 3/3
PEERS (3 shown, 3 validators)   MESH per topic: ● in-mesh  ◆ direct/explicit  ○ sub-only  · none
PEER         TYPE          DIR  consensus  mempool    CONTACT    msgs/s(consensus)
…MPByXee     validator     yes  ◆          ◆          derived    27.8
…PNpJTQk     validator     yes  ◆          ◆          derived    27.4
…6T9zXCY     validator     yes  ◆          ◆          derived    0.0
GRAPH (consensus mesh)
  …5Wf6xpR ─◆ …MPByXee   (direct/explicit peer)
```

Crawl (`?crawl=true&format=ascii`) — one adjacency matrix per topic. A cell is the row's view of the
column: `●` both meshed, `◆` linked with at least one side direct/explicit, `·` neither, `>`/`<` a
one-way link. Validators that can't be reached are listed rather than dropped:

```
MESH TOPOLOGY  nodes=4  unreachable=0  net=DEVNET
CONSENSUS LINKS (row->col:  ● mesh  ◆ direct/explicit  > row->col only  < col->row only  · none)
                SfBvi BJAnm RhdRc 7WeXa
…YvSfBvi (val)  —     ◆     ◆     ◆
…WQBJAnm (val)  ◆     —     ◆     ◆
…tfRhdRc (val)  ◆     ◆     —     ◆
…Pw7WeXa (val)  ◆     ◆     ◆     —
NODES (links per topic: mesh + direct)
PEER         ROLE        conse mempo VERSION
…YvSfBvi     validator   3     3     13
unreachable: (none)
```

**Direct/explicit peers.** Nodes configured with each other via `direct_peers` are registered as
libp2p gossipsub *explicit peers*. Gossipsub forwards every published message straight to an explicit
peer but deliberately keeps it out of `mesh_peers()` — so a perfectly healthy direct peer reports
`in_mesh = false`. The view treats a subscribed direct peer as a first-class link (`◆`), distinct
from the emergent mesh (`●`) and never flagged as a partition. A real partition (`╳` / `·` /
`consensus-partition risk`) is reserved for validators that are connected but neither meshed nor
explicitly peered.

Validators do not publish contact info to their peers, so contact data for them is `derived`
(the live, observed connection address) rather than `collected` (peer-attested contact info). The
two are tracked separately and never conflated. The related `GET /v1/currentPeers` endpoint now
also surfaces these derived peers, tagged accordingly.

## Upgrade

To upgrade your Snapchain node to the latest version, follow these steps:

```bash
cd snapchain
./snapchain.sh upgrade
```

This ensures your node is always running the latest available version.

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
2. Run `cargo build`, to make sure everything is working and update cargo.lock
3. Update the changelog by running `SNAPCHAIN_VERSION=0.x.y make changelog`
4. Commit the change and create and merge the PR
5. Ensure you have the release commit `git checkout main && git pull`
6. Tag the commit using `git tag v0.x.y`, and push it with `git push origin HEAD --tags` to trigger the docker build
7. Also tag with @latest using `git tag -f @latest`, and push it (with --force) so install scripts will use the latest version
8. Once automated build is complete, confirm the Docker image was [published](https://hub.docker.com/r/farcasterxyz/snapchain)
9. Create the GitHub release with `gh release create v0.x.y --generate-notes --latest`

