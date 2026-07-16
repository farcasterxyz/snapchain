# `fc` — Snapchain CLI

> **ALPHA.** Flags, subcommand names, output format, and error semantics may change without
> notice. Don't depend on it from scripts you care about, and don't point it at mainnet without
> reading what each subcommand actually does.

`fc` is a small command-line client for submitting Farcaster messages to a Snapchain node and
streaming HubEvents from it. It is shipped as a second binary inside the official Snapchain
Docker image, so the proto version baked into `fc` always matches the node binary in the same
image tag.

## Subcommands

| Subcommand    | What it does                                                                   |
| ------------- | ------------------------------------------------------------------------------ |
| `key-add`     | Submit a gasless `KEY_ADD` (FIP-272). Generates a fresh signer by default.     |
| `key-remove`  | Submit a `KEY_REMOVE`, either custody-signed or self-revoke.                   |
| `cast-add`    | Submit a `CAST_ADD` signed by an existing Ed25519 key.                         |
| `cast-remove` | Submit a `CAST_REMOVE` signed by an existing Ed25519 key.                      |
| `link`        | Submit `LINK_ADD` / `LINK_REMOVE` / `LINK_COMPACT_STATE` — follows and blocks. |
| `live-at`     | Submit `USER_DATA_ADD` of type `LIVE_AT` (FIP-268 presence heartbeat).         |
| `subscribe`   | Stream `HubEvent`s from a snapchain gRPC node and log them as JSON to stdout.  |

Run `fc <subcommand> --help` for the full flag list.

## Running it

The recommended way to run `fc` is out of the snapchain Docker image. The image tag determines
which proto version `fc` was built against — see [Versioning](#versioning) below.

```bash
docker run --rm farcasterxyz/snapchain:<version> fc --help
```

Example: stream HubEvents from a public testnet node:

```bash
docker run --rm farcasterxyz/snapchain:<version> \
  fc subscribe --shard 1 --grpc-node https://iris.farcaster.xyz:3383
```

Example: submit a CAST_ADD against the default HTTP endpoint:

```bash
docker run --rm -e SIGNER_SECRET=0x... farcasterxyz/snapchain:<version> \
  fc cast-add --fid 123 --text "hello snapchain"
```

If you already have a snapchain node running in a container, you can `docker exec` instead:

```bash
docker exec <node-container> fc subscribe --shard 1 --grpc-node http://localhost:3383
```

### Building from source

For local development, `fc` is a normal cargo workspace member:

```bash
cargo build --release -p fc-cli --bin fc
./target/release/fc --help
```

## Links: follows and blocks

`fc link` submits the three link message types. A link is directional: `--fid` is the author,
`--target-fid` is the FID being followed or blocked. `--type` defaults to `follow`.

Follow FID 456 as FID 123, then unfollow:

```bash
export SIGNER_SECRET=0x...

fc link add    --fid 123 --target-fid 456
fc link remove --fid 123 --target-fid 456
```

Block and unblock — same shape, `--type block`:

```bash
fc link add    --fid 123 --target-fid 456 --type block
fc link remove --fid 123 --target-fid 456 --type block
```

Follows and blocks are stored under separate link types, so the same pair can be both at once.
To watch the resulting `MERGE_MESSAGE` events land, run `fc subscribe` on the author's shard in
another terminal:

```bash
fc subscribe --shard 1 --event-types merge-message
```

`follow` and `block` are the well-known types, but the node bounds `--type` at 1–8 bytes and
keeps no allowlist, so any short string is accepted:

```bash
fc link add --fid 123 --target-fid 456 --type mytype
```

If your signer is a gasless key (`fc key-add`), it needs the matching scope — `link-add`,
`link-remove`, or `link-compact-state` — or the node rejects the message as out of scope.
`fc key-add` grants all scopes unless you pass `--scopes`.

### Compact state

`LINK_COMPACT_STATE` replaces the author's entire link set for one type with the listed targets.
**Any target of that type not listed is dropped** — this compacts 3 follows down to 2:

```bash
fc link compact-state --fid 123 --target-fids 456,789 --type follow
```

> **Compaction is only type-scoped from engine V19 (`ProtocolFeature::BlockLinks`) onward.**
> Before V19, compaction is type-blind: a `--type follow` compact state also deletes the
> author's **blocks**. V19 is live on testnet (activated 2026-07-15 14:00 UTC) and activates on
> mainnet 2026-07-27 14:00 UTC. Until then, a follow compaction on mainnet will silently wipe
> blocks.

## Versioning

`fc --version` prints both its own version and the `snapchain-proto` version it was compiled
against. The proto version is the one that determines wire compatibility with a snapchain node.

```text
$ fc --version
fc 0.1.0
snapchain-proto 0.11.0
```

Because `fc` is distributed inside the snapchain Docker image, the simplest compatibility rule
is: **use the `fc` from the same image tag as the node you're talking to.** A given
`farcasterxyz/snapchain:X.Y.Z` always ships `fc` built against the proto version that node uses.

The CLI version in `cli/Cargo.toml` is independent of the snapchain release version and only
bumps when the CLI's own flag/output surface changes meaningfully. The proto compatibility
guarantee comes from the image tag, not the CLI version.

## JSON output shape

`fc subscribe` serializes events using the Serde derives that `tonic-build` emits on the proto
types. This is a thinner shape than the hub-API JSON returned by snapchain's HTTP server: field
names are prost defaults (snake_case), and enums are emitted as variant strings. If you need the
exact hub-API shape, query the snapchain HTTP server directly. This may change in a future
release; pin a specific image tag if you're parsing the output programmatically.

## Environment variables

| Variable        | Used by                          | Description                                          |
| --------------- | -------------------------------- | ---------------------------------------------------- |
| `SIGNER_SECRET` | `cast-add`, `cast-remove`, `link`, `live-at`, `key-add`, `key-remove` | Hex-encoded Ed25519 secret (32 bytes, optional `0x` prefix). |
| `MNEMONIC`      | `key-add`, `key-remove` (custody) | BIP-39 mnemonic for the custody-key derivation path. |
