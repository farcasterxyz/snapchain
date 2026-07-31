# Testing DB migrations on a local devnet

DB migrations are gated off on Devnet (`ShardEngine::new_with_opts`) so the test suite never
pays for them. That also means the real startup path — schema-version check, backfill, sweep,
metrics, interrupted-run recovery — never runs locally. Set `migrations.run_on_devnet` to turn
it on:

```bash
SNAPCHAIN_MIGRATIONS__RUN_ON_DEVNET=true ./target/release/snapchain --config-path nodes/1/snapchain.toml
```

or, in a config file:

```toml
[migrations]
run_on_devnet = true
```

The flag only ever *widens* behavior on Devnet — every other network migrates regardless — so
it is safe to leave defined, but never set it in a deployed config. A node that starts with it
on logs a `WARN` saying so, once per data shard.

This document uses the `M2VerificationByAddressIndex` migration as the worked example. The
shape generalizes: seed pre-migration state with an older binary, snapshot it, then replay the
new binary against that snapshot for each scenario.

## What you need

A fresh devnet DB is at `schema_version = 0` with no data, and the current code writes the new
format directly — so there is nothing for a migration to do unless you seed it. The reliable
way to get genuine pre-migration state is to run a binary from before the change landed:

```bash
# 6917a25 is the parent of #958, which introduced the new by-address index format.
git worktree add ../snapchain-pre958 6917a25
(cd ../snapchain-pre958 && cargo build --release --bin snapchain)

# Everything else comes from your branch. `fc` talks HTTP/gRPC, and both endpoints are
# unchanged across this range, so one build drives the old node and the new one.
cargo build --release --workspace --bins
```

## 1. Create the bed

```bash
# Generate configs ONCE. Re-running this deletes nodes/<id>/.rocks without warning, which
# will silently destroy your seeded state.
./target/release/setup_local_testnet --num-nodes 1 --num-shards 1 \
  --block-time 250ms --statsd-addr 127.0.0.1:8125 --admin-rpc-auth dev:dev

# Metrics are statsd-over-UDP only; there is no /metrics endpoint. Leave this running.
./target/release/statsd_printer > /tmp/statsd.log 2>&1 &
```

A single-node, single-shard devnet does produce blocks; there is no need for a four-node set.
Node 1 serves gRPC on `127.0.0.1:3383` and HTTP on `127.0.0.1:3483`.

## 2. Seed pre-migration state

```bash
RUST_LOG=info ../snapchain-pre958/target/release/snapchain \
  --config-path nodes/1/snapchain.toml > /tmp/seed-node.log 2>&1 &

# Register FIDs, then have several of them verify the SAME address. The overlap is the point:
# the old index stored one FID per address, so N verifiers collapse into one legacy slot.
./target/release/fc devnet bootstrap --grpc-node http://127.0.0.1:3383 --auth dev:dev \
  --fid 200001 --count 60 --signer-secret 0x1111...1111
sleep 15
./target/release/fc --node http://127.0.0.1:3483 --network devnet verification add \
  --fid 200001 --wallet-secret 0x<shared-per-address> --signer-secret 0x1111...1111
# ... repeated for each (address, fid) pair
```

Let blocks settle for ~20s before shutting down, or the last few messages will not have been
committed. Then stop the node with SIGINT and check the seed:

```bash
./target/release/verification_index_inspect --db nodes/1/.rocks --shard 1 status
./target/release/verification_index_inspect --db nodes/1/.rocks --shard 1 counts
```

Everything below must hold before going further:

* `schema_version: 0`, no in-progress flags
* `new_entries: 0`, `legacy_slots > 0`
* `primary_adds ≈ N × legacy_slots` for N verifiers per address — **this ratio is the premise
  of the whole exercise.** If it is 1:1, the overlap did not take and the backfill has nothing
  interesting to do.

Snapshot it. You will restore this repeatedly, and the first successful migration destroys it:

```bash
cp -a nodes/1/.rocks /tmp/seed-rocks-baseline
```

## 3. Run the scenarios

Restore the baseline between each: `rm -rf nodes/1/.rocks && cp -a /tmp/seed-rocks-baseline nodes/1/.rocks`

### Happy path

Start the new binary with the flag, wait for `All pending background migrations complete`,
SIGINT, then assert:

```bash
./target/release/verification_index_inspect --db nodes/1/.rocks --shard 1 status   # schema_version: 2
./target/release/verification_index_inspect --db nodes/1/.rocks --shard 1 counts   # legacy 0, new == primary_adds
./target/release/verification_index_inspect --db nodes/1/.rocks --shard 1 verify   # exits 0
./target/release/verification_index_inspect --db nodes/1/.rocks --shard 1 \
  address --address 0x<a multi-verifier address>                                   # one row per verifier
```

`verify` is the oracle. It treats the two directions asymmetrically on purpose: an add with no
index entry is always a bug, while an index entry with no add is tolerated and reported
separately — `get_verifications_by_address` documents that a remove racing the backfill can
leave a stale entry that self-heals on next touch.

### Observability

```bash
grep migration /tmp/statsd.log
```

`migration.schema_version` is gauged at *every* startup, including one that short-circuits with
`DB schema is up to date` — restart an already-migrated DB to confirm it fires with the current
version and that `migration.started` does not.

### Interrupted-run recovery

Rather than racing a SIGKILL, build the post-crash state offline. The interesting version is a
*partial* one — some entries already migrated, some legacy slots left, schema un-bumped:

```bash
# From an already-migrated DB, re-inject legacy slots for half the addresses...
./target/release/verification_index_inspect --db nodes/1/.rocks --shard 1 \
  inject-legacy --address 0x<addr> --fid 200001 --yes
# ...then arm the flag. Note it is keyed by the schema version the migration STARTED from,
# so a crash during the 1 -> 2 upgrade is (schema_version 1, in_progress 1).
./target/release/verification_index_inspect --db nodes/1/.rocks --shard 1 \
  arm-wedge --schema-version 1 --in-progress 1 --yes
```

On restart expect `Detected an interrupted migration`, exactly one
`migration.recovered_interrupted`, only the interrupted migration re-running, and
`schema_version` reaching the latest. Critically, the already-migrated entry count must be
**unchanged** — if it drops, the sweep is misclassifying new entries as legacy.

## Reading the results

`verification_index_inspect` opens a read-only snapshot, so it works against a running node,
but it warns when it detects one — make hard assertions with the node stopped. `arm-wedge` and
`inject-legacy` take the exclusive lock and will fail outright if the node is up.

Two things to know before interpreting output:

* **Migrations run on data shards, not shard 0.** `MigrationRunner` is only constructed in
  `ShardEngine`; `BlockEngine` never migrates. Inspect `--shard 1`. Shard 0 is a useful
  control: it is never migrated, so it must show zero legacy slots anyway.
* **On current code, new verifications route to shard 0** and replay to data shards via
  `BlockReceiver` (which is why generated devnet configs set `block_receiver.enabled = true`).
  Pre-#958 they routed to the data shard directly. That is what puts the seeded legacy slots
  where the migration will find them.

## Docker

The devnet compose file passes the same env vars through, defaulting to today's behavior:

```bash
SNAPCHAIN_MIGRATIONS__RUN_ON_DEVNET=true \
SNAPCHAIN_STATSD__ADDR=host.docker.internal:8125 \
  make dev
```

Config is baked into the image at build time (`setup_local_testnet` runs in the Dockerfile), so
editing `nodes/*/snapchain.toml` on the host does nothing — env vars are the way in, since
figment merges them after the TOML file. `verification_index_inspect` ships in the image.

Use docker to confirm the flag is picked up over env and that `migration.schema_version`
reaches the latest on every shard of every node — that is the "rolled across the fleet" signal.
Seeding four containers with a pre-#958 DB means reconciling shard counts and stripping
per-node consensus WALs for little added signal; do legacy-state testing on the single-node bed.
