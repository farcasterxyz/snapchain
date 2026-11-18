//! Compares the RPC rate-limit precheck (`RateLimits::charge_rpc_message`) with the engine
//! simulation the gRPC server runs for every submitted message.
//!
//! Each case runs over messages built ahead of time, so signing is not timed, after an untimed
//! warm-up pass. The simulation case builds a `ShardEngine` per message, as `MyHubService` does.
//!
//! Environment variables:
//! - `ITERATIONS` (default 2000): messages timed per case.
//! - `SEED_FIDS` (default 50): registered fids, rotated through by every case.
//! - `SEED_MESSAGES` (default 10000): casts committed before timing, so lookups run against a
//!   non-empty DB and trie.

use std::collections::HashMap;
use std::error::Error;
use std::time::{Duration, Instant};

use crate::mempool::mempool::{RateLimits, RateLimitsConfig};
use crate::proto::StorageUnitType;
use crate::proto::{self, FarcasterNetwork};
use crate::storage::store::engine::ShardEngine;
use crate::storage::store::mempool_poller::MempoolMessage;
use crate::storage::store::stores::{Limits, StoreLimits, Stores};
use crate::storage::store::test_helper;
use crate::utils::cli::compose_message;
use crate::utils::factory::signers::generate_signer;
use crate::utils::factory::{events_factory, time};

const FIRST_FID: u64 = 10_000;

fn env_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

struct CaseResult {
    name: &'static str,
    per_op: Duration,
    accepted: usize,
    total: usize,
}

fn per_op(elapsed: Duration, count: usize) -> Duration {
    elapsed / count.max(1) as u32
}

async fn simulate(stores: &Stores, message: &proto::Message) -> bool {
    let mut engine = ShardEngine::new(
        stores.db.clone(),
        FarcasterNetwork::Devnet,
        stores.trie.clone(),
        1,
        stores.store_limits.clone(),
        test_helper::statsd_client(),
        100,
        None,
        None,
        None,
    )
    .await
    .expect("read-only engine");
    engine.simulate_message(message).is_ok()
}

pub async fn run() -> Result<(), Box<dyn Error>> {
    let iterations = env_or("ITERATIONS", 2000) as usize;
    let seed_fids = env_or("SEED_FIDS", 50).max(1);
    let seed_messages = env_or("SEED_MESSAGES", 10_000);

    let (mut engine, _tmpdir) = test_helper::new_engine_with_options(test_helper::EngineOptions {
        limits: Some(StoreLimits::new(
            Limits::of_type(StorageUnitType::UnitType2025),
            Limits::of_type(StorageUnitType::UnitType2024),
            Limits::of_type(StorageUnitType::UnitTypeLegacy),
        )),
        ..Default::default()
    })
    .await;

    // Rent enough units that no fid prunes its seed casts. `register_user` rents one unit.
    let casts_per_fid = seed_messages.div_ceil(seed_fids) as u32;
    let casts_per_unit = Limits::of_type(StorageUnitType::UnitType2025).casts;
    let extra_units = casts_per_fid.div_ceil(casts_per_unit).saturating_sub(1);
    let fids: Vec<u64> = (FIRST_FID..FIRST_FID + seed_fids).collect();
    for fid in &fids {
        test_helper::register_user(
            *fid,
            test_helper::default_signer(),
            test_helper::default_custody_address(),
            &mut engine,
        )
        .await;
        if extra_units > 0 {
            test_helper::commit_event(
                &mut engine,
                &events_factory::create_rent_event_with_timestamp(
                    *fid,
                    extra_units,
                    time::current_timestamp(),
                ),
            )
            .await;
        }
    }

    let mut seeded = 0;
    while seeded < seed_messages {
        let batch: Vec<MempoolMessage> = (seeded..(seeded + 500).min(seed_messages))
            .map(|i| {
                let fid = fids[i as usize % fids.len()];
                MempoolMessage::UserMessage(compose_message(fid, &format!("seed {i}"), None, None))
            })
            .collect();
        seeded += batch.len() as u64;
        let state_change = engine.propose_state_change(1, batch, None);
        test_helper::validate_and_commit_state_change(&mut engine, &state_change).await;
    }
    let stores = engine.get_stores();
    let retained_casts: u64 = fids
        .iter()
        .flat_map(|fid| stores.get_storage_limits(*fid).unwrap().limits)
        .filter(|limit| limit.store_type() == proto::StoreType::Casts)
        .map(|limit| limit.used)
        .sum();
    assert_eq!(
        retained_casts, seed_messages,
        "seed casts were pruned; the DB holds fewer messages than requested"
    );
    println!(
        "seeded {} fids, {} casts retained, trie items: {}",
        fids.len(),
        retained_casts,
        engine.trie_num_items()
    );

    let rate_limits = RateLimits::new(
        HashMap::from([(1, stores.clone())]),
        RateLimitsConfig::default(),
        test_helper::statsd_client(),
        1,
    );

    let attacker_key = generate_signer();
    let build = |label: &str, forged: bool| -> Vec<proto::Message> {
        (0..iterations)
            .map(|i| {
                let fid = fids[i % fids.len()];
                let text = format!("{label} {i}");
                if forged {
                    compose_message(fid, &text, None, Some(&attacker_key))
                } else {
                    compose_message(fid, &text, None, None)
                }
            })
            .collect()
    };
    let precheck_valid = build("precheck valid", false);
    let precheck_forged = build("precheck forged", true);
    let simulate_valid = build("simulate valid", false);
    let simulate_forged = build("simulate forged", true);

    // Untimed pass so every case starts with warm RocksDB caches and one limiter per fid.
    for message in build("warm up", false).iter().take(fids.len() * 2) {
        let _ = rate_limits.charge_rpc_message(message, FarcasterNetwork::Devnet);
        simulate(&stores, message).await;
    }

    let mut results = vec![];

    let start = Instant::now();
    let accepted = precheck_valid
        .iter()
        .filter(|m| {
            rate_limits
                .charge_rpc_message(m, FarcasterNetwork::Devnet)
                .is_ok()
        })
        .count();
    results.push(CaseResult {
        name: "precheck, registered signer",
        per_op: per_op(start.elapsed(), iterations),
        accepted,
        total: iterations,
    });

    let start = Instant::now();
    let accepted = precheck_forged
        .iter()
        .filter(|m| {
            rate_limits
                .charge_rpc_message(m, FarcasterNetwork::Devnet)
                .is_ok()
        })
        .count();
    results.push(CaseResult {
        name: "precheck, unregistered signer",
        per_op: per_op(start.elapsed(), iterations),
        accepted,
        total: iterations,
    });

    let start = Instant::now();
    let mut accepted = 0;
    for message in &simulate_valid {
        accepted += simulate(&stores, message).await as usize;
    }
    results.push(CaseResult {
        name: "simulate, registered signer",
        per_op: per_op(start.elapsed(), iterations),
        accepted,
        total: iterations,
    });

    let start = Instant::now();
    let mut accepted = 0;
    for message in &simulate_forged {
        accepted += simulate(&stores, message).await as usize;
    }
    results.push(CaseResult {
        name: "simulate, unregistered signer",
        per_op: per_op(start.elapsed(), iterations),
        accepted,
        total: iterations,
    });

    println!("{:<32} {:>12} {:>14}", "case", "per message", "accepted");
    for result in &results {
        println!(
            "{:<32} {:>12?} {:>8}/{:<6}",
            result.name, result.per_op, result.accepted, result.total
        );
    }
    let ratio = |simulate: &CaseResult, precheck: &CaseResult| {
        simulate.per_op.as_secs_f64() / precheck.per_op.as_secs_f64().max(f64::EPSILON)
    };
    println!(
        "simulate / precheck: {:.1}x (registered), {:.1}x (unregistered)",
        ratio(&results[2], &results[0]),
        ratio(&results[3], &results[1])
    );
    Ok(())
}
