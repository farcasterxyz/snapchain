use crate::core::util::FarcasterTime;
use crate::proto::{FarcasterNetwork, HubEventType, MessageType};
use crate::storage::db::RocksDbTransactionBatch;
use crate::storage::store::engine::{ShardEngine, ShardStateChange};
use crate::storage::store::mempool_poller::MempoolMessage;
use crate::storage::store::stores::{Limits, StoreLimits};
use crate::storage::store::test_helper::{
    self, default_custody_address, default_signer, lend_storage, limits, numbered_reaction,
    register_user, EngineOptions, FID2_FOR_TEST, FID_FOR_TEST,
};
use crate::version::version::{EngineVersion, ProtocolFeature};
use informalsystems_malachitebft_core_types::Round;

/// Largest number of non-`BlockConfirmed` events a block can carry: after `k` such events
/// the id generator's `current_seq` is `k + 1`, and `BlockConfirmed` needs it below 2^14.
const MAX_EVENTS_BEFORE_BLOCK_CONFIRMED: u32 = (1 << 14) - 2;

/// Most prune events one block may emit once `BoundedPruneSweep` is active.
const PRUNE_EVENT_BUDGET_PER_BLOCK: u32 = 4096;

/// Testnet's V24 activation.
const TESTNET_V24_ACTIVE_AT: u64 = 1_794_844_800;

/// A past testnet time between V23 and V24, so it never depends on the wall clock.
fn before_bounded_sweeps() -> (FarcasterNetwork, FarcasterTime) {
    (
        FarcasterNetwork::Testnet,
        FarcasterTime::from_unix_seconds(TESTNET_V24_ACTIVE_AT - 4 * 86_400),
    )
}

/// Devnet runs the latest version from time zero. Block times stay at or before now, since the
/// engine treats block time as already past.
fn with_bounded_sweeps() -> (FarcasterNetwork, FarcasterTime) {
    (FarcasterNetwork::Devnet, FarcasterTime::current())
}

#[test]
fn test_setups_straddle_the_bounded_sweep_gate() {
    let version = |(network, time): (FarcasterNetwork, FarcasterTime)| {
        EngineVersion::version_for(&time, network)
    };
    assert_eq!(version(before_bounded_sweeps()), EngineVersion::V23);
    assert!(version(with_bounded_sweeps()).is_enabled(ProtocolFeature::BoundedPruneSweep));
}

fn reaction_limits(reactions_per_unit: u32) -> StoreLimits {
    StoreLimits::new(
        Limits {
            reactions: reactions_per_unit,
            ..limits::test()
        },
        limits::zero(),
        limits::zero(),
    )
}

fn reaction_usage(engine: &ShardEngine, fid: u64) -> (u32, u32) {
    engine
        .get_stores()
        .get_usage(
            fid,
            MessageType::ReactionAdd,
            &mut RocksDbTransactionBatch::new(),
        )
        .unwrap()
}

fn prune_event_count(state_change: &ShardStateChange) -> usize {
    state_change
        .events
        .iter()
        .filter(|event| event.r#type() == HubEventType::PruneMessage)
        .count()
}

async fn engine_with_reactions_per_unit(
    network: FarcasterNetwork,
    reactions_per_unit: u32,
) -> (ShardEngine, tempfile::TempDir) {
    let (mut engine, tmp) = test_helper::new_engine_with_options(EngineOptions {
        limits: Some(reaction_limits(reactions_per_unit)),
        network: Some(network),
        ..Default::default()
    })
    .await;
    for fid in [FID_FOR_TEST, FID2_FOR_TEST] {
        register_user(
            fid,
            default_signer(),
            default_custody_address(),
            &mut engine,
        )
        .await;
    }
    (engine, tmp)
}

/// One fid holding `stored` reactions, with its only unit lent away so its cap is zero.
async fn over_cap_engine(
    network: FarcasterNetwork,
    stored: u32,
    block_time: &FarcasterTime,
) -> (ShardEngine, tempfile::TempDir) {
    let (mut engine, tmp) = engine_with_reactions_per_unit(network, stored).await;
    test_helper::fill_reactions(&mut engine, FID_FOR_TEST, stored, block_time).await;
    lend_storage(&mut engine, FID_FOR_TEST, FID2_FOR_TEST, 1, 1, block_time).await;
    assert_eq!(reaction_usage(&engine, FID_FOR_TEST), (stored, 0));
    (engine, tmp)
}

fn trigger(fid: u64, block_time: &FarcasterTime) -> MempoolMessage {
    MempoolMessage::UserMessage(numbered_reaction(fid, u64::MAX, block_time.to_u64() as u32))
}

/// Proposes, validates, then commits through the replay path (no cached transaction),
/// so all three block paths process the sweep.
async fn propose_validate_and_replay_commit(
    engine: &mut ShardEngine,
    messages: Vec<MempoolMessage>,
    block_time: &FarcasterTime,
) -> ShardStateChange {
    let height = engine.get_confirmed_height().increment();
    engine.start_round(height, Round::Nil);
    let state_change = engine.propose_state_change(1, messages, Some(block_time.clone()));

    engine.start_round(height, Round::Nil);
    assert!(engine.validate_state_change(&state_change, height));

    engine.start_round(height, Round::Nil);
    let chunk = test_helper::state_change_to_shard_chunk(1, height.block_number, &state_change);
    engine.commit_shard_chunk(&chunk).await;
    assert_eq!(engine.trie_root_hash(), state_change.new_state_root);
    assert_eq!(engine.get_confirmed_height(), height);
    state_change
}

#[tokio::test]
async fn sweep_that_exactly_fits_the_event_id_space_still_prunes() {
    let (network, at) = before_bounded_sweeps();
    // The sweep walks committed rows only, so the trigger itself survives: one merge
    // event plus `stored` prune events lands exactly on the ceiling. Current code commits
    // this block, so behavior must not change.
    let stored = MAX_EVENTS_BEFORE_BLOCK_CONFIRMED - 1;
    let (mut engine, _tmp) = over_cap_engine(network, stored, &at).await;

    let state_change =
        propose_validate_and_replay_commit(&mut engine, vec![trigger(FID_FOR_TEST, &at)], &at)
            .await;

    assert_eq!(prune_event_count(&state_change), stored as usize);
    assert_eq!(reaction_usage(&engine, FID_FOR_TEST), (1, 0));
}

#[tokio::test]
async fn sweep_one_past_the_event_id_space_is_skipped_and_the_block_commits() {
    let (network, at) = before_bounded_sweeps();
    let stored = MAX_EVENTS_BEFORE_BLOCK_CONFIRMED;
    let (mut engine, _tmp) = over_cap_engine(network, stored, &at).await;

    let state_change =
        propose_validate_and_replay_commit(&mut engine, vec![trigger(FID_FOR_TEST, &at)], &at)
            .await;

    assert_eq!(prune_event_count(&state_change), 0);
    assert_eq!(reaction_usage(&engine, FID_FOR_TEST), (stored + 1, 0));
}

#[tokio::test]
async fn sweep_is_skipped_when_earlier_transactions_used_the_event_id_space() {
    let (network, at) = before_bounded_sweeps();
    // Neither sweep overflows alone; together they would. Whichever fid replays second
    // must be skipped, and the first must prune normally.
    let first = 10_000;
    let second = 7_000;
    let (mut engine, _tmp) = engine_with_reactions_per_unit(network, first).await;
    test_helper::fill_reactions(&mut engine, FID_FOR_TEST, first, &at).await;
    test_helper::fill_reactions(&mut engine, FID2_FOR_TEST, second, &at).await;
    lend_storage(&mut engine, FID_FOR_TEST, FID2_FOR_TEST + 1, 1, 1, &at).await;
    lend_storage(&mut engine, FID2_FOR_TEST, FID2_FOR_TEST + 1, 1, 2, &at).await;

    let state_change = propose_validate_and_replay_commit(
        &mut engine,
        vec![trigger(FID_FOR_TEST, &at), trigger(FID2_FOR_TEST, &at)],
        &at,
    )
    .await;

    let usage = [
        reaction_usage(&engine, FID_FOR_TEST),
        reaction_usage(&engine, FID2_FOR_TEST),
    ];
    let pruned = usage.iter().filter(|u| **u == (1, 0)).count();
    let skipped = usage
        .iter()
        .filter(|u| **u == (first + 1, 0) || **u == (second + 1, 0))
        .count();
    assert_eq!((pruned, skipped), (1, 1), "usage after block: {:?}", usage);
    assert!(prune_event_count(&state_change) > 0);
}

#[tokio::test]
async fn bounded_sweep_prunes_up_to_the_block_budget_and_leaves_the_rest() {
    let (network, at) = with_bounded_sweeps();
    let stored = 10_000;
    let (mut engine, _tmp) = over_cap_engine(network, stored, &at).await;

    let state_change =
        propose_validate_and_replay_commit(&mut engine, vec![trigger(FID_FOR_TEST, &at)], &at)
            .await;

    assert_eq!(
        prune_event_count(&state_change),
        PRUNE_EVENT_BUDGET_PER_BLOCK as usize
    );
    assert_eq!(
        reaction_usage(&engine, FID_FOR_TEST),
        (stored + 1 - PRUNE_EVENT_BUDGET_PER_BLOCK, 0)
    );
}

#[tokio::test]
async fn bounded_sweep_resumes_on_the_fids_next_message() {
    let (network, at) = with_bounded_sweeps();
    let stored = 10_000;
    let (mut engine, _tmp) = over_cap_engine(network, stored, &at).await;
    let timestamp = at.to_u64() as u32;

    let mut remaining = stored;
    for i in 0..3u64 {
        let message =
            MempoolMessage::UserMessage(numbered_reaction(FID_FOR_TEST, u64::MAX - i, timestamp));
        let state_change =
            propose_validate_and_replay_commit(&mut engine, vec![message], &at).await;
        // Each sweep walks the rows committed before its block, so the new message survives
        // its own block and joins the rows the next sweep can reach.
        let pruned = remaining.min(PRUNE_EVENT_BUDGET_PER_BLOCK);
        assert_eq!(prune_event_count(&state_change), pruned as usize);
        remaining = remaining + 1 - pruned;
        assert_eq!(reaction_usage(&engine, FID_FOR_TEST), (remaining, 0));
    }
}

#[tokio::test]
async fn bounded_sweeps_share_one_budget_per_block() {
    let (network, at) = with_bounded_sweeps();
    let first = 3_000;
    let second = 3_000;
    let (mut engine, _tmp) = engine_with_reactions_per_unit(network, first).await;
    test_helper::fill_reactions(&mut engine, FID_FOR_TEST, first, &at).await;
    test_helper::fill_reactions(&mut engine, FID2_FOR_TEST, second, &at).await;
    lend_storage(&mut engine, FID_FOR_TEST, FID2_FOR_TEST + 1, 1, 1, &at).await;
    lend_storage(&mut engine, FID2_FOR_TEST, FID2_FOR_TEST + 1, 1, 2, &at).await;

    let state_change = propose_validate_and_replay_commit(
        &mut engine,
        vec![trigger(FID_FOR_TEST, &at), trigger(FID2_FOR_TEST, &at)],
        &at,
    )
    .await;

    assert_eq!(
        prune_event_count(&state_change),
        PRUNE_EVENT_BUDGET_PER_BLOCK as usize
    );
    let remaining =
        reaction_usage(&engine, FID_FOR_TEST).0 + reaction_usage(&engine, FID2_FOR_TEST).0;
    assert_eq!(remaining, first + second + 2 - PRUNE_EVENT_BUDGET_PER_BLOCK);

    // The budget resets each block.
    let state_change = propose_validate_and_replay_commit(
        &mut engine,
        vec![MempoolMessage::UserMessage(numbered_reaction(
            FID_FOR_TEST,
            u64::MAX - 1,
            at.to_u64() as u32,
        ))],
        &at,
    )
    .await;
    assert!(prune_event_count(&state_change) > 0);
}

#[tokio::test]
async fn bounded_sweep_leaves_ordinary_prunes_unchanged() {
    let (network, at) = with_bounded_sweeps();
    // A fid at its cap prunes one row per new message, as before the gate.
    let (mut engine, _tmp) = engine_with_reactions_per_unit(network, 5).await;
    test_helper::fill_reactions(&mut engine, FID_FOR_TEST, 5, &at).await;

    let state_change =
        propose_validate_and_replay_commit(&mut engine, vec![trigger(FID_FOR_TEST, &at)], &at)
            .await;

    assert_eq!(prune_event_count(&state_change), 1);
    assert_eq!(reaction_usage(&engine, FID_FOR_TEST), (5, 5));
}
