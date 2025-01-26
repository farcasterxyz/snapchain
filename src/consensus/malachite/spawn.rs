use informalsystems_malachitebft_config::{SyncConfig, TimeoutConfig};
use informalsystems_malachitebft_core_consensus::ValuePayload;
use informalsystems_malachitebft_engine::consensus::{Consensus, ConsensusParams, ConsensusRef};
use informalsystems_malachitebft_engine::host::HostRef;
use informalsystems_malachitebft_engine::network::NetworkRef;
use informalsystems_malachitebft_sync::Metrics as SyncMetrics;
use std::path::Path;
use tracing::{error, Span};

use crate::core::types::SnapchainValidatorContext;
use informalsystems_malachitebft_engine::sync::{Params as SyncParams, Sync, SyncCodec, SyncRef};
use informalsystems_malachitebft_engine::util::events::TxEvent;
use informalsystems_malachitebft_engine::wal::{Wal, WalRef};
use informalsystems_malachitebft_metrics::prometheus::registry::Metric;
use informalsystems_malachitebft_metrics::{Metrics, SharedRegistry};

pub async fn spawn_network_actor() {}

// pub async fn spawn_wal_actor(home_dir: &Path, ctx: SnapchainValidatorContext, registry: &SharedRegistry) -> Result<WalRef<SnapchainValidatorContext>, ractor::SpawnErr> {
//     let wal_dir = home_dir.join("wal");
//     std::fs::create_dir_all(&wal_dir).unwrap();
//
//     let wal_file = wal_dir.join("consensus.wal");
//
//     Wal::spawn(&ctx, codec, wal_file, registry.clone(), Span::current())
//         .await
//         .map_err(Into::into)
// }

pub async fn spawn_connector() {}

pub async fn spawn_consensus_actor(
    ctx: SnapchainValidatorContext,
    timeout_cfg: TimeoutConfig,
    initial_height: <SnapchainValidatorContext as informalsystems_malachitebft_core_types::Context>::Height,
    initial_validator_set: <SnapchainValidatorContext as informalsystems_malachitebft_core_types::Context>::ValidatorSet,
    address: <SnapchainValidatorContext as informalsystems_malachitebft_core_types::Context>::Address,
    network: NetworkRef<SnapchainValidatorContext>,
    host: HostRef<SnapchainValidatorContext>,
    wal: WalRef<SnapchainValidatorContext>,
    sync: Option<SyncRef<SnapchainValidatorContext>>,
    metrics: Metrics,
    tx_event: TxEvent<SnapchainValidatorContext>,
) -> Result<ConsensusRef<SnapchainValidatorContext>, ractor::SpawnErr> {
    let consensus_params = ConsensusParams {
        initial_height,
        initial_validator_set,
        address,
        threshold_params: Default::default(),
        value_payload: ValuePayload::ProposalAndParts,
    };

    Consensus::spawn(
        ctx,
        consensus_params,
        timeout_cfg,
        network,
        host,
        wal,
        sync,
        metrics,
        tx_event,
        Span::current(),
    )
    .await
    .map_err(Into::into)
}

pub async fn spawn_sync_actor(
    ctx: SnapchainValidatorContext,
    network: NetworkRef<SnapchainValidatorContext>,
    host: HostRef<SnapchainValidatorContext>,
    config: SyncConfig,
    registry: &SharedRegistry,
) -> Result<Option<SyncRef<SnapchainValidatorContext>>, ractor::SpawnErr> {
    let params = SyncParams {
        status_update_interval: config.status_update_interval,
        request_timeout: config.request_timeout,
    };

    let metrics = SyncMetrics::register(registry);

    let actor_ref = Sync::spawn(ctx, network, host, params, metrics, Span::current()).await?;

    Ok(Some(actor_ref))
}

pub async fn run() {}
