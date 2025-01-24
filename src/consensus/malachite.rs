use informalsystems_malachitebft_config::SyncConfig;
use informalsystems_malachitebft_engine::host::HostRef;
use informalsystems_malachitebft_engine::network::NetworkRef;
use informalsystems_malachitebft_sync::Metrics as SyncMetrics;
use tracing::Span;

use crate::core::types::SnapchainValidatorContext;
use informalsystems_malachitebft_engine::sync::{Params as SyncParams, Sync, SyncCodec, SyncRef};
use informalsystems_malachitebft_metrics::SharedRegistry;

pub async fn spawn_network_actor() {}

pub async fn spawn_wal_actor() {}

pub async fn spawn_connector() {}

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
