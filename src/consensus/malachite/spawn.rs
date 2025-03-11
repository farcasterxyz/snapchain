use informalsystems_malachitebft_config::{TimeoutConfig, ValueSyncConfig};
use informalsystems_malachitebft_core_consensus::{ValuePayload, VoteSyncMode};
use informalsystems_malachitebft_engine::consensus::{Consensus, ConsensusParams, ConsensusRef};
use informalsystems_malachitebft_engine::host::HostRef;
use informalsystems_malachitebft_engine::network::NetworkRef;
use informalsystems_malachitebft_network::{PeerId as MalachitePeerId, PeerIdExt};
use informalsystems_malachitebft_sync::Metrics as SyncMetrics;
use std::path::Path;
use std::time::Duration;
use tracing::Span;

use crate::consensus::consensus::Config;
use crate::consensus::malachite::host::{Host, HostState};
use crate::consensus::malachite::network_connector::{
    MalachiteNetworkActorMsg, MalachiteNetworkConnector, MalachiteNetworkEvent,
    NetworkConnectorArgs,
};
use crate::consensus::malachite::snapchain_codec::SnapchainCodec;
use crate::consensus::validator::ShardValidator;
use crate::core::types::{ShardId, SnapchainValidatorContext};
use crate::network::gossip::GossipEvent;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use informalsystems_malachitebft_engine::sync::{Params as SyncParams, Sync, SyncRef};
use informalsystems_malachitebft_engine::util::events::TxEvent;
use informalsystems_malachitebft_engine::wal::{Wal, WalRef};
use informalsystems_malachitebft_metrics::{Metrics, SharedRegistry};
use libp2p::PeerId;
use tokio::sync::mpsc;

pub async fn spawn_network_actor(
    gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    local_peer_id: PeerId,
) -> Result<NetworkRef<SnapchainValidatorContext>, ractor::SpawnErr> {
    let codec = SnapchainCodec;
    let args = NetworkConnectorArgs {
        gossip_tx,
        peer_id: MalachitePeerId::from_libp2p(&local_peer_id),
    };
    MalachiteNetworkConnector::spawn(codec, args)
        .await
        .map_err(Into::into)
}

pub async fn spawn_wal_actor(
    home_dir: &Path,
    ctx: SnapchainValidatorContext,
    registry: &SharedRegistry,
    span: Span,
) -> Result<WalRef<SnapchainValidatorContext>, ractor::SpawnErr> {
    let wal_dir = home_dir.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let wal_file = wal_dir.join("consensus.wal");
    let codec = SnapchainCodec;

    Wal::spawn(&ctx, codec, wal_file, registry.clone(), span)
        .await
        .map_err(Into::into)
}

pub async fn spawn_host(
    network: NetworkRef<SnapchainValidatorContext>,
    shard_validator: ShardValidator,
    gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
    consensus_start_delay: u32,
    statsd: StatsdClientWrapper,
) -> Result<HostRef<SnapchainValidatorContext>, ractor::SpawnErr> {
    let state = HostState {
        network,
        shard_validator,
        consensus_start_delay,
        gossip_tx,
        statsd,
    };
    let actor_ref = Host::spawn(state).await?;
    Ok(actor_ref)
}

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
    span: Span,
) -> Result<ConsensusRef<SnapchainValidatorContext>, ractor::SpawnErr> {
    let consensus_params = ConsensusParams {
        initial_height,
        initial_validator_set,
        address,
        threshold_params: Default::default(),
        value_payload: ValuePayload::ProposalAndParts,
        vote_sync_mode: VoteSyncMode::RequestResponse,
    };
    let signing_provider = ctx.signing_provider();

    Consensus::spawn(
        ctx,
        consensus_params,
        timeout_cfg,
        Box::new(signing_provider),
        network,
        host,
        wal,
        sync,
        metrics,
        tx_event,
        span,
    )
    .await
    .map_err(Into::into)
}

pub async fn spawn_sync_actor(
    ctx: SnapchainValidatorContext,
    network: NetworkRef<SnapchainValidatorContext>,
    host: HostRef<SnapchainValidatorContext>,
    config: ValueSyncConfig,
    registry: &SharedRegistry,
    span: Span,
) -> Result<SyncRef<SnapchainValidatorContext>, ractor::SpawnErr> {
    let params = SyncParams {
        status_update_interval: config.status_update_interval,
        request_timeout: config.request_timeout,
    };

    let metrics = SyncMetrics::register(registry);

    let actor_ref = Sync::spawn(ctx, network, host, params, metrics, span).await?;

    Ok(actor_ref)
}

#[derive(Clone)]
pub struct MalachiteConsensusActors {
    pub network_actor: NetworkRef<SnapchainValidatorContext>,
    pub wal_actor: WalRef<SnapchainValidatorContext>,
    pub host_actor: HostRef<SnapchainValidatorContext>,
    pub sync_actor: SyncRef<SnapchainValidatorContext>,
    pub consensus_actor: ConsensusRef<SnapchainValidatorContext>,
}

fn timeout_from_config(config: &Config) -> TimeoutConfig {
    TimeoutConfig {
        timeout_propose: config.step_time,
        timeout_prevote: config.step_time,
        timeout_precommit: config.step_time,
        timeout_precommit_delta: config.step_delta,
        timeout_prevote_delta: config.step_delta,
        timeout_propose_delta: config.step_delta,
        timeout_commit: config.block_time, // Sets up a fixed block production rate
        timeout_step: Duration::from_secs(10),
    }
}

impl MalachiteConsensusActors {
    pub async fn create_and_start(
        ctx: SnapchainValidatorContext,
        shard_validator: ShardValidator,
        local_peer_id: PeerId,
        db_dir: String,
        gossip_tx: mpsc::Sender<GossipEvent<SnapchainValidatorContext>>,
        registry: &SharedRegistry,
        config: Config,
        statsd: StatsdClientWrapper,
    ) -> Result<Self, ractor::SpawnErr> {
        let current_height = shard_validator.get_current_height();
        let validator_set = shard_validator.get_validator_set();
        let address = shard_validator.get_address();
        let shard_id = shard_validator.shard_id.shard_id();
        let name = if shard_id == 0 {
            format!("{:} Block", address.prefix())
        } else {
            format!("{:} Shard {}", address.prefix(), shard_id)
        };
        let span = tracing::info_span!("node", name = %name);

        let network_actor = spawn_network_actor(gossip_tx.clone(), local_peer_id).await?;
        let wal_actor = spawn_wal_actor(
            Path::new(format!("{}/shard-{}/wal", db_dir, shard_id).as_str()),
            ctx.clone(),
            registry,
            span.clone(),
        )
        .await?;
        let host_actor = spawn_host(
            network_actor.clone(),
            shard_validator,
            gossip_tx.clone(),
            config.consensus_start_delay,
            statsd,
        )
        .await?;
        let sync_actor = spawn_sync_actor(
            ctx.clone(),
            network_actor.clone(),
            host_actor.clone(),
            ValueSyncConfig::default(),
            registry,
            span.clone(),
        )
        .await?;

        let timeout_config = timeout_from_config(&config);
        let consensus_actor = spawn_consensus_actor(
            ctx.clone(),
            timeout_config,
            current_height,
            validator_set,
            address,
            network_actor.clone(),
            host_actor.clone(),
            wal_actor.clone(),
            Some(sync_actor.clone()),
            Metrics::new(),
            TxEvent::new(),
            span,
        )
        .await?;

        Ok(Self {
            network_actor,
            wal_actor,
            host_actor,
            sync_actor,
            consensus_actor,
        })
    }

    pub fn cast_network_event(
        &self,
        event: MalachiteNetworkEvent,
    ) -> Result<(), ractor::MessagingErr<MalachiteNetworkActorMsg>> {
        self.network_actor
            .cast(MalachiteNetworkActorMsg::NewEvent(event))
    }

    pub fn stop(&self) {
        self.consensus_actor.stop(None);
        self.host_actor.stop(None);
        self.network_actor.stop(None);
        self.wal_actor.stop(None);
        self.sync_actor.stop(None);
    }
}
