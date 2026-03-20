use crate::{
    connectors::{self},
    consensus,
    mempool::{self, block_receiver},
    network::{self, http_server},
    proto::FarcasterNetwork,
    storage,
};
use clap::Parser;
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::Path;
use std::time::Duration;

pub const DEFAULT_GOSSIP_PORT: u16 = 3382;
pub const DEFAULT_RPC_PORT: u16 = 3383;
pub const DEFAULT_HTTP_PORT: u16 = 3381;

#[derive(Debug, Deserialize, Serialize)]
pub struct StatsdConfig {
    pub prefix: String,
    pub addr: String,
    pub use_tags: bool,
}

impl Default for StatsdConfig {
    fn default() -> Self {
        Self {
            prefix: "".to_string(), //TODO: "snapchain" eventually
            addr: "127.0.0.1:8125".to_string(),
            use_tags: true,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PruningConfig {
    #[serde(
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none",
        default // TODO: for now defaults to None, but should be 1mo.
    )]
    pub block_retention: Option<Duration>,
    #[serde(with = "humantime_serde")]
    pub event_retention: Duration,
}

impl Default for PruningConfig {
    fn default() -> Self {
        Self {
            block_retention: None,
            event_retention: Duration::from_secs(60 * 60 * 24 * 3), // 3 days
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReplicationConfig {
    pub enable: bool,

    // Note: you shouldn't set these values in the config file, they are
    // intended to be statically defined across the whole network.
    pub snapshot_interval: u64, // Specified in number of blocks

    #[serde(with = "humantime_serde")]
    pub snapshot_max_age: Duration,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enable: true,
            snapshot_interval: (60 * 60 * 8), // every ~8 hours (in number of blocks)
            snapshot_max_age: Duration::from_secs(60 * 60 * 24), // keep snapshots for 24 hours
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LoggingConfig {
    pub level: String,
    pub preset: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub consensus_level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub storage_level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub network_level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub mempool_level: Option<String>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            preset: "default".to_string(),
            consensus_level: None,
            storage_level: None,
            network_level: None,
            mempool_level: None,
        }
    }
}

impl LoggingConfig {
    pub fn build_env_filter(&self) -> String {
        let (preset_consensus, preset_storage, preset_network, preset_mempool) =
            match self.preset.as_str() {
                "production" => ("warn", "warn", "error", "warn"),
                _ => (
                    self.level.as_str(),
                    self.level.as_str(),
                    self.level.as_str(),
                    self.level.as_str(),
                ),
            };

        let consensus = self
            .consensus_level
            .as_deref()
            .unwrap_or(preset_consensus);
        let storage = self.storage_level.as_deref().unwrap_or(preset_storage);
        let network = self.network_level.as_deref().unwrap_or(preset_network);
        let mempool = self.mempool_level.as_deref().unwrap_or(preset_mempool);

        format!(
            "snapchain::consensus={consensus},\
             snapchain::storage={storage},\
             snapchain::network={network},\
             snapchain::mempool={mempool},\
             {global}",
            consensus = consensus,
            storage = storage,
            network = network,
            mempool = mempool,
            global = self.level,
        )
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub log_format: String,
    pub logging: LoggingConfig,
    pub fnames: connectors::fname::Config,
    pub onchain_events: connectors::onchain_events::Config,
    pub base_onchain_events: connectors::onchain_events::Config,
    pub consensus: consensus::consensus::Config,
    pub gossip: network::gossip::Config,
    pub mempool: mempool::mempool::Config,
    pub snapshot: storage::db::snapshot::Config,
    pub rpc_auth: String,
    pub admin_rpc_auth: String,
    pub rpc_address: String,
    pub http_address: String,
    pub rocksdb_dir: String,
    pub clear_db: bool,
    pub statsd: StatsdConfig,
    pub l1_rpc_url: String,
    pub fc_network: FarcasterNetwork,
    pub read_node: bool,
    pub pruning: PruningConfig,
    pub http_server: http_server::Config,
    pub replication: ReplicationConfig,
    pub block_receiver: block_receiver::Config,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            log_format: "text".to_string(),
            logging: LoggingConfig::default(),
            fnames: connectors::fname::Config::default(),
            onchain_events: connectors::onchain_events::Config::default(),
            base_onchain_events: connectors::onchain_events::Config::default(),
            consensus: consensus::consensus::Config::default(),
            gossip: network::gossip::Config::default(),
            mempool: mempool::mempool::Config::default(),
            rpc_auth: "".to_string(),
            admin_rpc_auth: "".to_string(),
            rpc_address: format!("0.0.0.0:{}", DEFAULT_RPC_PORT),
            http_address: format!("0.0.0.0:{}", DEFAULT_HTTP_PORT),
            rocksdb_dir: ".rocks".to_string(),
            clear_db: false,
            statsd: StatsdConfig::default(),
            l1_rpc_url: "".to_string(),
            fc_network: FarcasterNetwork::Devnet,
            snapshot: storage::db::snapshot::Config::default(),
            read_node: false,
            pruning: PruningConfig::default(),
            http_server: http_server::Config::default(),
            replication: ReplicationConfig::default(),
            block_receiver: block_receiver::Config::default(),
        }
    }
}

#[derive(Parser)]
pub struct CliArgs {
    #[arg(long, help = "Log format (text or json)")]
    log_format: Option<String>,

    #[arg(long, help = "Path to the config file")]
    config_path: String,

    #[arg(long, action, help = "Start the node with a clean database")]
    clear_db: bool,

    #[arg(
        long,
        help = "Minimum log level: error | warn | info | debug | trace (overrides config)"
    )]
    log_level: Option<String>,
}

pub fn load_and_merge_config(args: Vec<String>) -> Result<Config, Box<dyn Error>> {
    let cli_args = CliArgs::try_parse_from(args)?;

    let mut figment = Figment::from(Serialized::defaults(Config::default()));

    if Path::new(&cli_args.config_path).exists() {
        figment = figment.merge(Toml::file(&cli_args.config_path));
    } else {
        return Err(format!("config file not found: {}", &cli_args.config_path).into());
    }

    figment = figment.merge(Env::prefixed("SNAPCHAIN_").split("__"));

    let mut config: Config = figment.extract()?;

    if let Some(log_format) = cli_args.log_format {
        config.log_format = log_format;
    }
    if let Some(log_level) = cli_args.log_level {
        config.logging.level = log_level;
    }
    config.clear_db = cli_args.clear_db;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_filter_is_info() {
        let cfg = LoggingConfig::default();
        let filter = cfg.build_env_filter();
        assert!(filter.ends_with(",info"), "filter was: {filter}");
        assert!(filter.contains("snapchain::consensus=info"), "filter was: {filter}");
        assert!(filter.contains("snapchain::storage=info"), "filter was: {filter}");
    }

    #[test]
    fn test_production_preset_quiets_hot_paths() {
        let cfg = LoggingConfig {
            preset: "production".to_string(),
            ..LoggingConfig::default()
        };
        let filter = cfg.build_env_filter();
        assert!(filter.contains("snapchain::consensus=warn"), "filter was: {filter}");
        assert!(filter.contains("snapchain::storage=warn"), "filter was: {filter}");
        assert!(filter.contains("snapchain::network=error"), "filter was: {filter}");
        assert!(filter.contains("snapchain::mempool=warn"), "filter was: {filter}");
        assert!(filter.ends_with(",info"), "filter was: {filter}");
    }

    #[test]
    fn test_per_subsystem_override_beats_preset() {
        let cfg = LoggingConfig {
            preset: "production".to_string(),
            consensus_level: Some("debug".to_string()), 
            ..LoggingConfig::default()
        };
        let filter = cfg.build_env_filter();
        assert!(filter.contains("snapchain::consensus=debug"), "filter was: {filter}");
        assert!(filter.contains("snapchain::storage=warn"), "filter was: {filter}");
    }

    #[test]
    fn test_cli_log_level_override() {
        let mut config = Config::default();
        config.logging.level = "debug".to_string();
        let filter = config.logging.build_env_filter();
        assert!(filter.ends_with(",debug"), "filter was: {filter}");
    }
}
