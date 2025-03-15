use std::sync::OnceLock;
use tracing_subscriber::{reload, EnvFilter, Registry};

static RELOAD_HANDLE: OnceLock<reload::Handle<EnvFilter, Registry>> = OnceLock::new();
static DEFAULT_LOG_LEVEL: OnceLock<String> = OnceLock::new();

pub use informalsystems_malachitebft_config::LogLevel;

pub fn set_reload_handle(handle: reload::Handle<EnvFilter, Registry>) {
    if RELOAD_HANDLE.set(handle).is_err() {
        eprintln!("ERROR: Failed to set the reload handle");
    }
}

pub fn set_default_log_level(level: String) {
    if DEFAULT_LOG_LEVEL.set(level).is_err() {
        eprintln!("ERROR: Failed to set the default log level");
    }
}

pub fn reset() {
    let log_level = match DEFAULT_LOG_LEVEL.get() {
        Some(level) => level,
        None => &"info".to_string(),
    };

    reload_env_filter(EnvFilter::new(log_level));
}

pub fn reload(log_level: LogLevel) {
    let env_filter = EnvFilter::new(log_level.to_string());
    reload_env_filter(env_filter);
}

fn reload_env_filter(env_filter: EnvFilter) {
    tracing::info!("Reloading log level: {env_filter}");

    if let Some(handle) = RELOAD_HANDLE.get() {
        if let Err(e) = handle.reload(env_filter) {
            tracing::error!("Failed to reload the log level: {e}");
        }
    } else {
        tracing::error!("ERROR: Failed to get the reload handle");
    }
}
