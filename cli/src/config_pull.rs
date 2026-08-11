//! `fc config pull` — fetch the onchain-managed node configuration from the
//! SnapchainConfigRegistry and merge it into a local `config.toml`.
//!
//! The registry renders a TOML fragment whose byte-level format is specified in
//! `farcasterxyz/contracts` (`docs/snapchain-config-registry.md`). This module is
//! the consumer side: it replaces exactly three keys in the local file —
//! `consensus.validator_sets`, `gossip.bootstrap_peers`, `gossip.direct_peers` —
//! and leaves every other key's *value*, `consensus.private_key` above all,
//! untouched. One opt-out: with `--accept-local-bootstrap-peers-config`, a
//! non-empty local `gossip.bootstrap_peers` is treated as operator-managed and
//! survives the merge (see `splice_managed_keys`); by default the registry's
//! list replaces it like the other managed keys. The file's *formatting* does not survive: the merge parses into
//! `toml::Table` and re-serializes, so the first pull strips comments and
//! re-sorts keys alphabetically. Operators who keep annotations in
//! `config.toml`, or tooling that greps it by shape, must not point this
//! command at that file. (Preserving formatting would mean `toml_edit`; the
//! deploy scripts regenerate the file from a heredoc every boot, so nothing on
//! the managed path keeps comments to lose.)
//!
//! Validation here is structural and covers only the three managed keys.
//! Full-config validation belongs to the node binary (`snapchain --check-config`),
//! which owns the real loader; the deploy scripts run it between pulling and
//! restarting the node.

use std::path::{Path, PathBuf};

use alloy_primitives::Address;
use alloy_sol_types::{sol, SolCall};
use serde::Deserialize;

use crate::{BoxedError, NetworkArg};

/// Cap on the eth_call HTTP response. configToml() output is bounded onchain
/// (8 KiB peer strings, capped key arrays), so a few KiB is normal; anything
/// approaching this cap is not the registry.
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

sol! {
    /// The one read `config pull` needs from ISnapchainConfigRegistry
    /// (farcasterxyz/contracts).
    function configToml() external view returns (string memory);
}

/// Registry address baked in per network. The mainnet registry lives on
/// Ethereum L1 (chain 1); the testnet registry lives on Sepolia (11155111).
/// C5's single-salt CREATE2 plan may make the two addresses coincide — keep
/// them separate constants anyway: a redeploy on either chain breaks the
/// coincidence. Devnet deployments are ephemeral and always need `--registry`.
fn baked_in_registry(_network: NetworkArg) -> Option<Address> {
    // NEYN-13022: fill in the mainnet (L1) and testnet (Sepolia) addresses
    // once the registry deploy lands. Devnet stays `None` permanently.
    None
}

/// The chain each network's registry lives on. Devnet (local anvil, arbitrary
/// chain id) is unchecked.
fn expected_chain_id(network: NetworkArg) -> Option<u64> {
    match network {
        NetworkArg::Mainnet => Some(1),
        NetworkArg::Testnet => Some(11_155_111), // Sepolia
        NetworkArg::Devnet => None,
    }
}

#[derive(clap::Args)]
pub struct ConfigPullArgs {
    /// SnapchainConfigRegistry contract address. Overrides the baked-in
    /// per-network address; required for devnet.
    #[arg(long)]
    registry: Option<String>,

    /// Ethereum JSON-RPC URL for the chain the selected network's registry
    /// lives on (mainnet: Ethereum L1; testnet: Sepolia). Mainnet defaults to
    /// `l1_rpc_url` from the target config file; testnet and devnet require
    /// this flag, since `l1_rpc_url` intentionally stays on Ethereum mainnet
    /// (the node uses it for ENS resolution).
    #[arg(long)]
    rpc_url: Option<String>,

    /// Path to the node config.toml to merge into.
    #[arg(long)]
    config: PathBuf,

    /// Print the merged document to stdout instead of writing the file
    /// (consensus.private_key is shown redacted).
    #[arg(long)]
    dry_run: bool,

    /// Keep a non-empty `gossip.bootstrap_peers` already enumerated in the
    /// local config instead of adopting the registry's list — for operators
    /// managing their own bootstrap topology (e.g. private addresses the
    /// registry's public list cannot carry). Without this flag, or when the
    /// local list is empty or absent, the registry's list applies.
    /// `consensus.validator_sets` and `gossip.direct_peers` are always taken
    /// from the registry regardless.
    #[arg(long)]
    accept_local_bootstrap_peers_config: bool,
}

/// Mirrors `ValidatorSetConfig` in `snapchain/src/consensus/consensus.rs`.
/// Redeclared locally so the CLI doesn't depend on snapchain proper at runtime;
/// the dev-dependency parity test asserts the two shapes stay identical.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ValidatorSetConfig {
    effective_at: u64,
    validator_public_keys: Vec<String>,
    shard_ids: Vec<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OnchainConsensus {
    validator_sets: Vec<ValidatorSetConfig>,
}

// The peer fields are never read directly — deserializing them is the point:
// required-field and deny_unknown_fields checks are what validate the shape.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OnchainGossip {
    bootstrap_peers: String,
    direct_peers: String,
}

/// The complete document `configToml()` renders. `deny_unknown_fields` is the
/// fail-closed choice: a registry that starts emitting keys this binary does not
/// understand should stop the sync loudly, not silently half-apply.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OnchainDoc {
    consensus: Option<OnchainConsensus>,
    gossip: OnchainGossip,
}

pub async fn run(args: ConfigPullArgs, network: NetworkArg) -> Result<(), BoxedError> {
    // Resolve symlinks up front so the rewrite replaces the real file, not the
    // symlink pointing at it.
    let config_path = std::fs::canonicalize(&args.config)
        .map_err(|e| format!("cannot resolve {}: {e}", args.config.display()))?;
    let local_raw = std::fs::read_to_string(&config_path)
        .map_err(|e| format!("cannot read {}: {e}", config_path.display()))?;
    let mut local: toml::Table = toml::from_str(&local_raw)
        .map_err(|e| sanitized_toml_error(&config_path.display().to_string(), &local_raw, &e))?;

    let env_fc_network = std::env::var("SNAPCHAIN_FC_NETWORK").ok();
    check_network_matches(&local, network, env_fc_network.as_deref())?;

    // The registry manages validators only; read nodes keep their own peer
    // configuration path (spec §2.5). A dry run is harmless inspection.
    let env_read_node = std::env::var("SNAPCHAIN_READ_NODE").ok();
    let is_read_node = effective_read_node(&local, env_read_node.as_deref())?;
    if is_read_node {
        if !args.dry_run {
            return Err(format!(
                "{} resolves to read_node = true (via the file or the SNAPCHAIN_READ_NODE \
                 overlay); the registry manages validator config only \
                 (use --dry-run to inspect the merge anyway)",
                args.config.display()
            )
            .into());
        }
        eprintln!("warning: read_node = true — the registry manages validator config only");
    }

    let registry = resolve_registry(args.registry.as_deref(), network)?;
    let env_l1_rpc_url = std::env::var("SNAPCHAIN_L1_RPC_URL").ok();
    let rpc_url = resolve_rpc_url(args.rpc_url, &local, network, env_l1_rpc_url.as_deref())?;

    // Deliberately not printing the RPC URL: hosted-provider URLs carry the
    // API key in the path, and this line lands in log collectors.
    eprintln!("Fetching configToml() from {registry}");
    if rpc_url.starts_with("http://") {
        eprintln!(
            "warning: --rpc-url is plaintext http and its response chooses the validator set; \
             use https outside local devnets"
        );
    }
    let client = http_client()?;
    check_chain_id(&client, &rpc_url, network).await?;
    let rendered = fetch_config_toml(&client, &rpc_url, registry).await?;

    let onchain: toml::Table =
        toml::from_str(&rendered).map_err(|e| format!("registry returned invalid TOML: {e}"))?;
    validate_onchain(&onchain)?;

    splice_managed_keys(
        &mut local,
        &onchain,
        args.accept_local_bootstrap_peers_config,
    )?;

    let merged =
        toml::to_string(&local).map_err(|e| format!("cannot serialize merged config: {e}"))?;
    // Belt and braces: never emit a document the parser itself would reject.
    toml::from_str::<toml::Table>(&merged)
        .map_err(|e| sanitized_toml_error("merged config does not re-parse", &merged, &e))?;

    if args.dry_run {
        print!("{}", redact_secrets(local));
        return Ok(());
    }

    write_replace(&config_path, &merged, &local_raw)?;
    eprintln!("Wrote {}", config_path.display());
    Ok(())
}

/// Refuse to splice one network's registry output into a config that declares
/// another. Latent until the baked-in registry addresses land; after that,
/// `fc config pull` (which defaults to mainnet) run against a testnet node's
/// config would otherwise silently install the mainnet validator set.
fn check_network_matches(
    local: &toml::Table,
    network: NetworkArg,
    env_fc_network: Option<&str>,
) -> Result<(), BoxedError> {
    // The node's loader overlays SNAPCHAIN_* env vars over the file, so the
    // env value — when set — is what the node will actually run with and
    // takes precedence here too.
    let declared = match env_fc_network.or_else(|| local.get("fc_network").and_then(|v| v.as_str()))
    {
        Some(s) => s,
        // Declared nowhere (the node would default it): nothing to check.
        None => return Ok(()),
    };
    let selected = match network {
        NetworkArg::Mainnet => "Mainnet",
        NetworkArg::Testnet => "Testnet",
        NetworkArg::Devnet => "Devnet",
    };
    if declared.eq_ignore_ascii_case(selected) {
        Ok(())
    } else {
        Err(format!(
            "config file declares fc_network = {declared:?} but the selected network is \
             {selected}; pass a matching --network"
        )
        .into())
    }
}

/// The node mode this config will actually run with. The node's figment
/// loader overlays `SNAPCHAIN_READ_NODE` over the file — same rule as
/// `fc_network` above — and parses it strictly: exactly `"true"` or
/// `"false"`, anything else aborts config loading (verified against the
/// loader in `snapchain::tests::cfg_tests::read_node_env_overlay_is_strict`).
/// Mirror both halves: honor the overlay, and refuse values the node would
/// refuse rather than guessing.
fn effective_read_node(
    local: &toml::Table,
    env_read_node: Option<&str>,
) -> Result<bool, BoxedError> {
    match env_read_node {
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(other) => Err(format!(
            "SNAPCHAIN_READ_NODE={other:?} is not a value the node's loader accepts \
             (exactly \"true\" or \"false\"); the node would fail to start with this \
             environment"
        )
        .into()),
        None => Ok(local
            .get("read_node")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)),
    }
}

/// Render a TOML error without echoing the offending source line. The local
/// config contains the consensus private key; the default error Display quotes
/// the source line, which would land verbatim in captured stderr.
fn sanitized_toml_error(context: &str, source: &str, err: &toml::de::Error) -> String {
    match err.span() {
        Some(span) => {
            let line = source[..span.start.min(source.len())].matches('\n').count() + 1;
            format!(
                "{context}: TOML parse error at line {line}: {}",
                err.message()
            )
        }
        None => format!("{context}: TOML parse error: {}", err.message()),
    }
}

/// Dry-run output goes to stdout, which deployment pipelines capture into log
/// collectors — mask every credential-bearing key the node config can hold,
/// not just the private key: `l1_rpc_url` and the `onchain_events` /
/// `base_onchain_events` `rpc_url`s carry the provider API key in their path
/// (the same reason this module never prints the RPC URL), `rpc_auth` /
/// `admin_rpc_auth` are credentials outright, and `snapshot.aws_*` are AWS
/// keys. The three managed keys, the thing dry-run exists to inspect, are
/// unaffected. Keys are redacted only when present and non-empty, so the
/// output still shows which of them the config sets.
fn redact_secrets(mut merged: toml::Table) -> String {
    fn redact(table: &mut toml::Table, key: &str) {
        let is_set = matches!(table.get(key), Some(toml::Value::String(s)) if !s.is_empty());
        if is_set {
            table.insert(
                key.to_string(),
                toml::Value::String("<redacted>".to_string()),
            );
        }
    }
    for key in ["l1_rpc_url", "rpc_auth", "admin_rpc_auth"] {
        redact(&mut merged, key);
    }
    if let Some(consensus) = merged.get_mut("consensus").and_then(|c| c.as_table_mut()) {
        redact(consensus, "private_key");
    }
    if let Some(snapshot) = merged.get_mut("snapshot").and_then(|c| c.as_table_mut()) {
        redact(snapshot, "aws_access_key_id");
        redact(snapshot, "aws_secret_access_key");
    }
    for table in ["onchain_events", "base_onchain_events"] {
        if let Some(events) = merged.get_mut(table).and_then(|c| c.as_table_mut()) {
            redact(events, "rpc_url");
        }
    }
    toml::to_string(&merged).expect("table serialized successfully before redaction")
}

/// Replace `path` with `contents` without exposing a world-readable copy of the
/// config (it carries the consensus private key) and without a window where a
/// crash leaves a truncated file: unique temp sibling created 0600, contents
/// fsync'd, the original file's permissions copied over, atomic rename, then a
/// best-effort directory fsync so the rename itself survives power loss.
fn write_replace(path: &Path, contents: &str, expected_current: &str) -> Result<(), BoxedError> {
    use std::io::Write as _;

    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("config path {} has no file name", path.display()))?;
    // PID-unique name so a racing pull cannot write into our temp file and get
    // its half-written bytes renamed into place.
    let tmp = path.with_file_name(format!(".{file_name}.tmp.{}", std::process::id()));

    // Sweep temp files left by a previous pull that was killed between write
    // and rename — each is a stale full copy of the config, private key
    // included, that would otherwise sit on disk forever.
    if let Some(dir) = path.parent() {
        let stale_prefix = format!(".{file_name}.tmp.");
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if name.starts_with(&stale_prefix) && entry.path() != tmp {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }

    let original_meta =
        std::fs::metadata(path).map_err(|e| format!("cannot stat {}: {e}", path.display()))?;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        // Create with the original file's mode from the start: it changes
        // nothing security-wise (the same bytes already sit at that mode in
        // the original), and setting it before sync_all means the mode is on
        // the inode the fsync covers — a chmod after the sync need not
        // survive a crash.
        use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
        options.mode(original_meta.mode() & 0o7777);
    }

    let written = (|| -> Result<(), BoxedError> {
        let mut file = options
            .open(&tmp)
            .map_err(|e| format!("cannot create {}: {e}", tmp.display()))?;
        file.write_all(contents.as_bytes())
            .map_err(|e| format!("cannot write {}: {e}", tmp.display()))?;
        file.sync_all()
            .map_err(|e| format!("cannot sync {}: {e}", tmp.display()))?;
        #[cfg(unix)]
        {
            // Best-effort ownership carry-over for the root-runs-fc,
            // node-runs-unprivileged split; EPERM when not privileged is fine
            // (we are then already running as the file's effective audience).
            use std::os::unix::fs::MetadataExt as _;
            let _ = std::os::unix::fs::chown(
                &tmp,
                Some(original_meta.uid()),
                Some(original_meta.gid()),
            );
        }
        // Refuse to clobber edits made while we were talking to the RPC (key
        // rotation, operator hand-edit): our merge was computed from a
        // snapshot. Narrows the lost-update window from the whole network
        // round-trip to the compare-rename gap; single-writer discipline is
        // the deploy script's job.
        let current = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot re-read {}: {e}", path.display()))?;
        if current != expected_current {
            return Err(format!(
                "{} changed while the pull was running; re-run to merge against the new contents",
                path.display()
            )
            .into());
        }
        std::fs::rename(&tmp, path).map_err(|e| {
            format!(
                "cannot rename {} over {}: {e} (note: a single-file bind mount cannot be \
                 replaced by rename)",
                tmp.display(),
                path.display()
            )
        })?;
        Ok(())
    })();

    if written.is_err() {
        // Never leave a stray copy of the private key on disk.
        let _ = std::fs::remove_file(&tmp);
        return written;
    }

    // Sync the directory so the rename itself survives power loss. Failure is
    // survivable (the old config would reappear, not a torn one) but should
    // not be silent.
    if let Some(dir) = path.parent() {
        let dir_sync = std::fs::File::open(dir).and_then(|d| d.sync_all());
        if let Err(e) = dir_sync {
            eprintln!(
                "warning: could not sync {} after rename ({e}); the update may not survive \
                 an immediate power loss",
                dir.display()
            );
        }
    }
    Ok(())
}

fn resolve_registry(flag: Option<&str>, network: NetworkArg) -> Result<Address, BoxedError> {
    if let Some(addr) = flag {
        return addr
            .parse::<Address>()
            .map_err(|e| format!("invalid --registry address {addr:?}: {e}").into());
    }
    baked_in_registry(network).ok_or_else(|| {
        match network {
            NetworkArg::Devnet => "devnet has no baked-in registry; pass --registry",
            NetworkArg::Mainnet | NetworkArg::Testnet => {
                "no registry deployed for this network yet (NEYN-13022); pass --registry"
            }
        }
        .into()
    })
}

fn resolve_rpc_url(
    flag: Option<String>,
    local: &toml::Table,
    network: NetworkArg,
    env_l1_rpc_url: Option<&str>,
) -> Result<String, BoxedError> {
    if let Some(url) = flag {
        return Ok(url);
    }
    match network {
        // Only mainnet may fall back to the l1_rpc_url the node runs with:
        // every node points that URL at Ethereum mainnet (it exists for ENS
        // resolution), which is also where the mainnet registry lives. The
        // SNAPCHAIN_L1_RPC_URL env overlay wins over the file value, mirroring
        // the node's own loader — same rule as the fc_network and read_node
        // overlays above.
        NetworkArg::Mainnet => {
            let from_env = env_l1_rpc_url.filter(|url| !url.is_empty());
            let from_file = local
                .get("l1_rpc_url")
                .and_then(|v| v.as_str())
                .filter(|url| !url.is_empty());
            match from_env.or(from_file) {
                Some(url) => Ok(url.to_string()),
                None => Err("no --rpc-url given and neither the config file nor \
                             SNAPCHAIN_L1_RPC_URL sets an l1_rpc_url"
                    .into()),
            }
        }
        NetworkArg::Testnet => Err(
            "the testnet registry lives on Sepolia; pass --rpc-url with a Sepolia endpoint (the \
             config's l1_rpc_url intentionally points at Ethereum mainnet for ENS and is not a \
             safe fallback)"
                .into(),
        ),
        NetworkArg::Devnet => Err("devnet requires an explicit --rpc-url".into()),
    }
}

fn http_client() -> Result<reqwest::Client, BoxedError> {
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| format!("cannot build HTTP client: {e}").into())
}

/// POST a JSON-RPC request and parse the response body, buffering at most
/// MAX_RESPONSE_BYTES. Every RPC read goes through this cap, not just the
/// document fetch: this binary runs on the validator boot path, and a broken
/// or malicious endpoint must not be able to OOM it with an unbounded body.
/// reqwest errors embed the request URL, which routinely carries an RPC API
/// key in its path — strip it (without_url) before the message reaches logs.
async fn post_json_capped(
    client: &reqwest::Client,
    rpc_url: &str,
    request: &serde_json::Value,
    what: &str,
) -> Result<serde_json::Value, BoxedError> {
    let mut http_response = client
        .post(rpc_url)
        .json(request)
        .send()
        .await
        .map_err(|e| format!("{what} request failed: {}", e.without_url()))?
        .error_for_status()
        .map_err(|e| format!("{what} HTTP error: {}", e.without_url()))?;
    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = http_response
        .chunk()
        .await
        .map_err(|e| format!("{what} read failed: {}", e.without_url()))?
    {
        body.extend_from_slice(&chunk);
        if body.len() > MAX_RESPONSE_BYTES {
            return Err(format!("{what} response exceeds {MAX_RESPONSE_BYTES} bytes").into());
        }
    }
    serde_json::from_slice(&body).map_err(|e| format!("{what} response is not JSON: {e}").into())
}

/// Refuse to read a registry-shaped address on the wrong chain — the one
/// mistake the mainnet-on-L1 / testnet-on-Sepolia split makes easy.
async fn check_chain_id(
    client: &reqwest::Client,
    rpc_url: &str,
    network: NetworkArg,
) -> Result<(), BoxedError> {
    let Some(expected) = expected_chain_id(network) else {
        return Ok(());
    };
    let request = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": [],
    });
    let response = post_json_capped(client, rpc_url, &request, "eth_chainId").await?;
    let got = response
        .get("result")
        .and_then(|r| r.as_str())
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .ok_or("eth_chainId returned no parseable chain id")?;
    if got != expected {
        return Err(format!(
            "RPC endpoint reports chain id {got}, but the selected network's registry lives on \
             chain {expected}; check --rpc-url"
        )
        .into());
    }
    Ok(())
}

async fn fetch_config_toml(
    client: &reqwest::Client,
    rpc_url: &str,
    registry: Address,
) -> Result<String, BoxedError> {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            {
                "to": format!("{registry}"),
                "data": format!("0x{}", hex::encode(configTomlCall {}.abi_encode())),
            },
            "latest",
        ],
    });
    let response = post_json_capped(client, rpc_url, &request, "eth_call").await?;
    if let Some(error) = response.get("error") {
        // Deliberately not echoing the raw error object: providers reflect
        // request details into it, including credential-bearing endpoint URLs.
        let code = error.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
        let message: String = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("(no message)")
            .chars()
            .take(200)
            .collect();
        return Err(format!("eth_call failed: code {code}: {message}").into());
    }
    let result = response
        .get("result")
        .and_then(|r| r.as_str())
        .ok_or("eth_call response carries neither result nor error")?;
    let raw = hex::decode(result.trim_start_matches("0x"))
        .map_err(|e| format!("eth_call result is not hex: {e}"))?;
    Ok(configTomlCall::abi_decode_returns(&raw)
        .map_err(|e| format!("cannot ABI-decode configToml() return: {e}"))?)
}

/// Structural validation of the rendered document, before any of it touches the
/// local file. Everything rejected here would otherwise surface as a panic or
/// misbehaviour at node startup — after the restart, when it is most expensive.
fn validate_onchain(onchain: &toml::Table) -> Result<(), BoxedError> {
    let doc: OnchainDoc = onchain
        .clone()
        .try_into()
        .map_err(|e| format!("registry document has unexpected shape: {e}"))?;

    let sets = match &doc.consensus {
        Some(c) if !c.validator_sets.is_empty() => &c.validator_sets,
        // An unseeded registry renders a bare [gossip] block. Syntactically
        // valid, but a node loading it panics on the empty set (spec §3.2).
        _ => return Err("registry has no validator sets — is it seeded?".into()),
    };

    let mut last_effective_at: std::collections::BTreeMap<u32, u64> =
        std::collections::BTreeMap::new();
    for (i, set) in sets.iter().enumerate() {
        if set.shard_ids.is_empty() {
            return Err(format!("validator set {i}: empty shard_ids").into());
        }
        if set.validator_public_keys.is_empty() {
            return Err(format!("validator set {i}: empty validator_public_keys").into());
        }
        for key in &set.validator_public_keys {
            // The node's startup does hex::decode(..).unwrap() and then full
            // Ed25519 point decompression, also unwrapped (src/consensus/
            // validator.rs:59, libp2p ed25519 PublicKey::try_from_bytes —
            // ed25519-dalek underneath). Roughly half of all 32-byte values
            // are not curve points, so hex-validity alone is not enough:
            // anything the node would panic on must die here instead.
            match hex::decode(key) {
                Ok(bytes) if bytes.len() == 32 => {
                    let arr: [u8; 32] = bytes.try_into().expect("length checked above");
                    if ed25519_dalek::VerifyingKey::from_bytes(&arr).is_err() {
                        return Err(format!(
                            "validator set {i}: key {key:?} is not a valid Ed25519 public key"
                        )
                        .into());
                    }
                }
                Ok(bytes) => {
                    return Err(format!(
                        "validator set {i}: key {key:?} decodes to {} bytes, expected 32",
                        bytes.len()
                    )
                    .into())
                }
                Err(e) => {
                    return Err(format!("validator set {i}: key {key:?} is not hex: {e}").into())
                }
            }
        }
        // effective_at is a per-shard height: shard counters advance
        // independently, so entries governing disjoint shards are unordered
        // relative to each other. Mirror the contract exactly: it keeps a
        // per-shard running maximum (`_lastEffectiveAt`) and bounds each
        // entry, for every shard it governs, against that shard's latest
        // height (>=, since sibling rollouts land on the same height). A
        // single nearest-overlapping-entry comparison is weaker: a
        // multi-shard entry would only be checked against one of its shards'
        // histories.
        for shard in &set.shard_ids {
            if let Some(&latest) = last_effective_at.get(shard) {
                if set.effective_at < latest {
                    return Err(format!(
                        "validator set {i}: effective_at {} regresses below {latest}, the \
                         latest height previously set for shard {shard}",
                        set.effective_at
                    )
                    .into());
                }
            }
        }
        for shard in &set.shard_ids {
            last_effective_at.insert(*shard, set.effective_at);
        }
    }

    // Entry 0 seeds the node's active-set scan unconditionally, so it must be
    // the genesis entry: effective_at 0 AND covering every shard the document
    // mentions (spec §2.3).
    let all_shards: std::collections::BTreeSet<u32> = sets
        .iter()
        .flat_map(|s| s.shard_ids.iter().copied())
        .collect();
    let entry0_shards: std::collections::BTreeSet<u32> =
        sets[0].shard_ids.iter().copied().collect();
    if sets[0].effective_at != 0 || !entry0_shards.is_superset(&all_shards) {
        // Hard error, not a warning: the node's active-set scan seeds from
        // sets[0] unconditionally, without checking its shards, so a partial
        // entry 0 silently governs shards it never listed. The spec requires
        // entry 0 to be the genesis entry and no legitimate registry violates
        // it.
        return Err(
            "first validator set must be the genesis entry (effective_at = 0, every shard \
             listed) — the node's active-set scan seeds from it unconditionally"
                .into(),
        );
    }

    Ok(())
}

/// Replace the managed keys in `local` with the values from `onchain`,
/// splicing the parsed values directly so the write-back carries exactly what
/// the registry rendered. An empty peer string is a value, not "unset" (spec
/// §2.4) — it replaces like any other.
///
/// `accept_local_bootstrap_peers` is the `--accept-local-bootstrap-peers-config`
/// opt-out: when set, a local config that already enumerates a non-empty
/// `gossip.bootstrap_peers` belongs to an operator managing that list
/// themselves (e.g. private addresses the registry's public list cannot
/// carry), and it survives the merge. An absent key or an empty/
/// whitespace-only string still adopts the registry list even with the flag,
/// and without the flag the registry replaces the list unconditionally — the
/// default keeps every node converging on the registry's canonical peers.
/// `consensus.validator_sets` (consensus membership) and `gossip.direct_peers`
/// are registry-authoritative either way. The registry document must carry
/// all three keys in every mode — a document missing one is malformed and
/// fails the pull.
fn splice_managed_keys(
    local: &mut toml::Table,
    onchain: &toml::Table,
    accept_local_bootstrap_peers: bool,
) -> Result<(), BoxedError> {
    const MANAGED: [(&str, &str); 3] = [
        ("consensus", "validator_sets"),
        ("gossip", "bootstrap_peers"),
        ("gossip", "direct_peers"),
    ];
    for (table, key) in MANAGED {
        let value = onchain
            .get(table)
            .and_then(|t| t.as_table())
            .and_then(|t| t.get(key))
            .ok_or_else(|| format!("registry document is missing {table}.{key}"))?
            .clone();
        if accept_local_bootstrap_peers && (table, key) == ("gossip", "bootstrap_peers") {
            let enumerated = local
                .get(table)
                .and_then(|t| t.as_table())
                .and_then(|t| t.get(key))
                .is_some_and(|v| match v.as_str() {
                    Some(s) => !s.trim().is_empty(),
                    // A non-string value is operator data too (and a loader
                    // error the node will report); never silently clobber it.
                    None => true,
                });
            if enumerated {
                continue;
            }
        }
        let target = local
            .entry(table.to_string())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        let target = target
            .as_table_mut()
            .ok_or_else(|| format!("local config: [{table}] is not a table"))?;
        target.insert(key.to_string(), value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worked example from the rendering spec (§3.2), byte-for-byte.
    const ONCHAIN: &str = r#"[[consensus.validator_sets]]
effective_at = 0
shard_ids = [0, 1, 2]
validator_public_keys = [
  "29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970",
  "6bc2d8901443de856d2670b0c2ea12b6727132fa830f9030d3a44ac5da9b1a72",
]

[gossip]
bootstrap_peers = "/ip4/10.0.0.148/udp/3382/quic-v1, /ip4/10.0.2.165/udp/3382/quic-v1"
direct_peers = "12D3KooWGmXDC2SfjSG7h7DchyVJHMB4GpA8JYpHf9iwz8L8BFqB"
"#;

    const PRIVATE_KEY: &str = "b2fd4d29e5e1d9bcf90fcaa9757ae2d47b7fc396c9d7b7d963d2aa1b3ba1c1ab";

    fn onchain_table() -> toml::Table {
        toml::from_str(ONCHAIN).unwrap()
    }

    /// Validate + splice + serialize + re-parse, the same path `run` takes
    /// with default flags.
    fn merge(local: &str) -> toml::Table {
        merge_with(local, false)
    }

    fn merge_with(local: &str, accept_local_bootstrap_peers: bool) -> toml::Table {
        let mut local: toml::Table = toml::from_str(local).unwrap();
        let onchain = onchain_table();
        validate_onchain(&onchain).unwrap();
        splice_managed_keys(&mut local, &onchain, accept_local_bootstrap_peers).unwrap();
        let rendered = toml::to_string(&local).unwrap();
        toml::from_str(&rendered).unwrap()
    }

    fn assert_managed_keys_applied(merged: &toml::Table) {
        let consensus = merged["consensus"].as_table().unwrap();
        assert_eq!(
            consensus["private_key"].as_str().unwrap(),
            PRIVATE_KEY,
            "private_key must survive the merge byte-identical"
        );
        let sets = consensus["validator_sets"].as_array().unwrap();
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0]["effective_at"].as_integer(), Some(0));
        let gossip = merged["gossip"].as_table().unwrap();
        assert!(gossip["bootstrap_peers"]
            .as_str()
            .unwrap()
            .starts_with("/ip4/10.0.0.148"));
        assert!(gossip["direct_peers"]
            .as_str()
            .unwrap()
            .starts_with("12D3Koo"));
    }

    #[test]
    fn merges_over_existing_gossip_table() {
        // Default mode: an enumerated local bootstrap list is replaced like
        // every other managed key.
        let merged = merge(&format!(
            r#"
l1_rpc_url = "https://example.invalid/rpc"

[consensus]
private_key = "{PRIVATE_KEY}"

[gossip]
address = "/ip4/0.0.0.0/udp/3382/quic-v1"
bootstrap_peers = "/ip4/9.9.9.9/udp/3382/quic-v1"
direct_peers = ""
"#
        ));
        assert_managed_keys_applied(&merged);
        // Unmanaged gossip keys survive.
        assert_eq!(
            merged["gossip"]["address"].as_str().unwrap(),
            "/ip4/0.0.0.0/udp/3382/quic-v1"
        );
    }

    #[test]
    fn flag_keeps_enumerated_bootstrap_peers() {
        // --accept-local-bootstrap-peers-config: a non-empty local bootstrap
        // list is operator-managed and survives, while validator_sets and
        // direct_peers stay registry-authoritative in the same merge.
        let merged = merge_with(
            &format!(
                r#"
[consensus]
private_key = "{PRIVATE_KEY}"

[gossip]
bootstrap_peers = "/ip4/9.9.9.9/udp/3382/quic-v1"
direct_peers = "12D3KooOldPeerThatMustGo"
"#
            ),
            true,
        );
        assert_eq!(
            merged["gossip"]["bootstrap_peers"].as_str().unwrap(),
            "/ip4/9.9.9.9/udp/3382/quic-v1"
        );
        assert!(merged["gossip"]["direct_peers"]
            .as_str()
            .unwrap()
            .starts_with("12D3KooWGmX"));
        assert_eq!(
            merged["consensus"]["validator_sets"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn flag_still_adopts_registry_when_local_list_is_blank() {
        // Empty or whitespace-only = not enumerated, even with the flag.
        for blank in ["\"\"", "\"  \""] {
            let merged = merge_with(
                &format!(
                    r#"
[consensus]
private_key = "{PRIVATE_KEY}"

[gossip]
bootstrap_peers = {blank}
"#
                ),
                true,
            );
            assert_managed_keys_applied(&merged);
        }
    }

    #[test]
    fn flag_keeps_non_string_bootstrap_peers_for_the_loader_to_reject() {
        // Wrong-typed operator data is still operator data under the flag:
        // leave it in place so `snapchain --check-config` reports the real
        // error instead of the merge silently papering over it. Without the
        // flag it is replaced like any other value.
        let raw = r#"
[gossip]
bootstrap_peers = ["/ip4/9.9.9.9/udp/3382/quic-v1"]
"#;
        let onchain = onchain_table();
        validate_onchain(&onchain).unwrap();

        let mut local: toml::Table = toml::from_str(raw).unwrap();
        splice_managed_keys(&mut local, &onchain, true).unwrap();
        assert!(local["gossip"]["bootstrap_peers"].is_array());

        let mut local: toml::Table = toml::from_str(raw).unwrap();
        splice_managed_keys(&mut local, &onchain, false).unwrap();
        assert!(local["gossip"]["bootstrap_peers"].is_str());
    }

    #[test]
    fn creates_gossip_table_when_absent() {
        let merged = merge(&format!(
            r#"
[consensus]
private_key = "{PRIVATE_KEY}"
"#
        ));
        assert_managed_keys_applied(&merged);
    }

    #[test]
    fn replaces_existing_array_of_tables_validator_sets() {
        let merged = merge(&format!(
            r#"
[consensus]
private_key = "{PRIVATE_KEY}"

[[consensus.validator_sets]]
effective_at = 0
shard_ids = [ 0 ]
validator_public_keys = [ "aa" ]

[[consensus.validator_sets]]
effective_at = 100
shard_ids = [ 1 ]
validator_public_keys = [ "bb" ]
"#
        ));
        // Replaced wholesale, not appended: two stale entries -> one onchain entry.
        assert_managed_keys_applied(&merged);
    }

    #[test]
    fn replaces_inline_array_validator_sets() {
        // The shape the Neynar validator entrypoint writes:
        // validator_sets = ${VALIDATOR_SETS}
        let merged = merge(&format!(
            r#"
[consensus]
private_key = "{PRIVATE_KEY}"
validator_sets = [{{ effective_at = 5, shard_ids = [0], validator_public_keys = ["cc"] }}]
"#
        ));
        assert_managed_keys_applied(&merged);
    }

    #[test]
    fn empty_peer_string_is_a_value_not_unset() {
        let onchain_empty_peers = ONCHAIN
            .replace(
                "direct_peers = \"12D3KooWGmXDC2SfjSG7h7DchyVJHMB4GpA8JYpHf9iwz8L8BFqB\"",
                "direct_peers = \"\"",
            )
            .parse::<toml::Table>()
            .unwrap();
        let mut local: toml::Table = toml::from_str(
            r#"
[gossip]
direct_peers = "12D3KooOldPeerThatMustGo"
bootstrap_peers = "x"
"#,
        )
        .unwrap();
        validate_onchain(&onchain_empty_peers).unwrap();
        splice_managed_keys(&mut local, &onchain_empty_peers, false).unwrap();
        assert_eq!(local["gossip"]["direct_peers"].as_str(), Some(""));
        // Default mode: the local bootstrap list is replaced like any other
        // managed key (--accept-local-bootstrap-peers-config would keep it).
        assert!(local["gossip"]["bootstrap_peers"]
            .as_str()
            .unwrap()
            .starts_with("/ip4/10.0.0.148"));
    }

    #[test]
    fn rejects_unseeded_registry() {
        let bare_gossip: toml::Table =
            toml::from_str("[gossip]\nbootstrap_peers = \"\"\ndirect_peers = \"\"\n").unwrap();
        let err = validate_onchain(&bare_gossip).unwrap_err().to_string();
        assert!(err.contains("no validator sets"), "got: {err}");
    }

    #[test]
    fn rejects_unknown_onchain_keys() {
        let mut onchain = onchain_table();
        onchain
            .get_mut("gossip")
            .and_then(|g| g.as_table_mut())
            .unwrap()
            .insert("mystery_knob".into(), toml::Value::Boolean(true));
        let err = validate_onchain(&onchain).unwrap_err().to_string();
        assert!(err.contains("unexpected shape"), "got: {err}");
    }

    #[test]
    fn rejects_malformed_key() {
        let onchain: toml::Table = toml::from_str(&ONCHAIN.replace(
            "29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970",
            "zz96eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970",
        ))
        .unwrap();
        assert!(validate_onchain(&onchain).is_err());
    }

    #[test]
    fn rejects_wrong_length_key() {
        let onchain: toml::Table = toml::from_str(&ONCHAIN.replace(
            "29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970",
            "296eca0909",
        ))
        .unwrap();
        assert!(validate_onchain(&onchain).is_err());
    }

    #[test]
    fn rejects_non_monotonic_effective_at() {
        let two_sets = format!(
            "{}\n[[consensus.validator_sets]]\neffective_at = 0\nshard_ids = [0]\nvalidator_public_keys = [\"{}\"]\n",
            ONCHAIN.replace("effective_at = 0", "effective_at = 10"),
            "29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970",
        );
        let onchain: toml::Table = toml::from_str(&two_sets).unwrap();
        let err = validate_onchain(&onchain).unwrap_err().to_string();
        assert!(err.contains("regresses below"), "got: {err}");
    }

    /// A validator-set block in the onchain grammar, with a known-good key.
    fn set_block(effective_at: u64, shard_ids: &str) -> String {
        format!(
            "[[consensus.validator_sets]]\neffective_at = {effective_at}\nshard_ids = \
             {shard_ids}\nvalidator_public_keys = [\n  \
             \"29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970\",\n]\n\n"
        )
    }

    const GOSSIP_BLOCK: &str = "[gossip]\nbootstrap_peers = \"\"\ndirect_peers = \"\"\n";

    #[test]
    fn accepts_lower_effective_at_on_disjoint_shards() {
        // Shard counters are independent clocks: shard 1 at height 30M after
        // shard 0 at 50M is a legitimate rollout, not a regression.
        let doc = format!(
            "{}{}{}{GOSSIP_BLOCK}",
            set_block(0, "[0, 1]"),
            set_block(50_000_000, "[0]"),
            set_block(30_000_000, "[1]"),
        );
        let onchain: toml::Table = toml::from_str(&doc).unwrap();
        validate_onchain(&onchain).unwrap();
    }

    #[test]
    fn rejects_regression_within_a_shard_across_interleaved_entries() {
        // Entry 3 regresses shard 0 (60 < 100) even though its immediate array
        // predecessor is a disjoint shard-1 entry — the bound is against the
        // most recent entry sharing a shard, not the previous array element.
        let doc = format!(
            "{}{}{}{}{GOSSIP_BLOCK}",
            set_block(0, "[0, 1]"),
            set_block(100, "[0]"),
            set_block(50, "[1]"),
            set_block(60, "[0]"),
        );
        let onchain: toml::Table = toml::from_str(&doc).unwrap();
        let err = validate_onchain(&onchain).unwrap_err().to_string();
        assert!(err.contains("shard 0"), "got: {err}");
    }

    #[test]
    fn rejects_multi_shard_entry_regressing_one_of_its_shards() {
        // Entry 4 governs [0, 1]; its nearest overlapping predecessor is the
        // shard-1 entry @50 (60 >= 50), but shard 0's latest is 100 — the
        // contract's per-shard _lastEffectiveAt map rejects it, so fc must
        // too. A nearest-overlap comparison passes this wrongly.
        let doc = format!(
            "{}{}{}{}{GOSSIP_BLOCK}",
            set_block(0, "[0, 1]"),
            set_block(100, "[0]"),
            set_block(50, "[1]"),
            set_block(60, "[0, 1]"),
        );
        let onchain: toml::Table = toml::from_str(&doc).unwrap();
        let err = validate_onchain(&onchain).unwrap_err().to_string();
        assert!(err.contains("shard 0"), "got: {err}");
    }

    #[test]
    fn rejects_entry0_that_is_not_a_full_genesis_entry() {
        // Non-zero height on entry 0.
        let doc = format!("{}{GOSSIP_BLOCK}", set_block(10, "[0]"));
        let onchain: toml::Table = toml::from_str(&doc).unwrap();
        let err = validate_onchain(&onchain).unwrap_err().to_string();
        assert!(err.contains("genesis"), "got: {err}");

        // Entry 0 not covering every shard the document mentions: the node
        // seeds its scan from entry 0 without checking shards, so shard 1
        // would silently be governed by an entry that never listed it.
        let doc = format!(
            "{}{}{GOSSIP_BLOCK}",
            set_block(0, "[0]"),
            set_block(100, "[1]"),
        );
        let onchain: toml::Table = toml::from_str(&doc).unwrap();
        let err = validate_onchain(&onchain).unwrap_err().to_string();
        assert!(err.contains("genesis"), "got: {err}");
    }

    #[test]
    fn network_check_honors_env_overlay() {
        // File says Testnet but SNAPCHAIN_FC_NETWORK overrides to Mainnet —
        // the node would run as mainnet, so fc must judge against the env.
        let local: toml::Table = toml::from_str(r#"fc_network = "Testnet""#).unwrap();
        assert!(check_network_matches(&local, NetworkArg::Mainnet, Some("Mainnet")).is_ok());
        assert!(check_network_matches(&local, NetworkArg::Testnet, Some("Mainnet")).is_err());
        // Env set, file silent: env alone decides.
        let empty = toml::Table::new();
        assert!(check_network_matches(&empty, NetworkArg::Mainnet, Some("Testnet")).is_err());
    }

    #[test]
    fn read_node_honors_env_overlay() {
        let read_node_file: toml::Table = toml::from_str("read_node = true").unwrap();
        let validator_file: toml::Table = toml::from_str("read_node = false").unwrap();
        let empty = toml::Table::new();

        // Env unset: the file decides; absent key defaults to validator.
        assert!(effective_read_node(&read_node_file, None).unwrap());
        assert!(!effective_read_node(&validator_file, None).unwrap());
        assert!(!effective_read_node(&empty, None).unwrap());

        // Env set: it is what the node will actually run with, in both
        // directions — a file-declared read node overridden to validator must
        // be pulled for, not refused.
        assert!(!effective_read_node(&read_node_file, Some("false")).unwrap());
        assert!(effective_read_node(&validator_file, Some("true")).unwrap());

        // Values the node's loader rejects must error, not be guessed at.
        for bad in ["TRUE", "1", "0", "yes", ""] {
            let err = effective_read_node(&empty, Some(bad))
                .unwrap_err()
                .to_string();
            assert!(err.contains("SNAPCHAIN_READ_NODE"), "got: {err}");
        }
    }

    /// Parity with the node: the mirrored `ValidatorSetConfig` must deserialize
    /// the real mainnet history identically to `snapchain`'s own struct. If this
    /// fails, the node's config shape moved and the mirror (and likely the
    /// registry renderer) must follow.
    #[test]
    fn mirror_matches_node_validator_set_config() {
        let doc: toml::Table = toml::from_str(include_str!("../../validators.toml")).unwrap();
        let sets = doc["consensus"].as_table().unwrap()["validator_sets"].clone();

        let mirrored: Vec<ValidatorSetConfig> = sets.clone().try_into().unwrap();
        let node: Vec<snapchain::consensus::consensus::ValidatorSetConfig> =
            sets.try_into().unwrap();

        assert!(!mirrored.is_empty());
        assert_eq!(mirrored.len(), node.len());
        for (m, n) in mirrored.iter().zip(&node) {
            assert_eq!(m.effective_at, n.effective_at);
            assert_eq!(m.validator_public_keys, n.validator_public_keys);
            assert_eq!(m.shard_ids, n.shard_ids);
        }
    }

    #[test]
    fn rejects_key_that_is_not_a_curve_point() {
        // Find a 32-byte value that fails Ed25519 point decompression (about
        // half of all values do) rather than hard-coding one.
        let bad = (0u8..=255)
            .map(|b| [b; 32])
            .find(|bytes| ed25519_dalek::VerifyingKey::from_bytes(bytes).is_err())
            .expect("some constant-byte array is not a curve point");
        let onchain: toml::Table = toml::from_str(&ONCHAIN.replace(
            "29696eb40eb900a329a8d2542edef15d552c9ba6ded7882276be1e9eca090970",
            &hex::encode(bad),
        ))
        .unwrap();
        let err = validate_onchain(&onchain).unwrap_err().to_string();
        assert!(err.contains("not a valid Ed25519 public key"), "got: {err}");
    }

    #[test]
    fn rpc_url_fallback_is_mainnet_only() {
        let local: toml::Table =
            toml::from_str(r#"l1_rpc_url = "https://eth.example.invalid""#).unwrap();
        assert_eq!(
            resolve_rpc_url(None, &local, NetworkArg::Mainnet, None).unwrap(),
            "https://eth.example.invalid"
        );
        // l1_rpc_url points at Ethereum mainnet (ENS) on every node — never a
        // valid endpoint for the Sepolia testnet registry.
        let err = resolve_rpc_url(None, &local, NetworkArg::Testnet, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Sepolia"), "got: {err}");
        assert!(resolve_rpc_url(None, &local, NetworkArg::Devnet, None).is_err());
        // An explicit flag always wins, on every network.
        assert_eq!(
            resolve_rpc_url(Some("https://x".into()), &local, NetworkArg::Testnet, None).unwrap(),
            "https://x"
        );
    }

    #[test]
    fn rpc_url_fallback_honors_the_l1_rpc_url_env_overlay() {
        // The node's loader lets SNAPCHAIN_L1_RPC_URL override the file value;
        // the fallback must read the URL the node actually runs with.
        let local: toml::Table =
            toml::from_str(r#"l1_rpc_url = "https://stale.example.invalid""#).unwrap();
        assert_eq!(
            resolve_rpc_url(
                None,
                &local,
                NetworkArg::Mainnet,
                Some("https://live.example.invalid")
            )
            .unwrap(),
            "https://live.example.invalid"
        );
        // An empty overlay is the node default, not a configured value.
        let empty = toml::Table::new();
        assert!(resolve_rpc_url(None, &empty, NetworkArg::Mainnet, Some("")).is_err());
        // The overlay never turns into a testnet fallback: it points at
        // Ethereum mainnet just like the file value.
        assert!(resolve_rpc_url(
            None,
            &local,
            NetworkArg::Testnet,
            Some("https://live.example.invalid")
        )
        .is_err());
    }

    /// Pins the sol! signature to the selector `forge inspect` reports for the
    /// built contract — an edit to the signature would otherwise fail only at
    /// runtime, against the real registry.
    #[test]
    fn config_toml_selector_matches_deployed_abi() {
        assert_eq!(configTomlCall::SELECTOR, [0x5a, 0x62, 0xbd, 0x75]);
    }

    #[test]
    fn expected_chain_ids_per_network() {
        assert_eq!(expected_chain_id(NetworkArg::Mainnet), Some(1));
        assert_eq!(expected_chain_id(NetworkArg::Testnet), Some(11_155_111));
        assert_eq!(expected_chain_id(NetworkArg::Devnet), None);
    }

    #[test]
    fn rejects_network_mismatch_against_fc_network() {
        let local: toml::Table = toml::from_str(r#"fc_network = "Testnet""#).unwrap();
        assert!(check_network_matches(&local, NetworkArg::Testnet, None).is_ok());
        let err = check_network_matches(&local, NetworkArg::Mainnet, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("fc_network"), "got: {err}");
        // Declared nowhere: nothing to cross-check.
        let empty = toml::Table::new();
        assert!(check_network_matches(&empty, NetworkArg::Mainnet, None).is_ok());
    }

    #[test]
    fn dry_run_output_redacts_every_secret_bearing_key() {
        let merged: toml::Table = toml::from_str(&format!(
            r#"
l1_rpc_url = "https://mainnet.example.com/v2/API_KEY_IN_PATH"
rpc_auth = "user:RPC_SECRET"
admin_rpc_auth = "admin:ADMIN_SECRET"

[consensus]
private_key = "{PRIVATE_KEY}"

[snapshot]
aws_access_key_id = "AKIA_ACCESS_KEY"
aws_secret_access_key = "AWS_SECRET_VALUE"

[onchain_events]
rpc_url = "https://opt-mainnet.example.com/v2/L2_API_KEY_IN_PATH"

[base_onchain_events]
rpc_url = "https://base-mainnet.example.com/v2/BASE_API_KEY_IN_PATH"
"#
        ))
        .unwrap();
        let printed = redact_secrets(merged);
        for secret in [
            PRIVATE_KEY,
            "API_KEY_IN_PATH",
            "RPC_SECRET",
            "ADMIN_SECRET",
            "AKIA_ACCESS_KEY",
            "AWS_SECRET_VALUE",
            "L2_API_KEY_IN_PATH",
            "BASE_API_KEY_IN_PATH",
        ] {
            assert!(!printed.contains(secret), "leaked {secret}: {printed}");
        }
        assert!(printed.contains("<redacted>"));
    }

    #[test]
    fn dry_run_redaction_leaves_empty_and_absent_secrets_alone() {
        // Empty strings are the node's defaults for these keys; redacting them
        // would hide which secrets the config actually sets.
        let merged: toml::Table = toml::from_str("l1_rpc_url = \"\"\n").unwrap();
        let printed = redact_secrets(merged);
        assert!(printed.contains("l1_rpc_url = \"\""), "got: {printed}");
        assert!(!printed.contains("<redacted>"));
    }

    #[test]
    fn sanitized_parse_error_does_not_echo_source() {
        let source = format!("private_key = \"{PRIVATE_KEY}\" stray");
        let err = toml::from_str::<toml::Table>(&source).unwrap_err();
        let msg = sanitized_toml_error("test", &source, &err);
        assert!(!msg.contains(PRIVATE_KEY), "leaked source line: {msg}");
        assert!(msg.contains("line 1"), "got: {msg}");
    }

    /// The spec's worked example must pass this binary's structural validation —
    /// the Solidity side asserts the rendered bytes, only the Rust side can
    /// assert they mean what we think.
    #[test]
    fn spec_example_round_trips() {
        let onchain = onchain_table();
        validate_onchain(&onchain).unwrap();
        let doc: OnchainDoc = onchain.try_into().unwrap();
        let sets = doc.consensus.unwrap().validator_sets;
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].shard_ids, vec![0, 1, 2]);
        assert_eq!(sets[0].validator_public_keys.len(), 2);
    }

    /// Render validator sets exactly as the contract's §3.1 grammar does:
    /// underscore-free integers, `[0, 1, 2]` shard arrays, trailing commas in
    /// the key array, two-space indent, blank line closing each block.
    fn render_per_spec_grammar(sets: &[ValidatorSetConfig]) -> String {
        let mut out = String::new();
        for set in sets {
            out.push_str("[[consensus.validator_sets]]\n");
            out.push_str(&format!("effective_at = {}\n", set.effective_at));
            let shards: Vec<String> = set.shard_ids.iter().map(u32::to_string).collect();
            out.push_str(&format!("shard_ids = [{}]\n", shards.join(", ")));
            out.push_str("validator_public_keys = [\n");
            for key in &set.validator_public_keys {
                out.push_str(&format!("  \"{key}\",\n"));
            }
            out.push_str("]\n\n");
        }
        out.push_str("[gossip]\nbootstrap_peers = \"\"\ndirect_peers = \"\"\n");
        out
    }

    /// The full mainnet history rendered through the registry's exact output
    /// grammar must deserialize equal to what the node reads from
    /// validators.toml — the two documents differ in every formatting detail
    /// the spec fixes (digit separators, trailing commas, inline arrays), and
    /// only the parser can prove they mean the same thing.
    #[test]
    fn spec_grammar_rendering_of_mainnet_history_round_trips() {
        let doc: toml::Table = toml::from_str(include_str!("../../validators.toml")).unwrap();
        let original: Vec<ValidatorSetConfig> = doc["consensus"].as_table().unwrap()
            ["validator_sets"]
            .clone()
            .try_into()
            .unwrap();
        assert!(original.len() >= 2, "expected the real multi-entry history");

        let rendered = render_per_spec_grammar(&original);
        let onchain: toml::Table = toml::from_str(&rendered).unwrap();
        validate_onchain(&onchain).unwrap();
        let round_tripped = onchain["consensus"].as_table().unwrap()["validator_sets"]
            .clone()
            .try_into::<Vec<ValidatorSetConfig>>()
            .unwrap();
        assert_eq!(round_tripped, original);
    }

    #[cfg(unix)]
    #[test]
    fn write_replace_preserves_restrictive_permissions() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = std::env::temp_dir().join(format!("fc-config-pull-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(&path, "old = true\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        // A secret-bearing temp orphaned by a previous killed pull must be
        // swept, not accumulated.
        let orphan = dir.join(".config.toml.tmp.99999999");
        std::fs::write(&orphan, "stale secret copy\n").unwrap();

        write_replace(&path, "new = true\n", "old = true\n").unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new = true\n");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "rewrite must not widen a 0600 key file");
        // No stray temp copy of the (secret-bearing) content left behind —
        // neither ours nor the pre-existing orphan.
        let strays: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name() != "config.toml")
            .collect();
        assert!(strays.is_empty(), "stray temp files: {strays:?}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn write_replace_refuses_when_file_changed_underneath() {
        let dir =
            std::env::temp_dir().join(format!("fc-config-pull-changed-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        // The file was rotated after we snapshotted "old = true".
        std::fs::write(&path, "rotated = true\n").unwrap();

        let err = write_replace(&path, "new = true\n", "old = true\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("changed while"), "got: {err}");
        // The rotated content must survive untouched, with no temp left over.
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "rotated = true\n");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Parity for the peer-field mirror: the node's gossip config must still
    /// carry string-typed `bootstrap_peers` / `direct_peers`, or the splice
    /// writes keys the node no longer reads.
    #[test]
    fn mirror_matches_node_gossip_peer_fields() {
        let node_gossip = toml::Value::try_from(snapchain::network::gossip::Config::default())
            .expect("node gossip config serializes");
        let node_gossip = node_gossip.as_table().expect("gossip config is a table");
        for field in ["bootstrap_peers", "direct_peers"] {
            assert!(
                node_gossip.get(field).is_some_and(|v| v.is_str()),
                "node gossip config no longer has string field {field:?} — update the \
                 OnchainGossip mirror and the registry renderer"
            );
        }
    }
}
