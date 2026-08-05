//! `fc config pull` — fetch the onchain-managed node configuration from the
//! SnapchainConfigRegistry and merge it into a local `config.toml`.
//!
//! The registry renders a TOML fragment whose byte-level format is specified in
//! `farcasterxyz/contracts` (`docs/snapchain-config-registry.md`). This module is
//! the consumer side: it replaces exactly three keys in the local file —
//! `consensus.validator_sets`, `gossip.bootstrap_peers`, `gossip.direct_peers` —
//! and leaves everything else, `consensus.private_key` above all, untouched.
//!
//! Validation here is structural and covers only the three managed keys.
//! Full-config validation belongs to the node binary (`snapchain --check-config`),
//! which owns the real loader; the deploy scripts run it between pulling and
//! restarting the node.

use std::path::PathBuf;

use alloy_primitives::Address;
use alloy_sol_types::{sol, SolCall};
use serde::Deserialize;

use crate::{BoxedError, NetworkArg};

sol! {
    /// The one read `config pull` needs from ISnapchainConfigRegistry
    /// (farcasterxyz/contracts).
    function configToml() external view returns (string memory);
}

/// Registry address baked in per network. Mainnet and testnet registries both
/// live on Ethereum L1 mainnet, distinguished by address; devnet deployments are
/// ephemeral and always need `--registry`.
fn baked_in_registry(_network: NetworkArg) -> Option<Address> {
    // NEYN-13022: fill in the mainnet and testnet addresses once the registry
    // deploy lands. Devnet stays `None` permanently.
    None
}

#[derive(clap::Args)]
pub struct ConfigPullArgs {
    /// SnapchainConfigRegistry contract address. Overrides the baked-in
    /// per-network address; required for devnet.
    #[arg(long)]
    registry: Option<String>,

    /// Ethereum L1 JSON-RPC URL. Defaults to `l1_rpc_url` from the target
    /// config file.
    #[arg(long)]
    rpc_url: Option<String>,

    /// Path to the node config.toml to merge into.
    #[arg(long)]
    config: PathBuf,

    /// Print the merged document to stdout instead of writing the file.
    #[arg(long)]
    dry_run: bool,
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
    let local_raw = std::fs::read_to_string(&args.config)
        .map_err(|e| format!("cannot read {}: {e}", args.config.display()))?;
    let mut local: toml::Table = toml::from_str(&local_raw)
        .map_err(|e| format!("cannot parse {}: {e}", args.config.display()))?;

    // The registry manages validators only; read nodes keep their own peer
    // configuration path (spec §2.5). A dry run is harmless inspection.
    let is_read_node = local
        .get("read_node")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if is_read_node {
        if !args.dry_run {
            return Err(format!(
                "{} sets read_node = true; the registry manages validator config only \
                 (use --dry-run to inspect the merge anyway)",
                args.config.display()
            )
            .into());
        }
        eprintln!("warning: read_node = true — the registry manages validator config only");
    }

    let registry = resolve_registry(args.registry.as_deref(), network)?;
    let rpc_url = resolve_rpc_url(args.rpc_url, &local)?;

    eprintln!("Fetching configToml() from {registry} via {rpc_url}");
    let rendered = fetch_config_toml(&rpc_url, registry).await?;

    let onchain: toml::Table =
        toml::from_str(&rendered).map_err(|e| format!("registry returned invalid TOML: {e}"))?;
    validate_onchain(&onchain)?;

    splice_managed_keys(&mut local, &onchain)?;

    let merged =
        toml::to_string(&local).map_err(|e| format!("cannot serialize merged config: {e}"))?;
    // Belt and braces: never emit a document the parser itself would reject.
    toml::from_str::<toml::Table>(&merged)
        .map_err(|e| format!("merged config does not re-parse: {e}"))?;

    if args.dry_run {
        print!("{merged}");
        return Ok(());
    }

    // Write via rename so a crash mid-write cannot leave a truncated config for
    // the next node start to trip over.
    let tmp = args.config.with_extension("toml.tmp");
    std::fs::write(&tmp, &merged).map_err(|e| format!("cannot write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, &args.config)
        .map_err(|e| format!("cannot rename {} into place: {e}", tmp.display()))?;
    eprintln!("Wrote {}", args.config.display());
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

fn resolve_rpc_url(flag: Option<String>, local: &toml::Table) -> Result<String, BoxedError> {
    if let Some(url) = flag {
        return Ok(url);
    }
    match local.get("l1_rpc_url").and_then(|v| v.as_str()) {
        Some(url) if !url.is_empty() => Ok(url.to_string()),
        _ => Err("no --rpc-url given and the config file sets no l1_rpc_url".into()),
    }
}

async fn fetch_config_toml(rpc_url: &str, registry: Address) -> Result<String, BoxedError> {
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
    let response: serde_json::Value = reqwest::Client::new()
        .post(rpc_url)
        .json(&request)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if let Some(error) = response.get("error") {
        return Err(format!("eth_call failed: {error}").into());
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

    for (i, set) in sets.iter().enumerate() {
        if set.shard_ids.is_empty() {
            return Err(format!("validator set {i}: empty shard_ids").into());
        }
        if set.validator_public_keys.is_empty() {
            return Err(format!("validator set {i}: empty validator_public_keys").into());
        }
        for key in &set.validator_public_keys {
            // The node does hex::decode(key).unwrap() at startup
            // (src/consensus/validator.rs), so a malformed key must die here.
            match hex::decode(key) {
                Ok(bytes) if bytes.len() == 32 => {}
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
        if i > 0 && set.effective_at < sets[i - 1].effective_at {
            return Err(format!(
                "validator set {i}: effective_at {} is below its predecessor's {}",
                set.effective_at,
                sets[i - 1].effective_at
            )
            .into());
        }
    }

    // Entry 0 seeds the node's active-set scan unconditionally, so it must be
    // the genesis entry (spec §2.3). Operator-owned data, so warn rather than
    // refuse.
    if sets[0].effective_at != 0 {
        eprintln!(
            "warning: first validator set has effective_at = {} (expected genesis entry at 0)",
            sets[0].effective_at
        );
    }

    Ok(())
}

/// Replace the three managed keys in `local` with the values from `onchain`,
/// splicing the parsed values directly so the write-back carries exactly what
/// the registry rendered. An empty peer string is a value, not "unset" (spec
/// §2.4) — it replaces like any other.
fn splice_managed_keys(local: &mut toml::Table, onchain: &toml::Table) -> Result<(), BoxedError> {
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

    /// Validate + splice + serialize + re-parse, the same path `run` takes.
    fn merge(local: &str) -> toml::Table {
        let mut local: toml::Table = toml::from_str(local).unwrap();
        let onchain = onchain_table();
        validate_onchain(&onchain).unwrap();
        splice_managed_keys(&mut local, &onchain).unwrap();
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
        splice_managed_keys(&mut local, &onchain_empty_peers).unwrap();
        assert_eq!(local["gossip"]["direct_peers"].as_str(), Some(""));
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
        assert!(err.contains("below its predecessor"), "got: {err}");
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
}
