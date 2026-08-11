//! `fc config slot` — this validator's deterministic restart-stagger slot.
//!
//! Prints `<index> <count>` on stdout, where `index` is the position of this
//! node's consensus public key in the SORTED, de-duplicated union of
//! `validator_public_keys` across the config's `consensus.validator_sets`,
//! and `count` is the size of that union. Sorting makes slots a function of
//! membership alone — a write that merely reorders keys cannot move anyone's
//! window (see slot_in_sets); a write that changes membership still can, so
//! the operator rule below stands. A node whose key is not in the
//! document gets the sentinel `index == count`. The sentinel is SHARED: if
//! one registry write removes several still-active validators at once, they
//! all land in the same window and can restart together — operators should
//! remove validators one write at a time. (In practice removed keys usually
//! persist in older history entries and keep a real slot; the sentinel bites
//! on wholesale history rewrites and fleet bootstrap.)
//!
//! The union deliberately spans ALL sets, not just the latest: a validator
//! present only in older entries is still running and restartable, and every
//! node must derive identical slots from the identical document. The cost is
//! that retired-but-retained keys each add one idle window to the cycle.
//!
//! The rollout watcher (scripts/onchain-config-watch.sh) turns the pair into
//! a wall-clock restart window: node `i` of `n` may only restart during
//! `[i*S, (i+1)*S)` seconds within a repeating cycle of `(n+1)*S` (the +1 is
//! the sentinel slot). Because every node computes its index from the same
//! freshly fetched document, the slots are distinct by construction — a
//! guarantee neither `$RANDOM` nor hash-mod-N schemes can make, which is why
//! this exists as a subcommand instead of shell arithmetic.
//!
//! Purely local: reads the config file, derives the public key from
//! `consensus.private_key`, and never touches the network. Key material is
//! never printed; errors go through the same sanitizer as `config pull`.

use std::path::PathBuf;

use crate::config_pull::{sanitized_toml_error, ValidatorSetConfig};
use crate::BoxedError;

#[derive(clap::Args)]
pub struct ConfigSlotArgs {
    /// Node config.toml supplying consensus.private_key and
    /// consensus.validator_sets. Read-only.
    #[arg(long)]
    config: PathBuf,
}

pub fn run(args: ConfigSlotArgs) -> Result<(), BoxedError> {
    let raw = std::fs::read_to_string(&args.config)
        .map_err(|e| format!("cannot read {}: {e}", args.config.display()))?;
    let local: toml::Table = toml::from_str(&raw)
        .map_err(|e| sanitized_toml_error(&args.config.display().to_string(), &raw, &e))?;

    let consensus = local
        .get("consensus")
        .and_then(|c| c.as_table())
        .ok_or("config has no [consensus] table")?;

    let env_key = std::env::var("SNAPCHAIN_CONSENSUS__PRIVATE_KEY").ok();
    let private_key = effective_private_key(consensus, env_key.as_deref())?;
    let public_key_hex = derive_public_key_hex(private_key)?;

    let sets: Vec<ValidatorSetConfig> = consensus
        .get("validator_sets")
        .cloned()
        .ok_or("config has no consensus.validator_sets — pull before computing a slot")?
        .try_into()
        .map_err(|e| format!("consensus.validator_sets has unexpected shape: {e}"))?;

    let (index, count) = slot_in_sets(&public_key_hex, &sets)?;
    println!("{index} {count}");
    Ok(())
}

/// The private key this config will actually run with. The node's figment
/// loader overlays `SNAPCHAIN_`-prefixed env vars (split on `__`) over the
/// file (src/cfg.rs), so `SNAPCHAIN_CONSENSUS__PRIVATE_KEY` — when set — is
/// the node's real identity and must drive the slot; a file-only read would
/// silently park such a node in the shared sentinel window.
fn effective_private_key<'a>(
    consensus: &'a toml::Table,
    env_key: Option<&'a str>,
) -> Result<&'a str, BoxedError> {
    if let Some(key) = env_key {
        return Ok(key);
    }
    consensus
        .get("private_key")
        .and_then(|k| k.as_str())
        .ok_or_else(|| {
            "config has no consensus.private_key (and SNAPCHAIN_CONSENSUS__PRIVATE_KEY is unset)"
                .into()
        })
}

/// Derive the 32-byte Ed25519 public key from the config's private key and
/// return it as lowercase hex. The node treats `consensus.private_key` as a
/// 32-byte seed (`Config::keypair` in src/consensus/consensus.rs, via
/// libp2p's `SecretKey::try_from_bytes`); RFC 8032 fixes the seed-to-public
/// derivation, so ed25519-dalek here and libp2p in the node must agree — the
/// RFC test vector below pins that assumption.
fn derive_public_key_hex(private_key: &str) -> Result<String, BoxedError> {
    // Never echo the offending value: it is the consensus signing key (or a
    // typo'd attempt at one).
    let bytes = hex::decode(private_key.trim())
        .map_err(|_| "consensus.private_key is not valid hex (value not shown)")?;
    let seed: [u8; 32] = bytes.try_into().map_err(|v: Vec<u8>| {
        format!(
            "consensus.private_key decodes to {} bytes, expected 32 (value not shown)",
            v.len()
        )
    })?;
    let verifying = ed25519_dalek::SigningKey::from_bytes(&seed).verifying_key();
    Ok(hex::encode(verifying.to_bytes()))
}

/// Index of `public_key_hex` in the SORTED union of the sets' keys, plus the
/// union's size. Sorted, not document order: a registry amend can reorder
/// keys within a set without changing membership, and with document-order
/// slots such a write landing while one validator restarts could hand its
/// window to a different node mid-flight (two validators down at once).
/// Sorting makes the slot a function of membership alone — only writes that
/// actually add or remove keys can shift anyone's slot. Case-insensitive:
/// the registry renders lowercase hex, but hand-maintained files may not.
fn slot_in_sets(
    public_key_hex: &str,
    sets: &[ValidatorSetConfig],
) -> Result<(usize, usize), BoxedError> {
    let union: std::collections::BTreeSet<String> = sets
        .iter()
        .flat_map(|set| set.validator_public_keys.iter())
        .map(|key| key.to_lowercase())
        .collect();
    if union.is_empty() {
        return Err("consensus.validator_sets lists no validator keys".into());
    }
    let index = union
        .iter()
        .position(|k| k == public_key_hex)
        .unwrap_or(union.len());
    Ok((index, union.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 8032 §7.1 test vector 1: pins that ed25519-dalek's seed-to-public
    /// derivation matches the spec — and therefore matches libp2p's, which the
    /// node uses on the same `consensus.private_key` bytes.
    const RFC_SEED: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
    const RFC_PUBLIC: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";

    fn set(keys: &[&str]) -> ValidatorSetConfig {
        toml::from_str(&format!(
            "effective_at = 0\nshard_ids = [0]\nvalidator_public_keys = [{}]",
            keys.iter()
                .map(|k| format!("{k:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .unwrap()
    }

    #[test]
    fn derivation_matches_rfc8032_vector() {
        assert_eq!(derive_public_key_hex(RFC_SEED).unwrap(), RFC_PUBLIC);
    }

    #[test]
    fn rejects_malformed_private_key_without_echoing_it() {
        for bad in ["zz61", "9d61b19d", ""] {
            let err = derive_public_key_hex(bad).unwrap_err().to_string();
            assert!(err.contains("not shown"), "got: {err}");
            assert!(!err.contains("9d61b19d"), "echoed key material: {err}");
        }
    }

    #[test]
    fn index_is_position_in_sorted_union() {
        let sets = [set(&["aa", RFC_PUBLIC, "bb"]), set(&["bb", "cc"])];
        // Sorted union: aa, bb, cc, d75a… — duplicates collapse, order is
        // lexicographic regardless of document order.
        assert_eq!(slot_in_sets(RFC_PUBLIC, &sets).unwrap(), (3, 4));
        assert_eq!(slot_in_sets("cc", &sets).unwrap(), (2, 4));
    }

    #[test]
    fn document_reordering_cannot_move_slots() {
        // A registry amend that only permutes keys must not reassign windows:
        // with document-order slots, a reorder landing mid-restart could hand
        // a down validator's window to a live one.
        let forward = [set(&["aa", RFC_PUBLIC, "bb"])];
        let shuffled = [set(&["bb", "aa", RFC_PUBLIC])];
        assert_eq!(
            slot_in_sets(RFC_PUBLIC, &forward).unwrap(),
            slot_in_sets(RFC_PUBLIC, &shuffled).unwrap(),
        );
        assert_eq!(
            slot_in_sets("aa", &forward).unwrap(),
            slot_in_sets("aa", &shuffled).unwrap(),
        );
    }

    #[test]
    fn unknown_key_gets_sentinel_trailing_slot() {
        let sets = [set(&["aa", "bb"])];
        // Not in the document: park it in the shared slot after every
        // member's (see the module doc for the sharing caveat).
        assert_eq!(slot_in_sets(RFC_PUBLIC, &sets).unwrap(), (2, 2));
    }

    #[test]
    fn env_overlay_wins_over_file_key() {
        let consensus: toml::Table =
            toml::from_str(&format!("private_key = \"{RFC_SEED}\"")).unwrap();
        let empty = toml::Table::new();

        // Env set: it is the node's real identity, in both directions.
        assert_eq!(effective_private_key(&consensus, Some("aa")).unwrap(), "aa");
        assert_eq!(effective_private_key(&empty, Some("aa")).unwrap(), "aa");
        // Env unset: the file decides; neither is an error naming both.
        assert_eq!(effective_private_key(&consensus, None).unwrap(), RFC_SEED);
        let err = effective_private_key(&empty, None).unwrap_err().to_string();
        assert!(
            err.contains("SNAPCHAIN_CONSENSUS__PRIVATE_KEY"),
            "got: {err}"
        );
    }

    #[test]
    fn key_comparison_is_case_insensitive() {
        let sets = [set(&[&RFC_PUBLIC.to_uppercase(), "bb"])];
        // Sorted after lowercasing: bb before d75a….
        assert_eq!(slot_in_sets(RFC_PUBLIC, &sets).unwrap(), (1, 2));
    }

    #[test]
    fn empty_sets_are_an_error() {
        let sets: [ValidatorSetConfig; 0] = [];
        assert!(slot_in_sets(RFC_PUBLIC, &sets).is_err());
    }
}
