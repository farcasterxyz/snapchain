//! Parsing of the CAIP-19 asset identifier that names a channel.
//!
//! A channel is an ERC-721 token in the Farcaster channel registrar: the
//! `channel_id` used everywhere else in the channel stores is the 32-byte
//! keccak256 registry label, which is also the token's `tokenId`. That makes a
//! channel nameable by a [CAIP-19] asset ID:
//!
//! ```text
//! eip155:11155111/erc721:0x7Dd80C661dED9bFC8D4440224A0b39b345a91BE4/186488426504904464…
//! ^ chain         ^ asset namespace + registrar contract           ^ tokenId
//! ```
//!
//! Reacting to that string is how a follow is expressed — see
//! [`channel_id_for_follow_target`]. The contract and chain in the URI are
//! checked against the registrar the network is configured for; a URI naming
//! some other ERC-721 collection is a perfectly valid reaction target, it just
//! isn't a channel follow.
//!
//! # Why the parser is strict
//!
//! `ReactionStoreDef::make_target_key` keys the by-target index on the raw URL
//! bytes, so two spellings of the same channel are two independent reactions.
//! If both spellings mapped to one follow, removing either would leave the
//! follow index disagreeing with the reaction set — and disagreeing
//! *differently* depending on whether a node replayed history or observed it
//! live, which is exactly the kind of divergence a replicated index cannot
//! have.
//!
//! So exactly one spelling per channel is accepted, and every other spelling is
//! simply not a follow:
//!
//! * namespaces are lowercase `eip155` / `erc721`,
//! * the chain reference and the tokenId are decimal with no leading zeros,
//! * the contract address is [EIP-55] checksummed.
//!
//! [`canonical_string`](ChannelAssetId::canonical_string) emits that form, and
//! `parse` accepts only what it emits — the round trip is pinned by test.
//!
//! [CAIP-19]: https://github.com/ChainAgnostic/CAIPs/blob/main/CAIPs/caip-19.md
//! [EIP-55]: https://eips.ethereum.org/EIPS/eip-55

use crate::proto::FarcasterNetwork;
use crate::storage::store::account::CHANNEL_ID_LENGTH;
use alloy_primitives::{address, Address, U256};
use thiserror::Error;

/// CAIP-2 namespace for EVM chains.
const EIP155_NAMESPACE: &str = "eip155";
/// CAIP-19 asset namespace for the channel registrar's token standard.
const ERC721_NAMESPACE: &str = "erc721";

/// Upper bound on an accepted URI, well above the longest canonical form: a
/// 20-digit chain reference, a 42-char checksummed address and a 78-digit
/// tokenId, plus the two namespaces and two separators, come to 156 bytes.
/// Reaction targets are already capped at 256 bytes by `validate_url`, so this
/// never fires on that path; it keeps the parser self-contained for other callers.
const MAX_URI_LENGTH: usize = 256;

/// CAIP-19 caps `token_id` at 78 characters, which is exactly the number of
/// decimal digits in `U256::MAX`.
const MAX_TOKEN_ID_DIGITS: usize = 78;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ChannelUriError {
    #[error("uri > {MAX_URI_LENGTH} bytes")]
    UriTooLong,
    #[error("uri is not a CAIP-19 asset id: expected <chain>/<asset>/<tokenId>")]
    MalformedAssetId,
    #[error("chain namespace is not `{EIP155_NAMESPACE}`")]
    UnsupportedChainNamespace,
    #[error("asset namespace is not `{ERC721_NAMESPACE}`")]
    UnsupportedAssetNamespace,
    #[error("chain reference is not a canonical decimal chain id")]
    InvalidChainReference,
    #[error("contract address is not an EIP-55 checksummed address")]
    InvalidContractAddress,
    #[error("tokenId is not a canonical decimal uint256")]
    InvalidTokenId,
}

/// The registrar contract a `channel_id` must have been minted by for a
/// reaction against it to count as a follow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChannelRegistrar {
    pub chain_id: u64,
    pub address: Address,
}

/// Ethereum Sepolia, where the channel registry currently lives. Mainnet will be
/// Ethereum L1 (chain 1), rooted under `farcasterchannels.eth` — not yet
/// deployed, so its chain id and address arrive together as a separate arm.
const SEPOLIA_CHAIN_ID: u64 = 11155111;

/// The channel registry's ERC-721, deployed to Sepolia on 2026-07-08 rooted under
/// `topochtest.eth` (`channel-registry/deployments/11155111.md`). This is a
/// development deployment — the registry is still being iterated on, so treat the
/// address as provisional.
///
/// This is the `BaseRegistrar`, NOT the `RegistrarController` at `0x90fc4218…`.
/// A CAIP-19 `erc721:<address>/<tokenId>` names a token in a collection, and the
/// token whose id is `keccak256(channel_key)` is minted by the BaseRegistrar —
/// `ownerOf(cast keccak "$CHANNEL")` is called against this address. The
/// controller only emits the `NameRegistered`/`NameRenewed` events the connector
/// ingests, and owns no tokens.
const SEPOLIA_CHANNEL_REGISTRAR: Address = address!("0x7dd80c661ded9bfc8d4440224a0b39b345a91be4");

/// The channel registrar whose tokens are followable on `network`, or `None`
/// where no registrar is deployed yet.
///
/// CONSENSUS CONSTANT. Every node on `network` must resolve this identically. It
/// decides which reactions enter the follow index, and that index lives outside
/// the merkle trie (`TrieKey::for_hub_event` derives nothing from it), so a node
/// that answers differently still publishes a matching state root and diverges
/// silently — no error, no mismatch, just a different answer to
/// `GetChannelFollowers`.
///
/// For that reason this MUST NOT be derived from
/// `onchain_events::Config::override_channel_registrar_address`. That field is
/// per-node operator config choosing which contract *this node's* connector
/// watches; reading it here would make replicated derived state depend on a
/// node's YAML. The addresses here are properties of the network, so nodes agree
/// with no configuration at all.
///
/// Mainnet is `None` until the production registry deploys, which short-circuits
/// the whole follow path before any parsing or DB access. See the sequencing rule
/// on `ProtocolFeature::ChannelFollows`: a mainnet activation timestamp may not be
/// scheduled while this is still `None`.
pub fn channel_registrar_for_network(network: FarcasterNetwork) -> Option<ChannelRegistrar> {
    match network {
        // TODO: the mainnet registry (`farcasterchannels.eth` on Ethereum L1) is
        // not deployed. Its chain id and address land in the same change that
        // schedules V20 on mainnet — see the sequencing rule above.
        FarcasterNetwork::Mainnet => None,
        // An unspecified network gets no registrar. Falling through to the
        // development arm would index follows against Sepolia on a node that
        // never said which network it is on, which is exactly the kind of
        // implicit answer a consensus constant must not give.
        FarcasterNetwork::None => None,
        // PROVISIONAL. Testnet's own deployment is not finalized either; it is
        // expected to be the same contract devnet uses, so both point at the
        // Sepolia development registry for now. Revisit when testnet is pinned —
        // changing this after V20 activates on testnet would rewrite which
        // reactions count as follows without a version boundary.
        _ => Some(ChannelRegistrar {
            chain_id: SEPOLIA_CHAIN_ID,
            address: SEPOLIA_CHANNEL_REGISTRAR,
        }),
    }
}

/// A parsed, canonical CAIP-19 asset ID naming one ERC-721 token.
///
/// Parsing does not check the token against any registrar — it only guarantees
/// the URI is well formed and canonically spelled. Use
/// [`channel_id_for_follow_target`] to get from a reaction target to a channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChannelAssetId {
    pub chain_id: u64,
    pub contract: Address,
    /// The tokenId as 32 big-endian bytes — identical to the `channel_id` the
    /// channel stores are keyed by.
    pub channel_id: [u8; CHANNEL_ID_LENGTH],
}

impl ChannelAssetId {
    /// Parses the one accepted spelling. See the module docs for why anything
    /// else is rejected rather than normalized.
    pub fn parse(uri: &str) -> Result<Self, ChannelUriError> {
        if uri.len() > MAX_URI_LENGTH {
            return Err(ChannelUriError::UriTooLong);
        }

        // Exactly two separators: `<chain_id>/<asset_type>/<token_id>`. splitn
        // would silently accept a third slash inside the tokenId.
        let mut segments = uri.split('/');
        let (chain, asset, token_id) = match (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ) {
            (Some(chain), Some(asset), Some(token_id), None) => (chain, asset, token_id),
            _ => return Err(ChannelUriError::MalformedAssetId),
        };

        let chain_reference = chain
            .strip_prefix(EIP155_NAMESPACE)
            .and_then(|rest| rest.strip_prefix(':'))
            .ok_or(ChannelUriError::UnsupportedChainNamespace)?;
        let chain_id: u64 = chain_reference
            .parse()
            .map_err(|_| ChannelUriError::InvalidChainReference)?;
        // Rejects `eip155:08453` and `eip155:+8453`, both of which `parse` takes
        // for some inputs and neither of which round-trips.
        if chain_id.to_string() != chain_reference {
            return Err(ChannelUriError::InvalidChainReference);
        }

        let address = asset
            .strip_prefix(ERC721_NAMESPACE)
            .and_then(|rest| rest.strip_prefix(':'))
            .ok_or(ChannelUriError::UnsupportedAssetNamespace)?;
        // `parse_checksummed` rejects an all-lowercase address, which is what
        // pins the address to a single spelling.
        let contract = Address::parse_checksummed(address, None)
            .map_err(|_| ChannelUriError::InvalidContractAddress)?;

        if token_id.len() > MAX_TOKEN_ID_DIGITS {
            return Err(ChannelUriError::InvalidTokenId);
        }
        let token =
            U256::from_str_radix(token_id, 10).map_err(|_| ChannelUriError::InvalidTokenId)?;
        // Same canonicality argument as the chain id: no leading zeros, no sign.
        if token.to_string() != token_id {
            return Err(ChannelUriError::InvalidTokenId);
        }

        Ok(Self {
            chain_id,
            contract,
            channel_id: token.to_be_bytes(),
        })
    }

    /// The single spelling [`parse`](Self::parse) accepts.
    pub fn canonical_string(&self) -> String {
        format!(
            "{EIP155_NAMESPACE}:{}/{ERC721_NAMESPACE}:{}/{}",
            self.chain_id,
            self.contract.to_checksum(None),
            U256::from_be_bytes(self.channel_id),
        )
    }

    /// Builds the asset ID naming `channel_id` in `registrar`.
    pub fn for_channel(registrar: &ChannelRegistrar, channel_id: [u8; CHANNEL_ID_LENGTH]) -> Self {
        Self {
            chain_id: registrar.chain_id,
            contract: registrar.address,
            channel_id,
        }
    }

    /// Whether this asset ID names a token in `registrar`.
    pub fn is_in(&self, registrar: &ChannelRegistrar) -> bool {
        self.chain_id == registrar.chain_id && self.contract == registrar.address
    }
}

/// Classifies a reaction's `target_url` as a channel follow.
///
/// Returns the followed `channel_id`, or `None` when the target is anything
/// else — a web URL, a non-canonical spelling, or a CAIP-19 asset ID for some
/// other collection. `None` is not an error: the reaction is still a valid
/// reaction, it just has no channel meaning.
pub fn channel_id_for_follow_target(
    target_url: &str,
    registrar: &ChannelRegistrar,
) -> Option<[u8; CHANNEL_ID_LENGTH]> {
    let asset = ChannelAssetId::parse(target_url).ok()?;
    asset.is_in(registrar).then_some(asset.channel_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::util::FarcasterTime;
    use crate::version::version::{EngineVersion, ProtocolFeature};

    /// The CAIP-19 spec's own example, so the accepted grammar is anchored to
    /// something outside this repo.
    const CRYPTOKITTIES: &str = "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769";

    fn registrar() -> ChannelRegistrar {
        ChannelRegistrar {
            chain_id: 8453,
            address: address!("0x06012c8cf97BEaD5deAe237070F9587f8E7A266d"),
        }
    }

    #[test]
    fn parses_the_caip19_spec_example() {
        let asset = ChannelAssetId::parse(CRYPTOKITTIES).unwrap();
        assert_eq!(asset.chain_id, 1);
        assert_eq!(
            asset.contract,
            address!("0x06012c8cf97BEaD5deAe237070F9587f8E7A266d")
        );
        assert_eq!(U256::from_be_bytes(asset.channel_id), U256::from(771769));
    }

    #[test]
    fn canonical_string_round_trips() {
        for uri in [
            CRYPTOKITTIES,
            "eip155:8453/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/0",
            // A full-width tokenId: the keccak label of a channel is a random
            // 256-bit value, so the 78-digit case is the common one, not an edge.
            "eip155:8453/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/1157920892373161954235709850086879078532699846656405640394575840079131296399\
35",
        ] {
            assert_eq!(ChannelAssetId::parse(uri).unwrap().canonical_string(), uri);
        }
    }

    #[test]
    fn every_channel_id_round_trips_through_the_uri() {
        // The channel_id is a keccak hash, so leading and trailing zero bytes
        // both occur; big-endian decimal must survive either.
        for channel_id in [[0u8; 32], [0xff; 32], {
            let mut id = [0u8; 32];
            id[31] = 1;
            id
        }] {
            let asset = ChannelAssetId::for_channel(&registrar(), channel_id);
            let reparsed = ChannelAssetId::parse(&asset.canonical_string()).unwrap();
            assert_eq!(reparsed, asset);
            assert_eq!(reparsed.channel_id, channel_id);
        }
    }

    #[test]
    fn rejects_non_canonical_spellings_of_an_accepted_channel() {
        // Each of these names the same token as CRYPTOKITTIES. Accepting any of
        // them would let one channel be followed under two reaction keys.
        let cases = [
            (
                "eip155:1/erc721:0x06012c8cf97bead5deae237070f9587f8e7a266d/771769",
                ChannelUriError::InvalidContractAddress,
            ),
            (
                "eip155:1/erc721:0x06012C8CF97BEAD5DEAE237070F9587F8E7A266D/771769",
                ChannelUriError::InvalidContractAddress,
            ),
            (
                "eip155:01/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769",
                ChannelUriError::InvalidChainReference,
            ),
            (
                "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/0771769",
                ChannelUriError::InvalidTokenId,
            ),
            (
                "EIP155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769",
                ChannelUriError::UnsupportedChainNamespace,
            ),
            (
                "eip155:1/ERC721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769",
                ChannelUriError::UnsupportedAssetNamespace,
            ),
        ];
        for (uri, expected) in cases {
            assert_eq!(ChannelAssetId::parse(uri).unwrap_err(), expected, "{uri}");
        }
    }

    #[test]
    fn rejects_malformed_uris() {
        let cases = [
            ("", ChannelUriError::MalformedAssetId),
            ("https://example.com/channel", ChannelUriError::MalformedAssetId),
            // Collection-level asset *type*, with no tokenId: names the whole
            // registrar rather than one channel.
            (
                "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d",
                ChannelUriError::MalformedAssetId,
            ),
            // A trailing segment must not be swallowed into the tokenId.
            (
                "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769/extra",
                ChannelUriError::MalformedAssetId,
            ),
            (
                "eip155:1/erc20:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/771769",
                ChannelUriError::UnsupportedAssetNamespace,
            ),
            (
                "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/1",
                ChannelUriError::UnsupportedChainNamespace,
            ),
            // 2^256, one past the representable range.
            (
                "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/115792089237316195423570985008687907853269984665640564039457584007913129639936",
                ChannelUriError::InvalidTokenId,
            ),
            (
                "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/-1",
                ChannelUriError::InvalidTokenId,
            ),
            (
                "eip155:1/erc721:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d/0x1",
                ChannelUriError::InvalidTokenId,
            ),
        ];
        for (uri, expected) in cases {
            assert_eq!(ChannelAssetId::parse(uri).unwrap_err(), expected, "{uri}");
        }
    }

    #[test]
    fn rejects_an_over_long_uri_before_parsing() {
        let uri = "e".repeat(MAX_URI_LENGTH + 1);
        assert_eq!(
            ChannelAssetId::parse(&uri).unwrap_err(),
            ChannelUriError::UriTooLong
        );
    }

    #[test]
    fn follow_target_matches_only_the_configured_registrar() {
        let registrar = registrar();
        let channel_id = [7u8; 32];
        let uri = ChannelAssetId::for_channel(&registrar, channel_id).canonical_string();
        assert_eq!(
            channel_id_for_follow_target(&uri, &registrar),
            Some(channel_id)
        );

        // Right contract, wrong chain.
        let other_chain = ChannelRegistrar {
            chain_id: 1,
            ..registrar
        };
        assert_eq!(channel_id_for_follow_target(&uri, &other_chain), None);

        // Right chain, wrong contract.
        let other_contract = ChannelRegistrar {
            address: address!("0x00000000Fc6c5F01Fc30151999387Bb99A9f489b"),
            ..registrar
        };
        assert_eq!(channel_id_for_follow_target(&uri, &other_contract), None);
    }

    #[test]
    fn the_sepolia_networks_share_a_registrar_and_mainnet_has_none_yet() {
        // Testnet and devnet must agree: the testnet acceptance run registers
        // channels against the same Sepolia contract devnet tests target.
        let devnet = channel_registrar_for_network(FarcasterNetwork::Devnet)
            .expect("devnet must have a registrar so tests and devnet nodes agree");
        assert_eq!(devnet.chain_id, SEPOLIA_CHAIN_ID);
        assert_eq!(devnet.address, SEPOLIA_CHANNEL_REGISTRAR);
        assert_eq!(
            channel_registrar_for_network(FarcasterNetwork::Testnet),
            Some(devnet)
        );

        assert_eq!(
            channel_registrar_for_network(FarcasterNetwork::Mainnet),
            None
        );
    }

    #[test]
    fn a_real_sepolia_channel_token_produces_the_expected_uri() {
        // Anchored to the deployment doc's worked example: the tokenId is
        // keccak256("myfirstchannel"), which is exactly the channel_id the stores
        // are keyed by. If the constant or the decimal encoding drifts, this URI
        // stops matching what a client following that channel would send.
        //
        // Both halves of the literal were derived OUTSIDE this code, so the
        // assertion is not circular:
        //   cast keccak "myfirstchannel"
        //     -> 0x293adef29fbf2b91f4f7d3c7cad0e16cd2371d3fe9e60ec69fe670660fbaa29d
        //   int(that, 16)
        //     -> 18648842650490446402472697417113953300360049939147561484912590753231139414685
        //   cast to-check-sum-address 0x7dd80c661ded9bfc8d4440224a0b39b345a91be4
        //     -> 0x7Dd80C661dED9bFC8D4440224A0b39b345a91BE4
        let registrar = channel_registrar_for_network(FarcasterNetwork::Devnet).unwrap();
        let channel_id = alloy_primitives::keccak256(b"myfirstchannel").0;
        let uri = ChannelAssetId::for_channel(&registrar, channel_id).canonical_string();
        assert_eq!(
            uri,
            "eip155:11155111/erc721:0x7Dd80C661dED9bFC8D4440224A0b39b345a91BE4/\
             18648842650490446402472697417113953300360049939147561484912590753231139414685"
        );
        assert_eq!(
            channel_id_for_follow_target(&uri, &registrar),
            Some(channel_id)
        );
    }

    #[test]
    fn the_registrar_round_trips_through_a_canonical_uri() {
        // Pins that the deployed registrar survives for_channel -> canonical_string
        // -> parse -> is_in. Note the constant's OWN casing is irrelevant:
        // `address!` stores 20 raw bytes and `to_checksum` re-derives EIP-55, so a
        // mis-cased literal cannot fail this — an earlier version of this comment
        // claimed otherwise.
        let registrar = channel_registrar_for_network(FarcasterNetwork::Devnet).unwrap();
        let channel_id = [0xabu8; 32];
        let uri = ChannelAssetId::for_channel(&registrar, channel_id).canonical_string();
        assert_eq!(
            channel_id_for_follow_target(&uri, &registrar),
            Some(channel_id)
        );
    }

    #[test]
    fn channel_follows_requires_a_registrar_wherever_it_is_scheduled() {
        // SEQUENCING RULE. Flipping `channel_registrar_for_network` from `None` to
        // `Some` after V20 has already activated would change which reactions are
        // indexed without a version boundary — an unversioned change to replicated
        // derived state. So the constant and the schedule entry have to land
        // together, and this fails the moment someone schedules one without the
        // other.
        let far_future = FarcasterTime::from_unix_seconds(4102444800); // 2100-01-01 UTC
        for network in [
            FarcasterNetwork::Mainnet,
            FarcasterNetwork::Testnet,
            FarcasterNetwork::Devnet,
        ] {
            if EngineVersion::version_for(&far_future, network)
                .is_enabled(ProtocolFeature::ChannelFollows)
            {
                assert!(
                    channel_registrar_for_network(network).is_some(),
                    "{network:?} schedules ChannelFollows but has no registrar constant"
                );
            }
        }
    }

    #[test]
    fn ordinary_reaction_targets_are_not_follows() {
        // The overwhelmingly common case: a reaction target that has nothing to
        // do with channels must classify as "not a follow" rather than error out
        // anywhere upstream.
        for target in ["https://example.com", "", "farcaster://cast/0xdeadbeef"] {
            assert_eq!(channel_id_for_follow_target(target, &registrar()), None);
        }
    }
}
