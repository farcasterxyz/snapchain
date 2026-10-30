//! Minimal ENS name-resolution utilities.
//!
//! Adapted from `foundry_common::ens`
//! (<https://github.com/foundry-rs/foundry>, MIT OR Apache-2.0), trimmed to only the
//! surface used by `resolve_ens_name`: the ENS Registry/Resolver `sol!` bindings, the
//! EIP-137 `namehash`, and the `EnsError` type. Inlined here so we no longer depend on the
//! `foundry-common` git pin, which freezes the alloy dependency tree.

use alloy_primitives::{Keccak256, B256};
use alloy_sol_types::sol;
use std::borrow::Cow;

// ENS Registry and Resolver contracts.
sol! {
    /// ENS Registry contract.
    #[allow(missing_docs)]
    #[sol(rpc)]
    contract EnsRegistry {
        /// Returns the resolver for the specified node.
        function resolver(bytes32 node) view returns (address);
        /// Returns the owner of the specified node.
        ///
        /// Used to derive the channel registry's ERC-721 from its controller: the
        /// deploy hands the root 2LD to the BaseRegistrar, so the owner of the
        /// controller's `rootNode` IS that registrar. See
        /// `Subscriber::resolve_channel_registrar_token`.
        function owner(bytes32 node) view returns (address);
    }

    /// ENS Resolver interface.
    #[allow(missing_docs)]
    #[sol(rpc)]
    contract EnsResolver {
        /// Returns the address associated with the specified node.
        function addr(bytes32 node) view returns (address);
    }
}

/// Error type for ENS resolution.
#[derive(Debug, thiserror::Error)]
pub enum EnsError {
    /// Failed to get resolver from the ENS registry.
    #[error("Failed to get resolver from the ENS registry: {0}")]
    Resolver(alloy_contract::Error),
    /// ENS resolver not found for the given name.
    #[error("ENS resolver not found for name {0:?}")]
    ResolverNotFound(String),
    /// Failed to resolve ENS name to an address.
    #[error("Failed to resolve ENS name to an address: {0}")]
    Resolve(alloy_contract::Error),
}

/// Returns the ENS namehash as specified in
/// [EIP-137](https://eips.ethereum.org/EIPS/eip-137).
pub fn namehash(name: &str) -> B256 {
    if name.is_empty() {
        return B256::ZERO;
    }

    // Remove the variation selector `U+FE0F` if present.
    const VARIATION_SELECTOR: char = '\u{fe0f}';
    let name = if name.contains(VARIATION_SELECTOR) {
        Cow::Owned(name.replace(VARIATION_SELECTOR, ""))
    } else {
        Cow::Borrowed(name)
    };

    // Generate the node starting from the right.
    // This buffer is `[node @ [u8; 32], label_hash @ [u8; 32]]`.
    let mut buffer = [0u8; 64];
    for label in name.rsplit('.') {
        // node = keccak256([node, keccak256(label)])

        // Hash the label.
        let mut label_hasher = Keccak256::new();
        label_hasher.update(label.as_bytes());
        label_hasher.finalize_into(&mut buffer[32..]);

        // Hash both the node and the label hash, writing into the node.
        let mut buffer_hasher = Keccak256::new();
        buffer_hasher.update(buffer.as_slice());
        buffer_hasher.finalize_into(&mut buffer[..32]);
    }
    buffer[..32].try_into().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::hex;

    fn assert_hex(hash: B256, val: &str) {
        assert_eq!(hash.0[..], hex::decode(val).unwrap()[..]);
    }

    #[test]
    fn test_namehash() {
        for (name, expected) in &[
            (
                "",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
            ),
            (
                "eth",
                "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae",
            ),
            (
                "foo.eth",
                "0xde9b09fd7c5f901e23a3f19fecc54828e9c848539801e86591bd9801b019f84f",
            ),
            (
                "alice.eth",
                "0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec",
            ),
            (
                "ret↩️rn.eth",
                "0x3de5f4c02db61b221e7de7f1c40e29b6e2f07eb48d65bf7e304715cd9ed33b24",
            ),
        ] {
            assert_hex(namehash(name), expected);
        }
    }
}
