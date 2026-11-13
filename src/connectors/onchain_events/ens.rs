//! Minimal ENS name-resolution utilities.
//!
//! Adapted from `foundry_common::ens`
//! (<https://github.com/foundry-rs/foundry>, MIT OR Apache-2.0), trimmed to only the
//! surface used by `resolve_ens_name`: the ENS Registry/Resolver `sol!` bindings, the
//! EIP-137 `namehash`, and the `EnsError` type. Inlined here so we no longer depend on the
//! `foundry-common` git pin, which freezes the alloy dependency tree. `resolve_name` adds
//! ENSIP-10 wildcard resolution on top.

use alloy_primitives::{Address, Keccak256, B256};
use alloy_provider::Provider;
use alloy_sol_types::{sol, SolCall};
use std::borrow::Cow;
use tracing::warn;

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

    /// ENSIP-10 extended resolver, which can answer for names below the node it is set on.
    #[allow(missing_docs)]
    #[sol(rpc)]
    contract ExtendedResolver {
        function supportsInterface(bytes4 interfaceID) view returns (bool);
        /// `name` is DNS wire-format encoded; `data` is an ABI-encoded resolver call.
        function resolve(bytes name, bytes data) view returns (bytes);
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
    /// The nearest resolver is set on an ancestor and does not implement ENSIP-10.
    #[error("ENS resolver for name {0:?} is on a parent and does not support wildcard resolution")]
    WildcardUnsupported(String),
    /// The name cannot be DNS-encoded.
    #[error("ENS name {0:?} is not valid")]
    InvalidName(String),
}

/// ENSIP-10 `IExtendedResolver` interface ID.
const EXTENDED_RESOLVER_INTERFACE_ID: [u8; 4] = [0x90, 0x61, 0xb9, 0x23];

/// Resolves `name` to an address through the ENS registry at `registry`.
///
/// Follows [ENSIP-10](https://docs.ens.domains/ensip/10): when `name` has no resolver
/// of its own, the nearest ancestor's resolver answers for it, and must implement
/// `resolve(bytes,bytes)`. ENSv2 relies on this: names registered on ENSv2 or migrated
/// to it have no resolver in the v1 registry and are served by the resolver on `eth`.
pub async fn resolve_name<P: Provider>(
    provider: &P,
    registry: Address,
    name: &str,
) -> Result<Address, EnsError> {
    let node = namehash(name);
    let (resolver, resolver_name) = find_resolver(provider, registry, name).await?;
    let result = if resolver_name == name {
        EnsResolver::new(resolver, provider)
            .addr(node)
            .call()
            .await
            .map_err(EnsError::Resolve)
    } else {
        resolve_wildcard(provider, resolver, name, node).await
    };
    result.inspect_err(|e| warn!("Failed to resolve ens name {name}: {e}"))
}

/// Returns the resolver set on `name` or its nearest ancestor, with the name it is set on.
async fn find_resolver<'a, P: Provider>(
    provider: &P,
    registry: Address,
    name: &'a str,
) -> Result<(Address, &'a str), EnsError> {
    let registry = EnsRegistry::new(registry, provider);
    let mut current = name;
    loop {
        let resolver = registry
            .resolver(namehash(current))
            .call()
            .await
            .map_err(EnsError::Resolver)?;
        if resolver != Address::ZERO {
            return Ok((resolver, current));
        }
        if current.is_empty() {
            return Err(EnsError::ResolverNotFound(name.to_string()));
        }
        current = current.split_once('.').map_or("", |(_, parent)| parent);
    }
}

async fn resolve_wildcard<P: Provider>(
    provider: &P,
    resolver: Address,
    name: &str,
    node: B256,
) -> Result<Address, EnsError> {
    let resolver = ExtendedResolver::new(resolver, provider);
    let supported = resolver
        .supportsInterface(EXTENDED_RESOLVER_INTERFACE_ID.into())
        .call()
        .await
        .map_err(EnsError::Resolve)?;
    if !supported {
        return Err(EnsError::WildcardUnsupported(name.to_string()));
    }
    let addr_call = EnsResolver::addrCall { node }.abi_encode();
    let encoded = resolver
        .resolve(dns_encode(name)?.into(), addr_call.into())
        .call()
        .await
        .map_err(EnsError::Resolve)?;
    EnsResolver::addrCall::abi_decode_returns(&encoded).map_err(|e| EnsError::Resolve(e.into()))
}

/// Encodes `name` in DNS wire format, as ENSIP-10 `resolve` expects.
fn dns_encode(name: &str) -> Result<Vec<u8>, EnsError> {
    let mut encoded = Vec::with_capacity(name.len() + 2);
    if !name.is_empty() {
        for label in name.split('.') {
            let len = u8::try_from(label.len())
                .ok()
                .filter(|len| *len > 0)
                .ok_or_else(|| EnsError::InvalidName(name.to_string()))?;
            encoded.push(len);
            encoded.extend_from_slice(label.as_bytes());
        }
    }
    encoded.push(0);
    Ok(encoded)
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
    use alloy_json_rpc::{
        ErrorPayload, RequestPacket, Response, ResponsePacket, ResponsePayload, SerializedRequest,
    };
    use alloy_primitives::{address, hex, Bytes, FixedBytes};
    use alloy_provider::RootProvider;
    use alloy_rpc_client::RpcClient;
    use alloy_rpc_types::TransactionRequest;
    use alloy_transport::{TransportError, TransportFut};
    use serde_json::value::to_raw_value;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    const REGISTRY: Address = address!("00000000000C2E074eC69A0dFb2997BA6C7d2e1e");
    const ENSV2_RESOLVER: Address = address!("3071AD643cE4Ed8b3D67b289845b45dB82b068A4");
    const PUBLIC_RESOLVER: Address = address!("231b0Ee14048e9dCcD1d247744d114a4EB5E8E63");
    const OWNER: Address = address!("a28506D332eDA2f6E1EDAE2abAA194D254AA0c4b");
    const STALE_OWNER: Address = address!("1111111111111111111111111111111111111111");
    /// `UnreachableName(bytes)`, which the ENSv2 resolver reverts with for an unknown name.
    const UNKNOWN_NAME_REVERT: [u8; 4] = hex!("5fe9a5df");

    /// In-memory L1 that answers `eth_call` from modeled registry and resolver state.
    #[derive(Clone, Default)]
    struct FakeChain {
        inner: Arc<FakeChainState>,
    }

    #[derive(Default)]
    struct FakeChainState {
        /// `registry.resolver(node)`; absent means the zero address.
        registry_resolvers: HashMap<B256, Address>,
        /// `addr(node)` answers from resolvers that implement it.
        addr_records: HashMap<(Address, B256), Address>,
        /// ENSIP-10 resolvers and the names they answer `resolve(name, addr(node))` for.
        wildcard_resolvers: HashSet<Address>,
        wildcard_records: HashMap<(Address, String), Address>,
    }

    impl FakeChainState {
        fn set_resolver(&mut self, name: &str, resolver: Address) {
            self.registry_resolvers.insert(namehash(name), resolver);
        }

        fn set_addr(&mut self, resolver: Address, name: &str, addr: Address) {
            self.addr_records.insert((resolver, namehash(name)), addr);
        }

        fn set_wildcard(&mut self, resolver: Address, name: &str, addr: Address) {
            self.wildcard_resolvers.insert(resolver);
            self.wildcard_records
                .insert((resolver, name.to_string()), addr);
        }

        fn is_contract(&self, address: Address) -> bool {
            address == REGISTRY
                || self.wildcard_resolvers.contains(&address)
                || self.addr_records.keys().any(|(r, _)| *r == address)
        }

        /// `Err` carries revert data.
        fn eth_call(&self, to: Address, input: &[u8]) -> Result<Vec<u8>, Vec<u8>> {
            if !self.is_contract(to) {
                return Ok(vec![]);
            }
            let selector: [u8; 4] = input[..4].try_into().unwrap();
            if to == REGISTRY {
                assert_eq!(selector, EnsRegistry::resolverCall::SELECTOR);
                let node = EnsRegistry::resolverCall::abi_decode(input).unwrap().node;
                let resolver = self
                    .registry_resolvers
                    .get(&node)
                    .copied()
                    .unwrap_or_default();
                return Ok(EnsRegistry::resolverCall::abi_encode_returns(&resolver));
            }
            match selector {
                EnsResolver::addrCall::SELECTOR => {
                    let node = EnsResolver::addrCall::abi_decode(input).unwrap().node;
                    match self.addr_records.get(&(to, node)) {
                        Some(addr) => Ok(EnsResolver::addrCall::abi_encode_returns(addr)),
                        None if self.wildcard_resolvers.contains(&to) => Err(vec![]),
                        None => Ok(EnsResolver::addrCall::abi_encode_returns(&Address::ZERO)),
                    }
                }
                ExtendedResolver::supportsInterfaceCall::SELECTOR => {
                    let id = ExtendedResolver::supportsInterfaceCall::abi_decode(input)
                        .unwrap()
                        .interfaceID;
                    let supported = id == FixedBytes(hex!("01ffc9a7"))
                        || (id == FixedBytes(hex!("9061b923"))
                            && self.wildcard_resolvers.contains(&to));
                    Ok(ExtendedResolver::supportsInterfaceCall::abi_encode_returns(
                        &supported,
                    ))
                }
                ExtendedResolver::resolveCall::SELECTOR
                    if self.wildcard_resolvers.contains(&to) =>
                {
                    let call = ExtendedResolver::resolveCall::abi_decode(input).unwrap();
                    let name = dns_decode(&call.name);
                    let inner = EnsResolver::addrCall::abi_decode(&call.data)
                        .expect("only addr(bytes32) is modeled");
                    assert_eq!(inner.node, namehash(&name), "node must match the name");
                    match self.wildcard_records.get(&(to, name)) {
                        Some(addr) => Ok(ExtendedResolver::resolveCall::abi_encode_returns(
                            &Bytes::from(EnsResolver::addrCall::abi_encode_returns(addr)),
                        )),
                        None => Err(UNKNOWN_NAME_REVERT.to_vec()),
                    }
                }
                _ => Err(vec![]),
            }
        }

        fn respond(&self, req: SerializedRequest) -> Response {
            assert_eq!(req.method(), "eth_call");
            let params: Vec<serde_json::Value> =
                serde_json::from_str(req.params().unwrap().get()).unwrap();
            let tx: TransactionRequest = serde_json::from_value(params[0].clone()).unwrap();
            let to = *tx.to.unwrap().to().unwrap();
            let input = tx.input.input().unwrap().clone();
            let payload = match self.eth_call(to, &input) {
                Ok(out) => ResponsePayload::Success(to_raw_value(&Bytes::from(out)).unwrap()),
                Err(revert) => ResponsePayload::Failure(ErrorPayload {
                    code: 3,
                    message: "execution reverted".into(),
                    data: Some(to_raw_value(&Bytes::from(revert)).unwrap()),
                }),
            };
            Response {
                id: req.id().clone(),
                payload,
            }
        }
    }

    impl tower::Service<RequestPacket> for FakeChain {
        type Response = ResponsePacket;
        type Error = TransportError;
        type Future = TransportFut<'static>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: RequestPacket) -> Self::Future {
            let state = self.inner.clone();
            Box::pin(async move {
                Ok(match req {
                    RequestPacket::Single(req) => ResponsePacket::Single(state.respond(req)),
                    RequestPacket::Batch(reqs) => ResponsePacket::Batch(
                        reqs.into_iter().map(|req| state.respond(req)).collect(),
                    ),
                })
            })
        }
    }

    fn dns_decode(wire: &[u8]) -> String {
        let mut labels = vec![];
        let mut rest = wire;
        while rest[0] != 0 {
            let len = rest[0] as usize;
            labels.push(std::str::from_utf8(&rest[1..=len]).unwrap().to_string());
            rest = &rest[len + 1..];
        }
        assert_eq!(rest, [0], "nothing may follow the root label");
        labels.join(".")
    }

    async fn resolve(state: FakeChainState, name: &str) -> Result<Address, EnsError> {
        let transport = FakeChain {
            inner: Arc::new(state),
        };
        let provider = RootProvider::new(RpcClient::new(transport, true));
        resolve_name(&provider, REGISTRY, name).await
    }

    #[tokio::test]
    async fn name_with_own_resolver_resolves_through_it() {
        let mut chain = FakeChainState::default();
        chain.set_resolver("vitalik.eth", PUBLIC_RESOLVER);
        chain.set_addr(PUBLIC_RESOLVER, "vitalik.eth", OWNER);
        chain.set_resolver("eth", ENSV2_RESOLVER);
        chain.set_wildcard(ENSV2_RESOLVER, "vitalik.eth", STALE_OWNER);

        assert_eq!(resolve(chain, "vitalik.eth").await.unwrap(), OWNER);
    }

    #[tokio::test]
    async fn ensv2_only_name_resolves_through_eth_wildcard_resolver() {
        let mut chain = FakeChainState::default();
        chain.set_resolver("eth", ENSV2_RESOLVER);
        chain.set_wildcard(ENSV2_RESOLVER, "basefriendbot.eth", OWNER);

        assert_eq!(resolve(chain, "basefriendbot.eth").await.unwrap(), OWNER);
    }

    #[tokio::test]
    async fn migrated_name_ignores_stale_v1_records() {
        // Migration clears the v1 registry resolver but leaves the old
        // PublicResolver's records in place.
        let mut chain = FakeChainState::default();
        chain.set_addr(PUBLIC_RESOLVER, "nick.eth", STALE_OWNER);
        chain.set_resolver("eth", ENSV2_RESOLVER);
        chain.set_wildcard(ENSV2_RESOLVER, "nick.eth", OWNER);

        assert_eq!(resolve(chain, "nick.eth").await.unwrap(), OWNER);
    }

    #[tokio::test]
    async fn nearest_ancestor_resolver_wins() {
        let other = address!("2222222222222222222222222222222222222222");
        let mut chain = FakeChainState::default();
        chain.set_resolver("base.eth", other);
        chain.set_wildcard(other, "jesse.base.eth", OWNER);
        chain.set_resolver("eth", ENSV2_RESOLVER);
        chain.set_wildcard(ENSV2_RESOLVER, "jesse.base.eth", STALE_OWNER);

        assert_eq!(resolve(chain, "jesse.base.eth").await.unwrap(), OWNER);
    }

    #[tokio::test]
    async fn name_with_no_resolver_at_any_level_is_rejected() {
        let result = resolve(FakeChainState::default(), "nobody.eth").await;

        assert!(
            matches!(result, Err(EnsError::ResolverNotFound(ref n)) if n == "nobody.eth"),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn ancestor_resolver_without_ensip10_is_rejected() {
        // A plain resolver on the parent answers for the parent only; asking it
        // `addr(node)` for the child would be guessing.
        let mut chain = FakeChainState::default();
        chain.set_resolver("eth", PUBLIC_RESOLVER);
        chain.set_addr(PUBLIC_RESOLVER, "alice.eth", STALE_OWNER);

        let result = resolve(chain, "alice.eth").await;

        assert!(
            matches!(result, Err(EnsError::WildcardUnsupported(_))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn unknown_name_under_wildcard_resolver_is_rejected_as_revert() {
        let mut chain = FakeChainState::default();
        chain.set_resolver("eth", ENSV2_RESOLVER);
        chain.set_wildcard(ENSV2_RESOLVER, "basefriendbot.eth", OWNER);

        let result = resolve(chain, "unregistered.eth").await;

        match result {
            Err(EnsError::Resolve(e)) => {
                assert_eq!(e.as_revert_data().unwrap().as_ref(), UNKNOWN_NAME_REVERT)
            }
            other => panic!("expected a revert, got {other:?}"),
        }
    }

    fn assert_hex(hash: B256, val: &str) {
        assert_eq!(hash.0[..], hex::decode(val).unwrap()[..]);
    }

    #[test]
    fn dns_encode_round_trips_and_rejects_empty_labels() {
        assert_eq!(dns_encode("").unwrap(), vec![0]);
        assert_eq!(
            dns_encode("basefriendbot.eth").unwrap(),
            b"\x0dbasefriendbot\x03eth\x00".to_vec()
        );
        assert_eq!(
            dns_decode(&dns_encode("jesse.base.eth").unwrap()),
            "jesse.base.eth"
        );
        assert!(matches!(
            dns_encode("foo..eth"),
            Err(EnsError::InvalidName(_))
        ));
        assert!(matches!(dns_encode(".eth"), Err(EnsError::InvalidName(_))));
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
