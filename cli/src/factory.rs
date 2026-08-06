//! Message builders lifted from `snapchain::utils::factory::messages_factory`.
//!
//! Only the subset the CLI uses is mirrored here:
//!   - [`casts::create_cast_add`], [`casts::create_cast_remove`]
//!   - [`keys::create_key_add`], [`keys::create_key_remove_custody`], [`keys::create_key_remove_self_revoke`]
//!   - [`links::create_link_add`], [`links::create_link_remove`], [`links::create_link_compact_state`]
//!   - [`user_data::create_user_data_add`]
//!
//! `create_message_with_data` hard-codes `FarcasterNetwork::Mainnet`; the CLI's `retarget_network`
//! helper in `main.rs` rewrites the network and re-signs before submission.

use ed25519_dalek::{Signer, SigningKey};
use prost::Message as _;
use snapchain_proto::{self as proto, FarcasterNetwork, MessageData, MessageType};

use crate::helpers::{calculate_message_hash, farcaster_time};

fn create_message_with_data(
    fid: u64,
    msg_type: MessageType,
    body: proto::message_data::Body,
    timestamp: Option<u32>,
    private_key: &SigningKey,
) -> proto::Message {
    let network = FarcasterNetwork::Mainnet;
    let timestamp = timestamp.unwrap_or_else(farcaster_time);

    let msg_data = MessageData {
        fid,
        r#type: msg_type as i32,
        timestamp,
        network: network as i32,
        body: Some(body),
    };

    let msg_data_bytes = msg_data.encode_to_vec();
    let hash = calculate_message_hash(&msg_data_bytes);
    let signature = private_key.sign(&hash).to_bytes();

    proto::Message {
        data: Some(msg_data),
        hash_scheme: proto::HashScheme::Blake3 as i32,
        hash: hash.clone(),
        signature_scheme: proto::SignatureScheme::Ed25519 as i32,
        signature: signature.to_vec(),
        signer: private_key.verifying_key().to_bytes().to_vec(),
        data_bytes: None,
    }
}

pub mod casts {
    use super::*;
    use snapchain_proto::{CastAddBody, CastRemoveBody, CastType};

    pub fn create_cast_add(
        fid: u64,
        text: &str,
        timestamp: Option<u32>,
        private_key: &SigningKey,
    ) -> proto::Message {
        let body = CastAddBody {
            text: text.to_string(),
            embeds: vec![],
            embeds_deprecated: vec![],
            mentions: vec![],
            mentions_positions: vec![],
            parent: None,
            r#type: CastType::Cast as i32,
        };
        create_message_with_data(
            fid,
            MessageType::CastAdd,
            proto::message_data::Body::CastAddBody(body),
            timestamp,
            private_key,
        )
    }

    pub fn create_cast_remove(
        fid: u64,
        target_hash: &[u8],
        timestamp: Option<u32>,
        private_key: &SigningKey,
    ) -> proto::Message {
        let body = CastRemoveBody {
            target_hash: target_hash.to_vec(),
        };
        create_message_with_data(
            fid,
            MessageType::CastRemove,
            proto::message_data::Body::CastRemoveBody(body),
            timestamp,
            private_key,
        )
    }
}

pub mod links {
    use super::*;
    use snapchain_proto::{link_body::Target, LinkBody, LinkCompactStateBody};

    pub fn create_link_add(
        fid: u64,
        link_type: &str,
        target_fid: u64,
        display_timestamp: Option<u32>,
        timestamp: Option<u32>,
        private_key: &SigningKey,
    ) -> proto::Message {
        let body = LinkBody {
            r#type: link_type.to_string(),
            display_timestamp,
            target: Some(Target::TargetFid(target_fid)),
        };
        create_message_with_data(
            fid,
            MessageType::LinkAdd,
            proto::message_data::Body::LinkBody(body),
            timestamp,
            private_key,
        )
    }

    pub fn create_link_remove(
        fid: u64,
        link_type: &str,
        target_fid: u64,
        timestamp: Option<u32>,
        private_key: &SigningKey,
    ) -> proto::Message {
        let body = LinkBody {
            r#type: link_type.to_string(),
            display_timestamp: None,
            target: Some(Target::TargetFid(target_fid)),
        };
        create_message_with_data(
            fid,
            MessageType::LinkRemove,
            proto::message_data::Body::LinkBody(body),
            timestamp,
            private_key,
        )
    }

    pub fn create_link_compact_state(
        fid: u64,
        link_type: &str,
        target_fids: Vec<u64>,
        timestamp: Option<u32>,
        private_key: &SigningKey,
    ) -> proto::Message {
        let body = LinkCompactStateBody {
            r#type: link_type.to_string(),
            target_fids,
        };
        create_message_with_data(
            fid,
            MessageType::LinkCompactState,
            proto::message_data::Body::LinkCompactStateBody(body),
            timestamp,
            private_key,
        )
    }
}

pub mod user_data {
    use super::*;
    use snapchain_proto::{UserDataBody, UserDataType};

    pub fn create_user_data_add(
        fid: u64,
        user_data_type: UserDataType,
        value: &str,
        timestamp: Option<u32>,
        private_key: &SigningKey,
    ) -> proto::Message {
        let body = UserDataBody {
            r#type: user_data_type as i32,
            value: value.to_string(),
        };
        create_message_with_data(
            fid,
            MessageType::UserDataAdd,
            proto::message_data::Body::UserDataBody(body),
            timestamp,
            private_key,
        )
    }
}

/// On-chain event builders, mirroring `snapchain::utils::factory::events_factory`.
///
/// These are only useful against a devnet, where the AdminService accepts synthetic events
/// instead of them being decoded from a real chain.
pub mod events {
    use super::*;
    use rand::RngCore;
    use snapchain_proto::{
        on_chain_event, IdRegisterEventBody, IdRegisterEventType, OnChainEvent, OnChainEventType,
        SignerEventBody, SignerEventType, StorageRentEventBody,
    };

    fn random_bytes(n: usize) -> Vec<u8> {
        let mut buf = vec![0u8; n];
        rand::rngs::OsRng.fill_bytes(&mut buf);
        buf
    }

    /// Block timestamp used for every synthetic event: 10s in the past, so it is safely
    /// behind the node's clock but still recent.
    ///
    /// For storage rent this lands past the 2025 cutoff, which the store classifies as a
    /// current-cohort `UNIT_TYPE_2025` rental valid for one year — exactly what a devnet fid
    /// needs. Back-dating to force an older cohort is not something this CLI supports.
    fn block_timestamp() -> u32 {
        crate::helpers::unix_time() - 10
    }

    /// The store relies on block number ordering matching timestamp ordering, so derive one
    /// from the other: shift the timestamp up by 10 bits and fill the low bits with noise so
    /// two events in the same second still get distinct block numbers.
    fn block_number_for(timestamp: u32) -> u32 {
        (timestamp << 10) + (rand::random::<u32>() % 1000)
    }

    fn base_event(event_type: OnChainEventType, fid: u64, timestamp: u32) -> OnChainEvent {
        OnChainEvent {
            r#type: event_type as i32,
            chain_id: 10,
            block_number: block_number_for(timestamp),
            block_hash: vec![],
            block_timestamp: timestamp as u64,
            transaction_hash: random_bytes(32),
            log_index: 0,
            fid,
            tx_index: 0,
            version: 1,
            body: None,
        }
    }

    pub fn create_rent_event(fid: u64, units: u32) -> OnChainEvent {
        let timestamp = block_timestamp();
        OnChainEvent {
            body: Some(on_chain_event::Body::StorageRentEventBody(
                StorageRentEventBody {
                    // Ignored by the store, which derives expiry from block_timestamp.
                    expiry: 0,
                    units,
                    payer: random_bytes(32),
                },
            )),
            ..base_event(OnChainEventType::EventTypeStorageRent, fid, timestamp)
        }
    }

    /// Registers the fid. Required before any message from it will merge, and separately
    /// required by the M2 verification migration, whose `FIDIterator` enumerates fids from
    /// IdRegister events — a fid without one is invisible to it.
    pub fn create_id_register_event(fid: u64, custody_address: Vec<u8>) -> OnChainEvent {
        let timestamp = block_timestamp();
        OnChainEvent {
            body: Some(on_chain_event::Body::IdRegisterEventBody(
                IdRegisterEventBody {
                    to: custody_address,
                    event_type: IdRegisterEventType::Register as i32,
                    from: vec![],
                    recovery_address: vec![],
                },
            )),
            ..base_event(OnChainEventType::EventTypeIdRegister, fid, timestamp)
        }
    }

    /// Authorizes `signer` to sign messages for `fid`.
    pub fn create_signer_event(fid: u64, signer: &SigningKey) -> OnChainEvent {
        let timestamp = block_timestamp();
        OnChainEvent {
            body: Some(on_chain_event::Body::SignerEventBody(SignerEventBody {
                key: signer.verifying_key().as_bytes().to_vec(),
                event_type: SignerEventType::Add as i32,
                metadata: vec![],
                key_type: 1,
                metadata_type: 1,
            })),
            ..base_event(OnChainEventType::EventTypeSigner, fid, timestamp)
        }
    }
}

pub mod verifications {
    use super::*;
    use crate::eip712::verification_claim_typed_data;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use snapchain_proto::{VerificationAddAddressBody, VerificationRemoveBody};

    /// EOA verification. The alternative, `1`, means "contract signature", which the node
    /// validates out of the consensus loop against a configured chain RPC
    /// (`server.rs::submit_message_internal`) — unusable against a devnet with no RPC URL.
    const VERIFICATION_TYPE_EOA: u32 = 0;

    /// `0` is in the node's accepted set (`EIP_712_FARCASTER_VERIFICATION_CLAIM_CHAIN_IDS`)
    /// and matches what `messages_factory::verifications` uses.
    const CLAIM_CHAIN_ID: u32 = 0;

    /// Ethereum. Solana verifications sign a plain ASCII message instead of EIP-712 typed
    /// data and are not supported here.
    const PROTOCOL_ETHEREUM: i32 = 0;

    /// Build a VERIFICATION_ADD_ETH_ADDRESS carrying a real EOA claim signature.
    ///
    /// The claim binds (fid, address, block_hash, network); the node recovers the signer from
    /// it and requires that it equal `address`, so `wallet` must be the key for the address
    /// being verified — this is a bidirectional proof, not just an assertion by `fid`.
    ///
    /// `network` must match the network the message is ultimately submitted under. Callers
    /// going through `retarget_network` therefore have to pass the *final* network here, not
    /// the factory's placeholder, or the claim will not match the re-tagged message.
    pub fn create_verification_add(
        fid: u64,
        wallet: &PrivateKeySigner,
        block_hash: &[u8],
        network: FarcasterNetwork,
        timestamp: Option<u32>,
        private_key: &SigningKey,
    ) -> Result<proto::Message, Box<dyn std::error::Error>> {
        let address = wallet.address().to_vec();
        let typed_data = verification_claim_typed_data(fid, &address, block_hash, network as i32)?;
        let claim_signature: Vec<u8> = wallet
            .sign_hash_sync(&typed_data.eip712_signing_hash()?)?
            .into();

        let body = VerificationAddAddressBody {
            address,
            claim_signature,
            block_hash: block_hash.to_vec(),
            verification_type: VERIFICATION_TYPE_EOA,
            chain_id: CLAIM_CHAIN_ID,
            protocol: PROTOCOL_ETHEREUM,
        };
        Ok(create_message_with_data(
            fid,
            MessageType::VerificationAddEthAddress,
            proto::message_data::Body::VerificationAddAddressBody(body),
            timestamp,
            private_key,
        ))
    }

    /// Build a VERIFICATION_REMOVE. No claim signature: the fid that added the verification
    /// is the only one that can remove it, so authority comes from the envelope signer.
    pub fn create_verification_remove(
        fid: u64,
        address: &[u8],
        timestamp: Option<u32>,
        private_key: &SigningKey,
    ) -> proto::Message {
        let body = VerificationRemoveBody {
            address: address.to_vec(),
            protocol: PROTOCOL_ETHEREUM,
        };
        create_message_with_data(
            fid,
            MessageType::VerificationRemove,
            proto::message_data::Body::VerificationRemoveBody(body),
            timestamp,
            private_key,
        )
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use snapchain::core::validations::verification::{
            validate_add_address, validate_remove_address,
        };

        fn body_of(msg: proto::Message) -> proto::message_data::Body {
            msg.data.expect("data").body.expect("body")
        }

        /// The CLI hand-mirrors the node's EIP-712 claim encoding, so run what it produces
        /// through the node's own validator. This is what makes the duplication safe: change
        /// the domain, the type list, or the hex encoding of a claim field on either side and
        /// this fails, instead of the CLI silently emitting messages the node rejects.
        ///
        /// Covers every network because the network is part of the signed claim — an encoding
        /// that only happens to work on Devnet would be a trap for anyone pointing `fc` at
        /// testnet.
        #[test]
        fn verification_add_satisfies_node_validation() {
            let wallet = PrivateKeySigner::random();
            let signer = SigningKey::from_bytes(&[7u8; 32]);
            let block_hash = [3u8; 32];

            for network in [
                FarcasterNetwork::Devnet,
                FarcasterNetwork::Testnet,
                FarcasterNetwork::Mainnet,
            ] {
                let msg = create_verification_add(42, &wallet, &block_hash, network, None, &signer)
                    .expect("claim should sign");
                let proto::message_data::Body::VerificationAddAddressBody(body) = body_of(msg)
                else {
                    panic!("expected a VerificationAddAddressBody");
                };

                assert_eq!(body.address, wallet.address().to_vec());
                assert_eq!(body.claim_signature.len(), 65);
                validate_add_address(&body, 42, network).unwrap_or_else(|e| {
                    panic!(
                        "node rejected CLI-built verification on {:?}: {:?}",
                        network, e
                    )
                });
            }
        }

        /// A claim is only valid for the fid it was signed for; the node recovers the signer
        /// and compares against the body address, so a mismatched fid changes the digest and
        /// must not validate. Guards against dropping `fid` from the claim message.
        #[test]
        fn verification_add_claim_is_bound_to_its_fid() {
            let wallet = PrivateKeySigner::random();
            let signer = SigningKey::from_bytes(&[9u8; 32]);
            let msg = create_verification_add(
                42,
                &wallet,
                &[3u8; 32],
                FarcasterNetwork::Devnet,
                None,
                &signer,
            )
            .expect("claim should sign");
            let proto::message_data::Body::VerificationAddAddressBody(body) = body_of(msg) else {
                panic!("expected a VerificationAddAddressBody");
            };

            assert!(validate_add_address(&body, 43, FarcasterNetwork::Devnet).is_err());
        }

        /// Same for the network, which is the field most likely to be quietly wrong: the CLI
        /// builds the claim before `retarget_network` rewrites the envelope, so if the two
        /// ever disagree the message is rejected on-node.
        #[test]
        fn verification_add_claim_is_bound_to_its_network() {
            let wallet = PrivateKeySigner::random();
            let signer = SigningKey::from_bytes(&[11u8; 32]);
            let msg = create_verification_add(
                42,
                &wallet,
                &[3u8; 32],
                FarcasterNetwork::Devnet,
                None,
                &signer,
            )
            .expect("claim should sign");
            let proto::message_data::Body::VerificationAddAddressBody(body) = body_of(msg) else {
                panic!("expected a VerificationAddAddressBody");
            };

            assert!(validate_add_address(&body, 42, FarcasterNetwork::Mainnet).is_err());
        }

        #[test]
        fn verification_remove_satisfies_node_validation() {
            let signer = SigningKey::from_bytes(&[13u8; 32]);
            let address = vec![0xabu8; 20];
            let msg = create_verification_remove(42, &address, None, &signer);
            let proto::message_data::Body::VerificationRemoveBody(body) = body_of(msg) else {
                panic!("expected a VerificationRemoveBody");
            };

            assert_eq!(body.address, address);
            validate_remove_address(&body).expect("node rejected CLI-built verification remove");
        }
    }
}

pub mod keys {
    use super::*;
    use crate::eip712::{
        key_add_typed_data, key_remove_typed_data, signed_key_request_typed_data, KeyAddPayload,
        KeyRemovePayload, ETH_MAINNET_CHAIN_ID, METADATA_TYPE_SIGNED_KEY_REQUEST,
    };
    use alloy_primitives::{Bytes, U256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use alloy_sol_types::{sol, SolValue};
    use snapchain_proto::{KeyAddBody, KeyRemoveBody};

    const KEY_TYPE_ED25519: u32 = 1;

    // Same field layout as the on-chain SignedKeyRequestValidator.SignedKeyRequestMetadata
    // struct. Redeclared locally so the CLI doesn't have to depend on snapchain proper.
    sol! {
        struct SignedKeyRequestMetadata {
            uint256 requestFid;
            address requestSigner;
            bytes signature;
            uint256 deadline;
        }
    }

    fn build_signed_metadata_bytes(
        app_custody: &PrivateKeySigner,
        request_fid: u64,
        key: &[u8],
        deadline: u64,
    ) -> Vec<u8> {
        let typed_data =
            signed_key_request_typed_data(request_fid, key, deadline, ETH_MAINNET_CHAIN_ID)
                .expect("typed data construction is infallible for valid inputs");
        let prehash = typed_data
            .eip712_signing_hash()
            .expect("eip712 prehash is infallible");
        let sig: Vec<u8> = app_custody
            .sign_hash_sync(&prehash)
            .expect("PrivateKeySigner sign cannot fail")
            .into();
        SignedKeyRequestMetadata {
            requestFid: U256::from(request_fid),
            requestSigner: app_custody.address(),
            signature: Bytes::from(sig),
            deadline: U256::from(deadline),
        }
        .abi_encode()
    }

    pub fn create_key_add(
        fid: u64,
        fid_custody: &PrivateKeySigner,
        request_fid: u64,
        app_custody: &PrivateKeySigner,
        envelope_signer: &SigningKey,
        scopes: Vec<MessageType>,
        ttl: u32,
        nonce: u32,
        deadline: u32,
        timestamp: Option<u32>,
    ) -> proto::Message {
        let key_bytes: [u8; 32] = envelope_signer.verifying_key().to_bytes();
        let scopes_i32: Vec<i32> = scopes.iter().map(|s| *s as i32).collect();

        let payload = KeyAddPayload {
            fid,
            key: &key_bytes,
            key_type: KEY_TYPE_ED25519,
            scopes: &scopes_i32,
            ttl,
            nonce,
            deadline,
        };
        let typed_data = key_add_typed_data(&payload, ETH_MAINNET_CHAIN_ID)
            .expect("typed data construction is infallible for valid inputs");
        let prehash = typed_data
            .eip712_signing_hash()
            .expect("eip712 prehash is infallible");
        let custody_sig: Vec<u8> = fid_custody
            .sign_hash_sync(&prehash)
            .expect("PrivateKeySigner sign cannot fail")
            .into();

        let metadata =
            build_signed_metadata_bytes(app_custody, request_fid, &key_bytes, deadline as u64);

        let body = KeyAddBody {
            key: key_bytes.to_vec(),
            key_type: KEY_TYPE_ED25519,
            custody_signature: custody_sig,
            deadline,
            nonce,
            metadata,
            metadata_type: METADATA_TYPE_SIGNED_KEY_REQUEST,
            registration_tx_hash: vec![],
            scopes: scopes_i32,
            ttl,
        };

        create_message_with_data(
            fid,
            MessageType::KeyAdd,
            proto::message_data::Body::KeyAddBody(body),
            timestamp,
            envelope_signer,
        )
    }

    pub fn create_key_remove_custody(
        fid: u64,
        fid_custody: &PrivateKeySigner,
        envelope_signer: &SigningKey,
        target_key: &[u8; 32],
        nonce: u32,
        deadline: u32,
        timestamp: Option<u32>,
    ) -> proto::Message {
        let payload = KeyRemovePayload {
            fid,
            key: target_key,
            nonce,
            deadline,
        };
        let typed_data = key_remove_typed_data(&payload, ETH_MAINNET_CHAIN_ID)
            .expect("typed data construction is infallible for valid inputs");
        let prehash = typed_data
            .eip712_signing_hash()
            .expect("eip712 prehash is infallible");
        let custody_sig: Vec<u8> = fid_custody
            .sign_hash_sync(&prehash)
            .expect("PrivateKeySigner sign cannot fail")
            .into();

        let body = KeyRemoveBody {
            key: target_key.to_vec(),
            signature: custody_sig,
            signature_type: 1, // Custody
            deadline,
            nonce,
        };

        create_message_with_data(
            fid,
            MessageType::KeyRemove,
            proto::message_data::Body::KeyRemoveBody(body),
            timestamp,
            envelope_signer,
        )
    }

    pub fn create_key_remove_self_revoke(
        fid: u64,
        envelope_signer: &SigningKey,
        nonce: u32,
        deadline: u32,
        timestamp: Option<u32>,
    ) -> proto::Message {
        let key_bytes: [u8; 32] = envelope_signer.verifying_key().to_bytes();
        let body = KeyRemoveBody {
            key: key_bytes.to_vec(),
            signature: vec![],
            signature_type: 2, // SelfRevoke
            deadline,
            nonce,
        };
        create_message_with_data(
            fid,
            MessageType::KeyRemove,
            proto::message_data::Body::KeyRemoveBody(body),
            timestamp,
            envelope_signer,
        )
    }
}
