//! Message builders lifted from `snapchain::utils::factory::messages_factory`.
//!
//! Only the subset the CLI uses is mirrored here:
//!   - [`casts::create_cast_add`], [`casts::create_cast_remove`]
//!   - [`keys::create_key_add`], [`keys::create_key_remove_custody`], [`keys::create_key_remove_self_revoke`]
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
