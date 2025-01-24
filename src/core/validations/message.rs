use crate::core::validations::error::ValidationError;
use crate::proto::{self, FarcasterNetwork, MessageData, MessageType};
use crate::storage::util::{blake3_20, bytes_compare};

use ed25519_dalek::{Signature, VerifyingKey};
use prost::Message;

use super::{cast, link, reaction, verification};

const MAX_DATA_BYTES: usize = 2048;
const EMBEDS_V1_CUTOFF: u32 = 73612800;

pub fn validate_message_type(message_type: i32) -> Result<(), ValidationError> {
    MessageType::try_from(message_type)
        .map_or_else(|_| Err(ValidationError::InvalidData), |_| Ok(()))
}

pub fn validate_message(message: &proto::Message) -> Result<(), ValidationError> {
    let data_bytes;
    let message_data;
    if message.data_bytes.is_some() {
        data_bytes = message.data_bytes.as_ref().unwrap().clone();
        if data_bytes.len() > MAX_DATA_BYTES {
            return Err(ValidationError::InvalidDataLength);
        }
        match MessageData::decode(message.data_bytes.as_ref().unwrap().as_slice()) {
            Ok(data) => {
                message_data = data.clone();
            }
            Err(_) => {
                return Err(ValidationError::InvalidData);
            }
        }
    } else {
        if message.data.is_none() {
            return Err(ValidationError::MissingData);
        }
        data_bytes = message.data.as_ref().unwrap().encode_to_vec();
        message_data = message.data.as_ref().unwrap().clone();
    }

    validate_message_hash(message.hash_scheme, &data_bytes, &message.hash)?;
    validate_signature(
        message.signature_scheme,
        &message.hash,
        &message.signature,
        &message.signer,
    )?;

    let network = FarcasterNetwork::try_from(message_data.network)
        .or_else(|_| Err(ValidationError::InvalidData))?;

    match &message_data.body {
        Some(proto::message_data::Body::UserDataBody(_)) => {
            // Split user data validation from engine
        }
        Some(proto::message_data::Body::UsernameProofBody(_)) => {
            // Validate ens
        }
        Some(proto::message_data::Body::VerificationAddAddressBody(add)) => {
            let result = verification::validate_add_address(&add, message_data.fid, network);
            if result.is_err() {
                return result;
            }
        }
        Some(proto::message_data::Body::LinkCompactStateBody(link_compact_state_body)) => {
            let result = link::validate_link_compact_state_body(&link_compact_state_body);
            if result.is_err() {
                return result;
            }
        }
        Some(proto::message_data::Body::CastAddBody(cast_add_body)) => {
            let result = cast::validate_cast_add_body(
                &cast_add_body,
                message_data.timestamp < EMBEDS_V1_CUTOFF,
            );
            if result.is_err() {
                return result;
            }
        }
        Some(proto::message_data::Body::CastRemoveBody(cast_remove_body)) => {
            let result = cast::validate_cast_remove_body(&cast_remove_body);
            if result.is_err() {
                return result;
            }
        }
        Some(proto::message_data::Body::ReactionBody(reaction_body)) => {
            let result = reaction::validate_reaction_body(&reaction_body);
            if result.is_err() {
                return result;
            }
        }
        Some(proto::message_data::Body::LinkBody(link_body)) => {
            let result = link::validate_link_body(&link_body);
            if result.is_err() {
                return result;
            }
        }
        Some(proto::message_data::Body::VerificationRemoveBody(remove_body)) => {
            let result = verification::validate_remove_address(&remove_body);
            if result.is_err() {
                return result;
            }
        }
        Some(proto::message_data::Body::FrameActionBody(_)) => {}
        None => {
            return Err(ValidationError::MissingData);
        }
    }

    Ok(())
}

fn validate_signature(
    signature_scheme: i32,
    data_bytes: &Vec<u8>,
    signature: &Vec<u8>,
    signer: &Vec<u8>,
) -> Result<(), ValidationError> {
    if signature_scheme != proto::SignatureScheme::Ed25519 as i32 {
        return Err(ValidationError::InvalidSignatureScheme);
    }

    if signature.len() == 0 {
        return Err(ValidationError::MissingSignature);
    }

    let sig = Signature::from_slice(signature).map_err(|_| ValidationError::InvalidSignature)?;
    let public_key = VerifyingKey::try_from(signer.as_slice())
        .map_err(|_| ValidationError::MissingOrInvalidSigner)?;

    public_key
        .verify_strict(data_bytes.as_slice(), &sig)
        .map_err(|_| ValidationError::InvalidSignature)?;

    Ok(())
}

fn validate_message_hash(
    hash_scheme: i32,
    data_bytes: &Vec<u8>,
    hash: &Vec<u8>,
) -> Result<(), ValidationError> {
    if hash_scheme != proto::HashScheme::Blake3 as i32 {
        return Err(ValidationError::InvalidHashScheme);
    }

    if data_bytes.len() == 0 {
        return Err(ValidationError::MissingData);
    }

    let result = blake3_20(data_bytes);
    if bytes_compare(&result, hash) != 0 {
        return Err(ValidationError::InvalidHash);
    }
    Ok(())
}
