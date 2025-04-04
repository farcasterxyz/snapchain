use crate::core::validations::error::ValidationError;
use crate::proto::{self, FarcasterNetwork, MessageData, MessageType, UserDataBody, UserDataType};
use crate::storage::util::{blake3_20, bytes_compare};

use ed25519_dalek::{Signature, VerifyingKey};
use fancy_regex::Regex;
use prost::Message;

use super::{cast, link, reaction, verification};

const MAX_DATA_BYTES: usize = 2048;
const MAX_DATA_BYTES_FOR_LINK_COMPACT: usize = 65536;
const EMBEDS_V1_CUTOFF: u32 = 73612800;

pub fn validate_message_type(message_type: i32) -> Result<(), ValidationError> {
    MessageType::try_from(message_type)
        .map_or_else(|_| Err(ValidationError::InvalidData), |_| Ok(()))
}

pub fn validate_message(
    message: &proto::Message,
    current_network: proto::FarcasterNetwork,
) -> Result<(), ValidationError> {
    let data_bytes;
    let message_data;
    if message.data_bytes.is_some() {
        data_bytes = message.data_bytes.as_ref().unwrap().clone();
        if data_bytes.len() == 0 {
            return Err(ValidationError::MissingData);
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

    if message_data.r#type() == MessageType::LinkCompactState
        && data_bytes.len() > MAX_DATA_BYTES_FOR_LINK_COMPACT
    {
        return Err(ValidationError::InvalidDataLength);
    } else if data_bytes.len() > MAX_DATA_BYTES {
        return Err(ValidationError::InvalidDataLength);
    }

    let network = FarcasterNetwork::try_from(message_data.network)
        .or_else(|_| Err(ValidationError::InvalidNetwork))?;

    if network == FarcasterNetwork::None {
        return Err(ValidationError::InvalidNetwork);
    }
    // Only allow mainnet messages on mainnet. On testnet and devnet, allow all messages.
    if current_network == FarcasterNetwork::Mainnet {
        if network != FarcasterNetwork::Mainnet {
            return Err(ValidationError::InvalidNetwork);
        }
    }

    validate_message_hash(message.hash_scheme, &data_bytes, &message.hash)?;
    validate_signature(
        message.signature_scheme,
        &message.hash,
        &message.signature,
        &message.signer,
    )?;

    match &message_data.body {
        Some(proto::message_data::Body::UserDataBody(user_data)) => {
            validate_user_data_add_body(user_data)?;
        }
        Some(proto::message_data::Body::UsernameProofBody(_)) => {
            // Validate ens
        }
        Some(proto::message_data::Body::VerificationAddAddressBody(add)) => {
            verification::validate_add_address(&add, message_data.fid, network)?;
        }
        Some(proto::message_data::Body::LinkCompactStateBody(link_compact_state_body)) => {
            link::validate_link_compact_state_body(&link_compact_state_body)?;
        }
        Some(proto::message_data::Body::CastAddBody(cast_add_body)) => {
            cast::validate_cast_add_body(
                &cast_add_body,
                message_data.timestamp < EMBEDS_V1_CUTOFF,
            )?;
        }
        Some(proto::message_data::Body::CastRemoveBody(cast_remove_body)) => {
            cast::validate_cast_remove_body(&cast_remove_body)?;
        }
        Some(proto::message_data::Body::ReactionBody(reaction_body)) => {
            reaction::validate_reaction_body(&reaction_body)?;
        }
        Some(proto::message_data::Body::LinkBody(link_body)) => {
            link::validate_link_body(&link_body)?;
        }
        Some(proto::message_data::Body::VerificationRemoveBody(remove_body)) => {
            verification::validate_remove_address(&remove_body)?;
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

pub fn validate_fname(input: &String) -> Result<(), ValidationError> {
    if input.len() == 0 {
        return Err(ValidationError::InvalidDataLength);
    }

    // FNAME_MAX_LENGTH - ".eth".length
    if input.len() > 16 {
        return Err(ValidationError::InvalidDataLength);
    }

    if !Regex::new("^[a-z0-9][a-z0-9-]{0,15}$")
        .unwrap()
        .is_match(&input)
        .map_err(|_| ValidationError::InvalidData)?
    {
        return Err(ValidationError::InvalidData);
    }

    Ok(())
}

pub fn validate_ens_name(input: &String) -> Result<(), ValidationError> {
    if !input.ends_with(".eth") {
        return Err(ValidationError::InvalidDataLength);
    }

    let name_parts: Vec<&str> = input.split('.').collect();
    if name_parts.len() != 2 || name_parts[0].is_empty() {
        return Err(ValidationError::InvalidData);
    }

    if input.len() > 20 {
        return Err(ValidationError::InvalidDataLength);
    }

    if !Regex::new("^[a-z0-9][a-z0-9-]{0,15}$")
        .unwrap()
        .is_match(name_parts[0])
        .map_err(|_| ValidationError::InvalidData)?
    {
        return Err(ValidationError::InvalidData);
    }

    Ok(())
}

pub fn validate_twitter_username(input: &String) -> Result<(), ValidationError> {
    if input.len() > 15 {
        return Err(ValidationError::InvalidDataLength);
    }

    if !Regex::new("^[a-z0-9_]{0,15}$")
        .unwrap()
        .is_match(&input)
        .map_err(|_| ValidationError::InvalidData)?
    {
        return Err(ValidationError::InvalidData);
    }

    Ok(())
}

pub fn validate_github_username(input: &String) -> Result<(), ValidationError> {
    if input.len() > 38 {
        return Err(ValidationError::InvalidDataLength);
    }

    if !Regex::new("^[a-zA-Z\\d](?:[a-zA-Z\\d]|-(?!-)){0,38}$")
        .unwrap()
        .is_match(&input)
        .map_err(|_| ValidationError::InvalidData)?
    {
        return Err(ValidationError::InvalidData);
    }

    Ok(())
}

fn validate_number(value: &str) -> Result<f64, ValidationError> {
    return value
        .parse::<f64>()
        .map_err(|_| ValidationError::InvalidData);
}

fn validate_latitude(value: &str) -> Result<(), ValidationError> {
    let number = validate_number(value)?;

    if number < -90.0 || number > 90.0 {
        return Err(ValidationError::InvalidData);
    }

    Ok(())
}

fn validate_longitude(value: &str) -> Result<(), ValidationError> {
    let number = validate_number(value)?;

    if number < -180.0 || number > 180.0 {
        return Err(ValidationError::InvalidData);
    }

    Ok(())
}

pub fn validate_user_location(location: &str) -> Result<(), ValidationError> {
    if location.is_empty() {
        return Ok(());
    }

    let captures = Regex::new(r"^geo:(-?\d{1,2}\.\d{2}),(-?\d{1,3}\.\d{2})$")
        .unwrap()
        .captures(location)
        .map_err(|_| ValidationError::InvalidData)?;

    if captures.is_none() {
        return Err(ValidationError::InvalidData);
    }

    let captured = captures.unwrap();

    let latitude = captured
        .get(1)
        .ok_or_else(|| ValidationError::InvalidData)?;
    validate_latitude(latitude.as_str())?;

    let longitude = captured
        .get(2)
        .ok_or_else(|| ValidationError::InvalidData)?;
    validate_longitude(longitude.as_str())?;

    Ok(())
}

pub fn validate_user_data_add_body(body: &UserDataBody) -> Result<(), ValidationError> {
    let value_bytes = body.value.as_bytes();

    match UserDataType::try_from(body.r#type).map_err(|_| ValidationError::InvalidData)? {
        UserDataType::Pfp => {
            if value_bytes.len() > 256 {
                return Err(ValidationError::InvalidDataLength);
            }
        }
        UserDataType::Display => {
            if value_bytes.len() > 32 {
                return Err(ValidationError::InvalidDataLength);
            }
        }
        UserDataType::Bio => {
            if value_bytes.len() > 256 {
                return Err(ValidationError::InvalidDataLength);
            }
        }
        UserDataType::Url => {
            if value_bytes.len() > 256 {
                return Err(ValidationError::InvalidDataLength);
            }
        }
        UserDataType::Username => {
            // Users are allowed to set fname = '' to remove their fname
            if !body.value.is_empty() {
                let fname_result = validate_fname(&body.value);
                let ens_result = validate_ens_name(&body.value);
                if fname_result.is_err() && ens_result.is_err() {
                    return fname_result;
                }
            }
        }
        UserDataType::Location => {
            validate_user_location(&body.value)?;
        }
        UserDataType::Twitter => {
            validate_twitter_username(&body.value)?;
        }
        UserDataType::Github => {
            validate_github_username(&body.value)?;
        }
        UserDataType::None => return Err(ValidationError::InvalidData),
    }

    Ok(())
}
