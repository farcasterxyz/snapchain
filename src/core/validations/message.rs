use crate::core::util::FarcasterTime;
use crate::core::validations::error::ValidationError;
use crate::core::validations::validate_cast_id;
use crate::proto::{
    self, FarcasterNetwork, FrameActionBody, MessageData, MessageType, UserDataBody, UserDataType,
    UserNameType,
};
use crate::storage::util::{blake3_20, bytes_compare};

use super::{cast, link, reaction, verification};
use crate::version::version::{EngineVersion, ProtocolFeature};
use alloy_primitives::hex::FromHex;
use alloy_primitives::Address;
use ed25519_dalek::{Signature, VerifyingKey};
use fancy_regex::Regex;
use prost::Message;

const MAX_DATA_BYTES: usize = 2048;
const MAX_DATA_BYTES_FOR_10K_CAST: usize = 16_384;
const MAX_DATA_BYTES_FOR_LINK_COMPACT: usize = 65536;
const EMBEDS_V1_CUTOFF: u32 = 73612800;
const TWITTER_USERNAME_REGEX: &str = "^[a-z0-9_]{0,15}$";
const FNAME_REGEX: &str = "^[a-z0-9][a-z0-9-]{0,15}$";
const GITHUB_USERNAME_REGEX: &str = "^[a-zA-Z\\d](?:[a-zA-Z\\d]|-(?!-)){0,38}$";
/** Number of seconds (10 minutes) that is appropriate for clock skew */
const ALLOWED_CLOCK_SKEW_SECONDS: u64 = 10 * 60;

pub fn validate_message_type(message_type: i32) -> Result<(), ValidationError> {
    MessageType::try_from(message_type)
        .map_or_else(|_| Err(ValidationError::InvalidMessageType), |_| Ok(()))
}

fn validate_bytes_as_string(
    byte_array: &Vec<u8>,
    max_length: u64,
    required: bool,
) -> Result<(), ValidationError> {
    if required && byte_array.len() == 0 {
        return Err(ValidationError::MissingString);
    }
    if byte_array.len() as u64 > max_length {
        return Err(ValidationError::StringTooLong);
    }
    Ok(())
}

fn validate_frame_action_body(body: &FrameActionBody) -> Result<(), ValidationError> {
    // url and buttonId are required and must not exceed the length limits. cast id is optional
    if body.button_index > 5 {
        return Err(ValidationError::InvalidButtonIndex);
    }

    validate_bytes_as_string(&body.url, 1024, true)?;

    validate_bytes_as_string(&body.input_text, 256, false)?;

    validate_bytes_as_string(&body.state, 4096, false)?;

    validate_bytes_as_string(&body.transaction_id, 256, false)?;

    validate_bytes_as_string(&body.address, 64, false)?;

    if let Some(cast_id) = &body.cast_id {
        validate_cast_id(cast_id)?;
    }

    Ok(())
}

pub fn validate_timestamp(
    message_timestamp: u32,
    block_timestamp: &FarcasterTime,
    engine_version: EngineVersion,
) -> Result<(), ValidationError> {
    if engine_version.is_enabled(ProtocolFeature::FutureTimestampValidation) {
        // Using checked_sub to manage message timestamps from the past correctly. The times are all represented as uints so with regular subtraction this code will panic on underflow.
        if let Some(difference) = (message_timestamp as u64).checked_sub(block_timestamp.to_u64()) {
            if difference > ALLOWED_CLOCK_SKEW_SECONDS {
                return Err(ValidationError::TimestampTooFarInFuture);
            }
        }
    }
    Ok(())
}

pub fn validate_message(
    message: &proto::Message,
    current_network: proto::FarcasterNetwork,
    is_pro_user: bool,
    block_timestamp: &FarcasterTime,
    version: EngineVersion,
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

    let max_data_size =
        if version.is_enabled(crate::version::version::ProtocolFeature::MessageLengthCheckFix) {
            if message_data.r#type() == MessageType::LinkCompactState {
                MAX_DATA_BYTES_FOR_LINK_COMPACT
            } else if is_pro_user && message_data.r#type() == MessageType::CastAdd {
                MAX_DATA_BYTES_FOR_10K_CAST
            } else {
                MAX_DATA_BYTES
            }
        } else {
            MAX_DATA_BYTES
        };

    if data_bytes.len() > max_data_size {
        return Err(ValidationError::DataBytesTooLong(max_data_size as u64));
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
    validate_timestamp(message_data.timestamp, block_timestamp, version)?;

    match &message_data.body {
        Some(proto::message_data::Body::UserDataBody(user_data)) => {
            validate_user_data_add_body(user_data, is_pro_user, version)?;
        }
        Some(proto::message_data::Body::UsernameProofBody(proof)) => {
            match UserNameType::try_from(proof.r#type) {
                Ok(UserNameType::UsernameTypeEnsL1) => {
                    if version.is_enabled(ProtocolFeature::EnsValidation) {
                        let name = &std::str::from_utf8(&proof.name)
                            .map_err(|_| ValidationError::InvalidData)?
                            .to_string();
                        validate_ens_name(name)?;
                    }
                }
                Ok(UserNameType::UsernameTypeBasename) => {
                    if version.is_enabled(ProtocolFeature::Basenames) {
                        let name = &std::str::from_utf8(&proof.name)
                            .map_err(|_| ValidationError::InvalidData)?
                            .to_string();
                        validate_base_name(name)?;
                    } else {
                        return Err(ValidationError::UnsupportedFeature);
                    }
                }
                _ => return Err(ValidationError::InvalidUsernameType),
            }
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
                is_pro_user,
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
        Some(proto::message_data::Body::FrameActionBody(frame_action_body)) => {
            validate_frame_action_body(&frame_action_body)?;
        }
        None => {}
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
        return Err(ValidationError::FnameIsMissing);
    }

    // FNAME_MAX_LENGTH - ".eth".length
    if input.len() > 16 {
        return Err(ValidationError::FnameExceedsLength(input.clone()));
    }

    if !Regex::new(FNAME_REGEX)
        .unwrap()
        .is_match(&input)
        .map_err(|_| ValidationError::InvalidData)?
    {
        return Err(ValidationError::FnameDoesntMatch(
            input.clone(),
            FNAME_REGEX.to_string(),
        ));
    }

    Ok(())
}

pub fn validate_ens_name(input: &String) -> Result<(), ValidationError> {
    if !input.ends_with(".eth") {
        return Err(ValidationError::EnsNameDoesntEndWith(
            input.clone(),
            ".eth".to_string(),
        ));
    }

    let name_parts: Vec<&str> = input.split('.').collect();

    if name_parts.len() > 2 {
        return Err(ValidationError::EnsNameUnsupportedSubdomain(input.clone()));
    }

    if name_parts.len() != 2 || name_parts[0].is_empty() {
        return Err(ValidationError::EnsNameNotValid(input.clone()));
    }

    if input.len() > 20 {
        return Err(ValidationError::EnsNameExceedsLength(input.clone()));
    }

    if !Regex::new(FNAME_REGEX)
        .unwrap()
        .is_match(name_parts[0])
        .map_err(|_| ValidationError::InvalidData)?
    {
        return Err(ValidationError::EnsNameDoesntMatch(
            input.clone(),
            FNAME_REGEX.to_string(),
        ));
    }

    Ok(())
}

pub fn validate_base_name(input: &String) -> Result<(), ValidationError> {
    if !input.ends_with(".base.eth") {
        return Err(ValidationError::EnsNameDoesntEndWith(
            input.clone(),
            ".base.eth".to_string(),
        ));
    }

    let name_parts: Vec<&str> = input.split('.').collect();
    if name_parts.len() != 3 || name_parts[0].is_empty() {
        return Err(ValidationError::InvalidData);
    }

    if input.len() > 25 {
        // 16 for fname + 9 for ".base.eth"
        return Err(ValidationError::EnsNameExceedsLength(input.clone()));
    }

    if !Regex::new(FNAME_REGEX)
        .unwrap()
        .is_match(&name_parts[0])
        .map_err(|_| ValidationError::InvalidData)?
    {
        return Err(ValidationError::EnsNameDoesntMatch(
            input.clone(),
            FNAME_REGEX.to_string(),
        ));
    }

    Ok(())
}

pub fn validate_twitter_username(input: &String) -> Result<(), ValidationError> {
    if input.len() > 15 {
        return Err(ValidationError::UsernameExceedsLength(input.clone(), 15));
    }

    if !Regex::new(TWITTER_USERNAME_REGEX)
        .unwrap()
        .is_match(&input)
        .map_err(|_| ValidationError::InvalidData)?
    {
        return Err(ValidationError::UsernameDoesntMatch(
            input.clone(),
            TWITTER_USERNAME_REGEX.to_string(),
        ));
    }

    Ok(())
}

pub fn validate_github_username(input: &String) -> Result<(), ValidationError> {
    if input.len() > 38 {
        return Err(ValidationError::UsernameExceedsLength(input.clone(), 38));
    }

    if !Regex::new(GITHUB_USERNAME_REGEX)
        .unwrap()
        .is_match(&input)
        .map_err(|_| ValidationError::InvalidData)?
    {
        return Err(ValidationError::UsernameDoesntMatch(
            input.clone(),
            GITHUB_USERNAME_REGEX.to_string(),
        ));
    }

    Ok(())
}

fn validate_number(value: &str) -> Result<f64, ValidationError> {
    return value
        .parse::<f64>()
        .map_err(|_| ValidationError::InvalidLocationString);
}

fn validate_latitude(value: &str) -> Result<(), ValidationError> {
    let number = validate_number(value)?;

    if number < -90.0 || number > 90.0 {
        return Err(ValidationError::LatitudeOutOfRange);
    }

    Ok(())
}

fn validate_longitude(value: &str) -> Result<(), ValidationError> {
    let number = validate_number(value)?;

    if number < -180.0 || number > 180.0 {
        return Err(ValidationError::LongitudeOutOfRange);
    }

    Ok(())
}

pub fn validate_user_data_primary_address_ethereum(input: &String) -> Result<(), ValidationError> {
    // Empty string is allowed for unsetting the primary address
    if input.is_empty() {
        return Ok(());
    }

    if !input.starts_with("0x") || input.len() != 42 {
        return Err(ValidationError::InvalidEthAddressLength);
    }

    let parsed = Address::from_hex(input).map_err(|_| ValidationError::InvalidData)?;
    verification::validate_eth_address(&parsed.to_vec())?;

    // Also check the checksum
    let checksummed = parsed.to_checksum(None);
    if checksummed != *input {
        return Err(ValidationError::InvalidData);
    }
    Ok(())
}

pub fn validate_user_data_primary_address_solana(input: &String) -> Result<(), ValidationError> {
    // Empty string is allowed for unsetting the primary address
    if input.is_empty() {
        return Ok(());
    }

    // validate the address is base58
    let decoded = bs58::decode(input)
        .into_vec()
        .map_err(|_| ValidationError::InvalidData)?;
    verification::validate_sol_address(&decoded)?;

    Ok(())
}

pub fn validate_user_location(location: &str) -> Result<(), ValidationError> {
    if location.is_empty() {
        return Ok(());
    }

    let captures = Regex::new(r"^geo:(-?\d{1,2}\.\d{2}),(-?\d{1,3}\.\d{2})$")
        .unwrap()
        .captures(location)
        .map_err(|_| ValidationError::InvalidLocationString)?;

    if captures.is_none() {
        return Err(ValidationError::InvalidLocationString);
    }

    let captured = captures.unwrap();

    let latitude = captured
        .get(1)
        .ok_or_else(|| ValidationError::InvalidLocationString)?;
    validate_latitude(latitude.as_str())?;

    let longitude = captured
        .get(2)
        .ok_or_else(|| ValidationError::InvalidLocationString)?;
    validate_longitude(longitude.as_str())?;

    Ok(())
}

pub fn validate_user_data_add_body(
    body: &UserDataBody,
    is_pro_user: bool,
    version: EngineVersion,
) -> Result<(), ValidationError> {
    let value_bytes = body.value.as_bytes();

    match UserDataType::try_from(body.r#type).map_err(|_| ValidationError::InvalidData)? {
        UserDataType::Pfp => {
            if value_bytes.len() > 256 {
                return Err(ValidationError::PfpValueTooLong);
            }
        }
        UserDataType::Banner => {
            if !is_pro_user {
                return Err(ValidationError::ProUserFeature);
            }
            if value_bytes.len() > 256 {
                return Err(ValidationError::BannerValueTooLong);
            }
        }
        UserDataType::Display => {
            if value_bytes.len() > 32 {
                return Err(ValidationError::DisplayValueTooLong);
            }
        }
        UserDataType::Bio => {
            if value_bytes.len() > 256 {
                return Err(ValidationError::BioValueTooLong);
            }
        }
        UserDataType::Url => {
            if value_bytes.len() > 256 {
                return Err(ValidationError::UrlValueTooLong);
            }
        }
        UserDataType::Username => {
            // Users are allowed to set fname = '' to remove their fname
            if !body.value.is_empty() {
                if body.value.ends_with(".base.eth") {
                    validate_base_name(&body.value)?;
                } else if body.value.ends_with(".eth") {
                    validate_ens_name(&body.value)?;
                } else {
                    validate_fname(&body.value)?;
                };
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
        UserDataType::UserDataPrimaryAddressEthereum => {
            if !version.is_enabled(ProtocolFeature::PrimaryAddresses) {
                return Err(ValidationError::UnsupportedFeature);
            }
            validate_user_data_primary_address_ethereum(&body.value)?;
        }
        UserDataType::UserDataPrimaryAddressSolana => {
            if !version.is_enabled(ProtocolFeature::PrimaryAddresses) {
                return Err(ValidationError::UnsupportedFeature);
            }
            validate_user_data_primary_address_solana(&body.value)?;
        }
        UserDataType::None => return Err(ValidationError::InvalidUserDataType),
    }

    Ok(())
}
