use crate::proto::{self, VerificationAddAddressBody};
use crate::storage::util::{blake3_20, bytes_compare};
use alloy_dyn_abi::TypedData;
use ed25519_dalek::{Signature, VerifyingKey};
use prost::Message;
use serde_json::{json, Value};
use thiserror::Error;

const MAX_DATA_BYTES: usize = 2048;
const EIP_712_FARCASTER_VERIFICATION_CLAIM_CHAIN_IDS: [u16; 5] = [0, 1, 5, 10, 420];

#[derive(Error, Debug, Clone, PartialEq)]
pub enum ValidationError {
    #[error("Message data is missing")]
    MissingData,
    #[error("Invalid message hash")]
    InvalidHash,
    #[error("Unrecognized hash scheme")]
    InvalidHashScheme,
    #[error("Message data invalid")]
    InvalidData,
    #[error("Message data too large")]
    InvalidDataLength,
    #[error("Unrecognized signature scheme")]
    InvalidSignatureScheme,
    #[error("Signer is empty or invalid")]
    MissingOrInvalidSigner,
    #[error("Signature is empty")]
    MissingSignature,
    #[error("Invalid message signature")]
    InvalidSignature,
}

pub fn validate_message(message: &proto::Message) -> Result<(), ValidationError> {
    let data_bytes;
    if message.data_bytes.is_some() {
        data_bytes = message.data_bytes.as_ref().unwrap().clone();
        if data_bytes.len() > MAX_DATA_BYTES {
            return Err(ValidationError::InvalidDataLength);
        }
    } else {
        if message.data.is_none() {
            return Err(ValidationError::MissingData);
        }
        data_bytes = message.data.as_ref().unwrap().encode_to_vec();
    }

    validate_message_hash(message.hash_scheme, &data_bytes, &message.hash)?;
    validate_signature(
        message.signature_scheme,
        &message.hash,
        &message.signature,
        &message.signer,
    )?;

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

fn eip_712_domain() -> Value {
    json!({
        "EIP712Domain": [
            {
                "name": "name",
                "type": "string"
            },
            {
                "name": "version",
                "type": "string"
            },
            {
                "name": "chainId",
                "type": "uint256"
            },
            {
                "name": "verifyingContract",
                "type": "address"
            }
        ],
        "UserNameProof": [
            { "name": "name", "type": "string" },
            { "name": "timestamp", "type": "uint256" },
            { "name": "owner", "type": "address" }
        ]
    })
}

fn name_registry_domain() -> Value {
    json!({
        "name": "Farcaster name verification",
        "version": "1",
        "chainId": 1,
        "verifyingContract": "0xe3be01d99baa8db9905b33a3ca391238234b79d1" // name registry contract, will be the farcaster ENS CCIP contract later
    })
}

pub fn validate_fname_transfer(transfer: &proto::FnameTransfer) -> Result<(), ValidationError> {
    let proof = transfer.proof.as_ref().unwrap();
    let username = std::str::from_utf8(&proof.name);
    if username.is_err() {
        return Err(ValidationError::MissingData);
    }

    let json = json!({
        "types": eip_712_domain(),
        "primaryType": "UserNameProof",
        "domain": name_registry_domain(),
        "message": {
            "name": username.unwrap(),
            "timestamp": proof.timestamp,
            "owner": hex::encode(proof.owner.clone())
        }
    });

    let typed_data = serde_json::from_value::<TypedData>(json);
    if typed_data.is_err() {
        return Err(ValidationError::InvalidData);
    }

    let data = typed_data.unwrap();
    let prehash = data.eip712_signing_hash();
    if prehash.is_err() {
        return Err(ValidationError::InvalidHash);
    }

    if proof.signature.len() != 65 {
        return Err(ValidationError::InvalidSignature);
    }

    let hash = prehash.unwrap();
    let fname_signer = alloy_primitives::address!("Bc5274eFc266311015793d89E9B591fa46294741");
    let signature = alloy_primitives::PrimitiveSignature::from_bytes_and_parity(
        &proof.signature[0..64],
        proof.signature[64] != 0x1b,
    );

    let recovered_address = signature.recover_address_from_prehash(&hash);
    if recovered_address.is_err() {
        return Err(ValidationError::InvalidSignature);
    }

    let recovered = recovered_address.unwrap();
    if recovered != fname_signer {
        return Err(ValidationError::InvalidSignature);
    }

    Ok(())
}

fn validate_eth_address(address: Vec<u8>) -> Result<(), ValidationError> {
    if address.len() == 0 {
        return Err(ValidationError::InvalidData);
    }

    if address.len() != 20 {
        return Err(ValidationError::InvalidDataLength);
    }

    Ok(())
}

fn validate_eth_block_hash(block_hash: Vec<u8>) -> Result<(), ValidationError> {
    if block_hash.len() == 0 {
        return Err(ValidationError::InvalidData);
    }

    if block_hash.len() != 32 {
        return Err(ValidationError::InvalidDataLength);
    }

    Ok(())
}

fn validate_verification_eoa_signature(
    body: &VerificationAddAddressBody,
) -> Result<(), ValidationError> {
    Ok(())
}

fn validate_verification_contract_signature() -> Result<(), ValidationError> {
    Ok(())
}

fn validate_verification_add_eth_address_signature(
    body: &proto::VerificationAddAddressBody,
    fid: u64,
    network: proto::FarcasterNetwork,
) -> Result<(), ValidationError> {
    if body.claim_signature.len() > 2048 {
        return Err(ValidationError::InvalidDataLength);
    }

    let chain_id = body.chain_id as u16;
    if !EIP_712_FARCASTER_VERIFICATION_CLAIM_CHAIN_IDS.contains(&chain_id) {
        return Err(ValidationError::InvalidData);
    }

    match body.verification_type {
        0 => validate_verification_eoa_signature(body),
        1 => validate_verification_contract_signature(),
        _ => Err(ValidationError::InvalidData),
    }
}

fn validate_add_eth_address(
    body: &proto::VerificationAddAddressBody,
    fid: u64,
    network: proto::FarcasterNetwork,
) -> Result<(), ValidationError> {
    let valid_address = validate_eth_address(body.address.clone());
    if valid_address.is_err() {
        return Err(valid_address.unwrap_err());
    }

    let valid_block_hash = validate_eth_block_hash(body.block_hash.clone());
    if valid_block_hash.is_err() {
        return Err(valid_block_hash.unwrap_err());
    }

    let valid_signature = validate_verification_add_eth_address_signature(body, fid, network);
    if valid_signature.is_err() {
        return Err(valid_signature.unwrap_err());
    }

    Ok(())
}

fn validate_add_sol_address(
    body: &proto::VerificationAddAddressBody,
) -> Result<(), ValidationError> {
    Ok(())
}

pub fn validate_add_address(
    body: &proto::VerificationAddAddressBody,
    fid: u64,
    network: proto::FarcasterNetwork,
) -> Result<(), ValidationError> {
    match body.protocol {
        x if x == proto::Protocol::Ethereum as i32 => validate_add_eth_address(body, fid, network),
        x if x == proto::Protocol::Solana as i32 => validate_add_sol_address(body),
        _ => Err(ValidationError::InvalidData),
    }
}

#[cfg(test)]
mod tests {
    use proto::{FnameTransfer, UserNameProof};

    use super::*;

    #[test]
    fn test_fname_transfer_verify_valid_signature() {
        let transfer = &FnameTransfer{
            id: 1,
            from_fid: 1,
            proof: Some(UserNameProof{
                timestamp: 1628882891,
                name: "farcaster".into(),
                owner: hex::decode("8773442740c17c9d0f0b87022c722f9a136206ed").unwrap(),
                signature: hex::decode("b7181760f14eda0028e0b647ff15f45235526ced3b4ae07fcce06141b73d32960d3253776e62f761363fb8137087192047763f4af838950a96f3885f3c2289c41b").unwrap(),
                fid: 1,
                r#type: 1,
            })
        };
        let result = validate_fname_transfer(transfer);
        assert!(result.is_ok());
    }

    #[test]
    fn test_fname_transfer_verify_wrong_address_for_signature_fails() {
        let transfer = &FnameTransfer{
            id: 1,
            from_fid: 1,
            proof: Some(UserNameProof{
                timestamp: 1628882891,
                name: "farcaster".into(),
                owner: hex::decode("8773442740c17c9d0f0b87022c722f9a136206ed").unwrap(),
                signature: hex::decode("a7181760f14eda0028e0b647ff15f45235526ced3b4ae07fcce06141b73d32960d3253776e62f761363fb8137087192047763f4af838950a96f3885f3c2289c41b").unwrap(),
                fid: 1,
                r#type: 1,
            })
        };
        let result = validate_fname_transfer(transfer);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ValidationError::InvalidSignature);
    }

    #[test]
    fn test_fname_transfer_verify_invalid_signature_fails() {
        let transfer = &FnameTransfer{
      id: 1,
      from_fid: 1,
      proof: Some(UserNameProof{
        timestamp: 1628882891,
        name: "farcaster".into(),
        owner: hex::decode("8773442740c17c9d0f0b87022c722f9a136206ed").unwrap(),
        signature: hex::decode("181760f14eda0028e0b647ff15f45235526ced3b4ae07fcce06141b73d32960d3253776e62f761363fb8137087192047763f4af838950a96f3885f3c2289c41b").unwrap(),
        fid: 1,
        r#type: 1,
      })
    };
        let result = validate_fname_transfer(transfer);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ValidationError::InvalidSignature);
    }
}
