use thiserror::Error;

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
    #[error("Protocol feature is not supported in this version")]
    UnsupportedFeature,
    #[error("Invalid data length")]
    InvalidDataLength,
    #[error("Unrecognized signature scheme")]
    InvalidSignatureScheme,
    #[error("Signer is empty or invalid")]
    MissingOrInvalidSigner,
    #[error("Signature is empty")]
    MissingSignature,
    #[error("Invalid message signature")]
    InvalidSignature,
    #[error("Invalid network")]
    InvalidNetwork,
    #[error("Invalid button index")]
    InvalidButtonIndex,
    #[error("Pro subscription required")]
    ProUserFeature,
}
