use thiserror::Error;

use crate::core::error::HubError;

#[derive(Error, Debug, Clone, PartialEq)]
pub enum ValidationError {
    #[error("data is missing")]
    MissingData,
    #[error("invalid hash")]
    InvalidHash,
    #[error("invalid hashScheme")]
    InvalidHashScheme,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("Message data invalid")]
    InvalidData,
    #[error("Protocol feature is not supported in this version")]
    UnsupportedFeature,
    #[error("invalid signatureScheme")]
    InvalidSignatureScheme,
    #[error("signer is missing or invalid")]
    MissingOrInvalidSigner,
    #[error("signature is missing")]
    MissingSignature,
    #[error("invalid network")]
    InvalidNetwork,
    #[error("Invalid button index")]
    InvalidButtonIndex,
    #[error("Pro subscription required")]
    ProUserFeature,
    #[error("fname \"{0}\" > 16 characters")]
    FnameExceedsLength(String),
    #[error("fname \"{0}\" doesn't match {1}")]
    FnameDoesntMatch(String, String),
    #[error("ensName \"{0}\" doesn't match {1}")]
    EnsNameDoesntMatch(String, String),
    #[error("ensName \"{0}\" > 20 characters")]
    EnsNameExceedsLength(String),
    #[error("ensName \"{0}\" doesn't end with {1}")]
    EnsNameDoesntEndWith(String, String),
    #[error("ensName \"{0}\" unsupported subdomain")]
    EnsNameUnsupportedSubdomain(String),
    #[error("ensName \"{0}\" is not a valid ENS name")]
    EnsNameNotValid(String),
    #[error("text > 1024 bytes for long cast")]
    TextTooLongForLongCast,
    #[error("text too short for long cast")]
    TextTooShortForLongCast,
    #[error("text too short for 10k cast")]
    TextTooShortFor10kCast,
    #[error("text > 10,000 bytes for 10k cast")]
    TextTooLongFor10kCast,
    #[error("invalid cast type")]
    InvalidCastType,
    #[error("string embeds > 2")]
    StringEmbedsExceedsLimit,
    #[error("url < 1 byte")]
    UrlTooShort,
    #[error("url > 256 bytes")]
    UrlTooLong,
    #[error("cast is empty")]
    CastIsEmpty,
    #[error("text > 320 bytes")]
    TextTooLong,
    #[error("embeds > 4")]
    EmbedsExceedsLimit,
    #[error("string embeds have been deprecated")]
    StringEmbedsDeprecated,
    #[error("cannot use both embeds and string embeds")]
    InvalidEmbedsAndStringEmbeds,
    #[error("fid is missing")]
    FidIsMissing,
    #[error("fname is missing")]
    FnameIsMissing,
    #[error("hash is missing")]
    HashIsMissing,
    #[error("mentions > 10")]
    MentionsExceedsLimit,
    #[error("mentions and mentionsPositions must match")]
    MentionsMismatch,
    #[error("mentionsPositions must be a position in text")]
    MentionsPositionsInvalid,
    #[error("mentionsPositions must be sorted in ascending order")]
    MentionsPositionsNotSorted,
    #[error("invalid reaction type")]
    InvalidReactionType,
    #[error("target is missing")]
    TargetIsMissing,
    #[error("pfp value > 256")]
    PfpValueTooLong,
    #[error("banner value > 256")]
    BannerValueTooLong,
    #[error("display value > 32")]
    DisplayValueTooLong,
    #[error("bio value > 256")]
    BioValueTooLong,
    #[error("url value > 256")]
    UrlValueTooLong,
    #[error("Latitude value outside valid range")]
    LatitudeOutOfRange,
    #[error("Longitude value outside valid range")]
    LongitudeOutOfRange,
    #[error("Invalid location string")]
    InvalidLocationString,
    #[error("username \"{0}\" doesn't match {1}")]
    UsernameDoesntMatch(String, String),
    #[error("username \"{0}\" > {1} characters")]
    UsernameExceedsLength(String, u64),
    #[error("invalid length for eth address")]
    InvalidEthAddressLength,
    #[error("invalid length for sol address")]
    InvalidSolAddressLength,
    #[error("ethereum address is missing")]
    EthAddressMissing,
    #[error("solana address is missing")]
    SolAddressMissing,
    #[error("blockHash must be 32 bytes")]
    InvalidBlockhashLength,
    #[error("blockHash is missing")]
    BlockHashMissing,
    #[error("claimSignature > 2048 bytes")]
    InvalidEthClaimSignatureLength,
    #[error("claimSignature != 64 bytes")]
    InvalidSolClaimSignatureLength,
    #[error("invalid message type")]
    InvalidMessageType,
    #[error("invalid user data type")]
    InvalidUserDataType,
    #[error("invalid username type")]
    InvalidUsernameType,
    #[error("invalid claimSignature")]
    InvalidClaimSignature,
    #[error("type must be between 1-8 characters")]
    InvalidLinkType,
    #[error("invalid username")]
    InvalidUsername,
    #[error("value is missing")]
    MissingString,
    #[error("value is too long")]
    StringTooLong,
    #[error("dataBytes > {0} bytess")]
    DataBytesTooLong(u64),
    #[error("embed is missing")]
    MissingEmbed,
    #[error("timestamp more than 10 mins in the future")]
    TimestampTooFarInFuture,
    #[error(transparent)]
    HubError(#[from] HubError),
}
