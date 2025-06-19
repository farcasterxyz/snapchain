use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq)]
pub enum ValidationError {
    #[error("Message data is missing")]
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
    #[error("Invalid data length")]
    InvalidDataLength,
    #[error("invalid signatureScheme")]
    InvalidSignatureScheme,
    #[error("Signer is empty or invalid")]
    MissingOrInvalidSigner,
    #[error("Signature is empty")]
    MissingSignature,
    #[error("Invalid network")]
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
    #[error("ensName \"{0}\" doesn't end with .eth")]
    EnsNameDoesntEndWithEth(String),
    #[error("ensName \"{0}\" unsupported subdomain")]
    EnsNameUnsupportedSubdomain(String),
    #[error("ensName \"{0}\" is not a valid ENS name")]
    EnsNameNotValid(String),
    #[error("text > 1024 bytes for long cast")]
    TextTooLongForLongCast,
    #[error("text too short for long cast")]
    TextTooShortForLongCast,
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
    #[error("text is missing")]
    TextIsMissing,
    #[error("text > 320 bytes")]
    TextTooLong,
    #[error("embeds > 4")]
    EmbedsExceedsLimit,
    #[error("string embeds have been deprecated")]
    StringEmbedsDeprecated,
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
    #[error("mentionsPositions must be integers")]
    MentionsPositionsMustBeIntegers,
    #[error("mentionsPositions must be sorted in ascending order")]
    MentionsPositionsNotSorted,
    #[error("invalid reaction type")]
    InvalidReactionType,
    #[error("target is missing")]
    TargetIsMissing,
    #[error("cannot use both targetUrl and targetCastId")]
    CannotUseBothTargets,
    #[error("pfp value > 256")]
    PfpValueTooLong,
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
    #[error("username \"{0}\" > 15 characters")]
    UsernameExceedsLength(String),
    #[error("only one body can be set")]
    OnlyOneBodyCanBeSet,
    #[error("invalid length for eth address")]
    InvalidEthAddressLength,
    #[error("invalid length for sol address")]
    InvalidSolAddressLength,
}
