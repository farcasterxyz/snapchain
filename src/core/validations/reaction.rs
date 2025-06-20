use crate::core::validations::error::ValidationError;
use crate::proto::reaction_body::Target;
use crate::proto::{FarcasterNetwork, ReactionBody, ReactionType};

use super::{validate_cast_id, validate_url};

pub fn validate_reaction_type(type_num: i32) -> Result<(), ValidationError> {
    ReactionType::try_from(type_num)
        .map_or_else(|_| Err(ValidationError::InvalidReactionType), |_| Ok(()))
}

pub fn validate_network(network: i32) -> Result<(), ValidationError> {
    FarcasterNetwork::try_from(network)
        .map_or_else(|_| Err(ValidationError::InvalidNetwork), |_| Ok(()))
}

pub fn validate_target(target: &Target) -> Result<(), ValidationError> {
    match target {
        Target::TargetUrl(url) => validate_url(url),
        Target::TargetCastId(cast_id) => validate_cast_id(cast_id),
    }
}

pub fn validate_reaction_body(body: &ReactionBody) -> Result<(), ValidationError> {
    validate_reaction_type(body.r#type)?;

    match &body.target {
        Some(target) => validate_target(&target),
        None => Err(ValidationError::TargetIsMissing),
    }
}
