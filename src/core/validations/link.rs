use crate::proto::{link_body::Target, LinkBody, LinkCompactStateBody};

use super::{error::ValidationError, validate_fid};

pub fn validate_link_type(type_str: &str) -> Result<(), ValidationError> {
    if type_str.is_empty() || type_str.len() > 8 {
        return Err(ValidationError::InvalidLinkType);
    }
    Ok(())
}

pub fn validate_link_compact_state_body(
    body: &LinkCompactStateBody,
) -> Result<(), ValidationError> {
    validate_link_type(&body.r#type)?;

    if body.target_fids.is_empty() {
        return Err(ValidationError::TargetIsMissing);
    }

    for &fid in &body.target_fids {
        validate_fid(fid)?;
    }

    Ok(())
}

pub fn validate_target(target: &Target) -> Result<(), ValidationError> {
    match target {
        Target::TargetFid(fid) => validate_fid(*fid),
    }
}

pub fn validate_link_body(body: &LinkBody) -> Result<(), ValidationError> {
    validate_link_type(&body.r#type)?;

    if let Some(target) = &body.target {
        match target {
            Target::TargetFid(target_fid) => validate_target(&Target::TargetFid(*target_fid))?,
        }
    } else {
        return Err(ValidationError::TargetIsMissing);
    }

    Ok(())
}
