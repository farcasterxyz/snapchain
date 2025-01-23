use error::ValidationError;

use crate::proto::CastId;

pub mod cast;
pub mod error;
pub mod link;
pub mod message;
pub mod reaction;
pub mod verification;

#[cfg(test)]
mod message_test;

#[cfg(test)]
mod validations_test;

#[cfg(test)]
mod cast_test;

#[cfg(test)]
mod reaction_test;

#[cfg(test)]
mod link_test;

pub fn validate_fid(fid: u64) -> Result<(), ValidationError> {
    match fid {
        0 => Err(ValidationError::InvalidData),
        _f => Ok(()),
    }
}

pub fn validate_cast_id(cast_id: &CastId) -> Result<(), ValidationError> {
    validate_fid(cast_id.fid)?;

    if cast_id.hash.len() != 20 {
        return Err(ValidationError::InvalidData);
    }

    Ok(())
}

pub fn validate_url(url: &str) -> Result<(), ValidationError> {
    let url_bytes = url.as_bytes();

    if url_bytes.is_empty() {
        return Err(ValidationError::InvalidDataLength);
    }

    if url_bytes.len() > 256 {
        return Err(ValidationError::InvalidDataLength);
    }

    if !url.is_ascii() {
        return Err(ValidationError::InvalidData);
    }

    Ok(())
}
