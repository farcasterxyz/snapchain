use crate::core::validations::error::ValidationError;
use crate::proto::cast_add_body::Parent;
use crate::proto::{embed, CastAddBody, CastRemoveBody, CastType, Embed};
use std::result::Result;

use super::{validate_cast_id, validate_fid, validate_url};

pub fn validate_cast_add_body(
    body: &CastAddBody,
    allow_embeds_deprecated: bool,
    is_pro_user: bool,
) -> Result<(), ValidationError> {
    let text_bytes = body.text.as_bytes();

    match CastType::try_from(body.r#type) {
        Ok(CastType::Cast) if text_bytes.len() > 320 => {
            return Err(ValidationError::InvalidDataLength);
        }
        Ok(CastType::LongCast) if text_bytes.len() > 1024 => {
            return Err(ValidationError::InvalidDataLength);
        }
        Ok(CastType::LongCast) if text_bytes.len() <= 320 => {
            return Err(ValidationError::InvalidDataLength);
        }
        Ok(CastType::TenKCast) if !is_pro_user => {
            return Err(ValidationError::ProUserFeature);
        }
        Ok(CastType::TenKCast) if text_bytes.len() > 10_000 => {
            return Err(ValidationError::InvalidDataLength);
        }
        Ok(CastType::TenKCast) if text_bytes.len() <= 1024 => {
            return Err(ValidationError::InvalidDataLength);
        }
        _ => {}
    }

    let num_embeds = if is_pro_user { 4 } else { 2 };
    if body.embeds.len() > num_embeds {
        return Err(ValidationError::InvalidData);
    }

    if allow_embeds_deprecated && body.embeds_deprecated.len() > 2 {
        return Err(ValidationError::InvalidData);
    }

    if !allow_embeds_deprecated && !body.embeds_deprecated.is_empty() {
        return Err(ValidationError::InvalidData);
    }

    if body.mentions.len() > 10 {
        return Err(ValidationError::InvalidData);
    }

    if body.mentions.len() != body.mentions_positions.len() {
        return Err(ValidationError::InvalidData);
    }

    if !body.embeds.is_empty() && !body.embeds_deprecated.is_empty() {
        return Err(ValidationError::InvalidData);
    }

    if body.text.is_empty()
        && body.embeds.is_empty()
        && body.embeds_deprecated.is_empty()
        && body.mentions.is_empty()
    {
        return Err(ValidationError::InvalidData);
    }

    for embed in &body.embeds {
        validate_embed(embed)?;
    }

    for embed_url in &body.embeds_deprecated {
        validate_url(embed_url)?;
    }

    let mut prev_position: i64 = -1;
    for (idx, &fid) in body.mentions.iter().enumerate() {
        validate_fid(fid)?;
        let position = body.mentions_positions[idx];

        if position > text_bytes.len() as u32 {
            return Err(ValidationError::InvalidData);
        }

        if idx > 0 && (position as i64) < prev_position {
            return Err(ValidationError::InvalidData);
        }
        prev_position = position as i64;
    }

    match &body.parent {
        Some(parent) => validate_parent(parent)?,
        None => {}
    }

    Ok(())
}

pub fn validate_cast_remove_body(body: &CastRemoveBody) -> Result<(), ValidationError> {
    if body.target_hash.len() != 20 {
        return Err(ValidationError::InvalidDataLength);
    }
    Ok(())
}

pub fn validate_embed(embed: &Embed) -> Result<(), ValidationError> {
    match &embed.embed {
        Some(embed_type) => match embed_type {
            embed::Embed::Url(url) => validate_url(&url),
            embed::Embed::CastId(cast_id) => validate_cast_id(&cast_id),
        },
        None => Err(ValidationError::InvalidData),
    }
}

pub fn validate_parent(parent: &Parent) -> Result<(), ValidationError> {
    match parent {
        Parent::ParentCastId(cast_id) => validate_cast_id(cast_id),
        Parent::ParentUrl(url) => validate_url(url),
    }
}
