use base64::prelude::*;
use hyper::{body::Bytes, Method};
use hyper::{Request, Response, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::future::Future;
use std::sync::Arc;
use tonic::async_trait;

use crate::proto::{FidRequest, Protocol};
use crate::{
    proto::{
        self, embed, hub_service_server::HubService, link_body::Target, message_data::Body,
        CastType, FarcasterNetwork, FidTimestampRequest, HashScheme, MessageType, ReactionType,
        SignatureScheme, UserDataType, UserNameType,
    },
    storage::store::engine::MessageValidationError,
};

use super::server::MyHubService;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Message {
    data: MessageData,
    hash: String,
    #[serde(rename = "hashScheme")]
    hash_scheme: String,
    signature: Vec<u8>,
    #[serde(rename = "signatureScheme")]
    signature_scheme: String,
    signer: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MessageData {
    #[serde(rename = "type")]
    pub message_type: String,
    pub fid: u64,
    pub timestamp: u32,
    pub network: String,
    #[serde(rename = "castAddBody", skip_serializing_if = "Option::is_none")]
    pub cast_add_body: Option<CastAddBody>,
    #[serde(rename = "castRemoveBody", skip_serializing_if = "Option::is_none")]
    pub cast_remove_body: Option<CastRemoveBody>,
    #[serde(rename = "reactionBody", skip_serializing_if = "Option::is_none")]
    pub reaction_body: Option<ReactionBody>,
    #[serde(
        rename = "verificationAddAddressBody",
        skip_serializing_if = "Option::is_none"
    )]
    pub verification_add_address_body: Option<VerificationAddAddressBody>,
    #[serde(
        rename = "verificationRemoveBody",
        skip_serializing_if = "Option::is_none"
    )]
    pub verification_remove_body: Option<VerificationRemoveBody>,
    #[serde(rename = "userDataBody", skip_serializing_if = "Option::is_none")]
    pub user_data_body: Option<UserDataBody>,
    #[serde(rename = "linkBody", skip_serializing_if = "Option::is_none")]
    pub link_body: Option<LinkBody>,
    #[serde(rename = "usernameProofBody", skip_serializing_if = "Option::is_none")]
    pub username_proof_body: Option<UsernameProofBody>,
    #[serde(rename = "frameActionBody", skip_serializing_if = "Option::is_none")]
    pub frame_action_body: Option<FrameActionBody>,
    #[serde(
        rename = "linkCompactStateBody",
        skip_serializing_if = "Option::is_none"
    )]
    pub link_compact_state_body: Option<LinkCompactStateBody>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
enum EmbedUrlOrCastId {
    Url(EmbedUrl),
    CastId(EmbedCastId),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct EmbedUrl {
    url: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct EmbedCastId {
    #[serde(rename = "castId")]
    cast_id: CastId,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CastAddBody {
    #[serde(rename = "embedsDeprecated")]
    embeds_deprecated: Vec<String>,
    mentions: Vec<u64>,
    #[serde(rename = "parentCastId")]
    parent_cast_id: Option<CastId>,
    #[serde(rename = "parentUrl")]
    parent_url: Option<String>,
    text: String,
    embeds: Vec<EmbedUrlOrCastId>,
    #[serde(rename = "mentionsPositions")]
    mentions_positions: Vec<u32>,
    #[serde(rename = "type")]
    cast_type: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CastRemoveBody {
    #[serde(rename = "targetHash")]
    target_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReactionBody {
    #[serde(rename = "type")]
    pub reaction_type: String,
    #[serde(rename = "targetCastId", skip_serializing_if = "Option::is_none")]
    pub target_cast_id: Option<CastId>,
    #[serde(rename = "targetUrl", skip_serializing_if = "Option::is_none")]
    pub target_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationAddAddressBody {
    pub address: String,
    #[serde(rename = "claimSignature")]
    pub claim_signature: String,
    #[serde(rename = "blockHash")]
    pub block_hash: String,
    #[serde(rename = "type")]
    pub verification_type: u32,
    #[serde(rename = "chainId")]
    pub chain_id: u32,
    pub protocol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationRemoveBody {
    pub address: String,
    pub protocol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDataBody {
    #[serde(rename = "type")]
    pub user_data_type: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkBody {
    #[serde(rename = "type")]
    pub link_type: String,
    #[serde(rename = "displayTimestamp")]
    pub display_timestamp: Option<u32>,
    #[serde(rename = "targetFid")]
    pub target_fid: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsernameProofBody {
    pub timestamp: u64,
    pub name: String,
    pub owner: String,
    pub signature: String,
    pub fid: u64,
    #[serde(rename = "type")]
    pub username_proof_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameActionBody {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkCompactStateBody {
    #[serde(rename = "type")]
    pub link_compact_type: String,
    #[serde(rename = "targetFids")]
    pub target_fids: Vec<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CastId {
    fid: u64,
    hash: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PagedResponse {
    messages: Vec<Message>,
    #[serde(rename = "nextPageToken", skip_serializing_if = "Option::is_none")]
    next_page_token: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IdRequest {
    pub fid: String,
    pub hash: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FidRequest {
    pub fid: u64,
    pub page_size: Option<u32>,
    pub page_token: Option<Vec<u8>>,
    pub reverse: Option<bool>,
    pub start_timestamp: Option<u64>,
    pub stop_timestamp: Option<u64>,
}

// Common error response
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub error_detail: Option<String>,
}

// Implementation struct
pub struct HubHttpServiceImpl {
    service: MyHubService,
}

fn map_proto_cast_add_body_to_json_cast_add_body(
    cast_add_body: proto::CastAddBody,
) -> Result<CastAddBody, ErrorResponse> {
    Ok(CastAddBody {
        embeds_deprecated: cast_add_body.embeds_deprecated,
        mentions: cast_add_body.mentions,
        parent_cast_id: cast_add_body.parent.clone().map_or_else(
            || None,
            |p| match p {
                proto::cast_add_body::Parent::ParentCastId(cast_id) => Some(CastId {
                    fid: cast_id.fid,
                    hash: format!("0x{}", hex::encode(cast_id.hash)),
                }),
                proto::cast_add_body::Parent::ParentUrl(_) => None,
            },
        ),
        parent_url: cast_add_body.parent.clone().map_or_else(
            || None,
            |p| match p {
                proto::cast_add_body::Parent::ParentCastId(_) => None,
                proto::cast_add_body::Parent::ParentUrl(url) => Some(url),
            },
        ),
        text: cast_add_body.text,
        embeds: cast_add_body
            .embeds
            .iter()
            .map(|e| match &e.embed {
                Some(embed::Embed::CastId(cast_id)) => EmbedUrlOrCastId::CastId(EmbedCastId {
                    cast_id: CastId {
                        fid: cast_id.fid,
                        hash: format!("0x{}", hex::encode(cast_id.hash.clone())),
                    },
                }),
                Some(embed::Embed::Url(url)) => EmbedUrlOrCastId::Url(EmbedUrl {
                    url: url.to_string(),
                }),
                None => EmbedUrlOrCastId::Url(EmbedUrl {
                    url: "".to_string(),
                }), // This arm never executes
            })
            .collect(),
        mentions_positions: cast_add_body.mentions_positions,
        cast_type: CastType::try_from(cast_add_body.r#type)
            .map(|t| t.as_str_name().to_string())
            .map_err(|_| ErrorResponse {
                error: "Invalid cast type".to_string(),
                error_detail: None,
            })?,
    })
}

fn map_proto_link_body_to_json_link_body(
    link_body: proto::LinkBody,
) -> Result<LinkBody, ErrorResponse> {
    Ok(LinkBody {
        link_type: link_body.r#type,
        display_timestamp: link_body.display_timestamp,
        target_fid: link_body.target.map_or_else(
            || {
                Err(ErrorResponse {
                    error: "Invalid link target".to_string(),
                    error_detail: None,
                })
            },
            |t| match t {
                Target::TargetFid(fid) => Ok(fid),
            },
        )?,
    })
}

fn map_proto_link_compact_body_to_json_link_compact_body(
    link_compact_body: proto::LinkCompactStateBody,
) -> Result<LinkCompactStateBody, ErrorResponse> {
    Ok(LinkCompactStateBody {
        link_compact_type: link_compact_body.r#type,
        target_fids: link_compact_body.target_fids,
    })
}

fn map_proto_reaction_body_to_json_reaction_body(
    reaction_body: proto::ReactionBody,
) -> Result<ReactionBody, ErrorResponse> {
    Ok(ReactionBody {
        reaction_type: ReactionType::try_from(reaction_body.r#type)
            .map_err(|_| ErrorResponse {
                error: "Invalid reaction type".to_string(),
                error_detail: None,
            })?
            .as_str_name()
            .to_owned(),
        target_cast_id: reaction_body.target.clone().map_or_else(
            || None,
            |t| match t {
                proto::reaction_body::Target::TargetCastId(cast_id) => Some(CastId {
                    fid: cast_id.fid,
                    hash: format!("0x{}", hex::encode(cast_id.hash)),
                }),
                proto::reaction_body::Target::TargetUrl(_) => None,
            },
        ),
        target_url: reaction_body.target.clone().map_or_else(
            || None,
            |t| match t {
                proto::reaction_body::Target::TargetCastId(_) => None,
                proto::reaction_body::Target::TargetUrl(url) => Some(url),
            },
        ),
    })
}

fn map_proto_user_data_body_to_json_user_data_body(
    user_data_body: proto::UserDataBody,
) -> Result<UserDataBody, ErrorResponse> {
    Ok(UserDataBody {
        user_data_type: UserDataType::try_from(user_data_body.r#type)
            .map_err(|_| ErrorResponse {
                error: "Invalid user data type".to_string(),
                error_detail: None,
            })?
            .as_str_name()
            .to_owned(),
        value: user_data_body.value,
    })
}

fn map_proto_username_proof_body_to_json_username_proof_body(
    username_proof_body: proto::UserNameProof,
) -> Result<UsernameProofBody, ErrorResponse> {
    Ok(UsernameProofBody {
        username_proof_type: UserNameType::try_from(username_proof_body.r#type)
            .map_err(|_| ErrorResponse {
                error: "Invalid username proof type".to_string(),
                error_detail: None,
            })?
            .as_str_name()
            .to_owned(),
        timestamp: username_proof_body.timestamp,
        name: std::str::from_utf8(&username_proof_body.name)
            .map_err(|_| ErrorResponse {
                error: "Invalid name".to_string(),
                error_detail: None,
            })?
            .to_string(),
        owner: format!("0x{}", hex::encode(username_proof_body.owner)),
        signature: BASE64_STANDARD.encode(username_proof_body.signature),
        fid: username_proof_body.fid,
    })
}

fn map_proto_verification_add_body_to_json_verification_add_body(
    verification_add_address_body: proto::VerificationAddAddressBody,
) -> Result<VerificationAddAddressBody, ErrorResponse> {
    Ok(VerificationAddAddressBody {
        address: if verification_add_address_body.protocol == 0 {
            format!("0x{}", hex::encode(verification_add_address_body.address))
        } else {
            bs58::encode(verification_add_address_body.address).into_string()
        },
        claim_signature: BASE64_STANDARD.encode(verification_add_address_body.claim_signature),
        block_hash: if verification_add_address_body.protocol == 0 {
            format!(
                "0x{}",
                hex::encode(verification_add_address_body.block_hash)
            )
        } else {
            bs58::encode(verification_add_address_body.block_hash).into_string()
        },
        verification_type: verification_add_address_body.verification_type,
        chain_id: verification_add_address_body.chain_id,
        protocol: Protocol::try_from(verification_add_address_body.protocol)
            .map_err(|_| ErrorResponse {
                error: "Invalid protocol type".to_string(),
                error_detail: None,
            })?
            .as_str_name()
            .to_owned(),
    })
}

fn map_proto_verification_remove_body_to_json_verification_remove_body(
    verification_remove_body: proto::VerificationRemoveBody,
) -> Result<VerificationRemoveBody, ErrorResponse> {
    Ok(VerificationRemoveBody {
        address: if verification_remove_body.protocol == 0 {
            format!("0x{}", hex::encode(verification_remove_body.address))
        } else {
            bs58::encode(verification_remove_body.address).into_string()
        },
        protocol: Protocol::try_from(verification_remove_body.protocol)
            .map_err(|_| ErrorResponse {
                error: "Invalid protocol type".to_string(),
                error_detail: None,
            })?
            .as_str_name()
            .to_owned(),
    })
}

fn map_proto_message_data_to_json_message_data(
    message_data: proto::MessageData,
) -> Result<MessageData, ErrorResponse> {
    match message_data.body {
        Some(Body::CastAddBody(cast_add_body)) => {
            let result = map_proto_cast_add_body_to_json_cast_add_body(cast_add_body)?;
            Ok(MessageData {
                message_type: MessageType::try_from(message_data.r#type)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid message type".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                fid: message_data.fid,
                network: FarcasterNetwork::try_from(message_data.network)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid network".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                timestamp: message_data.timestamp,
                cast_add_body: Some(result),
                cast_remove_body: None,
                reaction_body: None,
                verification_add_address_body: None,
                verification_remove_body: None,
                user_data_body: None,
                link_body: None,
                username_proof_body: None,
                frame_action_body: None,
                link_compact_state_body: None,
            })
        }
        Some(Body::CastRemoveBody(cast_remove_body)) => Ok(MessageData {
            message_type: MessageType::try_from(message_data.r#type)
                .map_err(|_| ErrorResponse {
                    error: "Invalid message type".to_string(),
                    error_detail: None,
                })?
                .as_str_name()
                .to_owned(),
            fid: message_data.fid,
            network: FarcasterNetwork::try_from(message_data.network)
                .map_err(|_| ErrorResponse {
                    error: "Invalid network".to_string(),
                    error_detail: None,
                })?
                .as_str_name()
                .to_owned(),
            timestamp: message_data.timestamp,
            cast_add_body: None,
            cast_remove_body: Some(CastRemoveBody {
                target_hash: format!("0x{}", hex::encode(cast_remove_body.target_hash)),
            }),
            reaction_body: None,
            verification_add_address_body: None,
            verification_remove_body: None,
            user_data_body: None,
            link_body: None,
            username_proof_body: None,
            frame_action_body: None,
            link_compact_state_body: None,
        }),
        Some(Body::FrameActionBody(_)) => Err(ErrorResponse {
            error: "No message data".to_string(),
            error_detail: None,
        }),
        Some(Body::LinkBody(link_body)) => {
            let result = map_proto_link_body_to_json_link_body(link_body)?;
            return Ok(MessageData {
                message_type: MessageType::try_from(message_data.r#type)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid message type".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                fid: message_data.fid,
                network: FarcasterNetwork::try_from(message_data.network)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid network".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                timestamp: message_data.timestamp,
                cast_add_body: None,
                cast_remove_body: None,
                reaction_body: None,
                verification_add_address_body: None,
                verification_remove_body: None,
                user_data_body: None,
                link_body: Some(result),
                username_proof_body: None,
                frame_action_body: None,
                link_compact_state_body: None,
            });
        }
        Some(Body::LinkCompactStateBody(link_compact_state_body)) => {
            let result =
                map_proto_link_compact_body_to_json_link_compact_body(link_compact_state_body)?;
            return Ok(MessageData {
                message_type: MessageType::try_from(message_data.r#type)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid message type".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                fid: message_data.fid,
                network: FarcasterNetwork::try_from(message_data.network)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid network".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                timestamp: message_data.timestamp,
                cast_add_body: None,
                cast_remove_body: None,
                reaction_body: None,
                verification_add_address_body: None,
                verification_remove_body: None,
                user_data_body: None,
                link_body: None,
                username_proof_body: None,
                frame_action_body: None,
                link_compact_state_body: Some(result),
            });
        }
        Some(Body::ReactionBody(reaction_body)) => {
            let result = map_proto_reaction_body_to_json_reaction_body(reaction_body)?;
            return Ok(MessageData {
                message_type: MessageType::try_from(message_data.r#type)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid message type".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                fid: message_data.fid,
                network: FarcasterNetwork::try_from(message_data.network)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid network".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                timestamp: message_data.timestamp,
                cast_add_body: None,
                cast_remove_body: None,
                reaction_body: None,
                verification_add_address_body: None,
                verification_remove_body: None,
                user_data_body: None,
                link_body: None,
                username_proof_body: None,
                frame_action_body: None,
                link_compact_state_body: None,
            });
        }
        Some(Body::UserDataBody(user_data_body)) => {
            let result = map_proto_user_data_body_to_json_user_data_body(user_data_body)?;
            return Ok(MessageData {
                message_type: MessageType::try_from(message_data.r#type)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid message type".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                fid: message_data.fid,
                network: FarcasterNetwork::try_from(message_data.network)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid network".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                timestamp: message_data.timestamp,
                cast_add_body: None,
                cast_remove_body: None,
                reaction_body: None,
                verification_add_address_body: None,
                verification_remove_body: None,
                user_data_body: Some(result),
                link_body: None,
                username_proof_body: None,
                frame_action_body: None,
                link_compact_state_body: None,
            });
        }
        Some(Body::UsernameProofBody(username_proof_body)) => {
            let result =
                map_proto_username_proof_body_to_json_username_proof_body(username_proof_body)?;
            return Ok(MessageData {
                message_type: MessageType::try_from(message_data.r#type)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid message type".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                fid: message_data.fid,
                network: FarcasterNetwork::try_from(message_data.network)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid network".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                timestamp: message_data.timestamp,
                cast_add_body: None,
                cast_remove_body: None,
                reaction_body: None,
                verification_add_address_body: None,
                verification_remove_body: None,
                user_data_body: None,
                link_body: None,
                username_proof_body: Some(result),
                frame_action_body: None,
                link_compact_state_body: None,
            });
        }
        Some(Body::VerificationAddAddressBody(verification_add_address_body)) => {
            let result = map_proto_verification_add_body_to_json_verification_add_body(
                verification_add_address_body,
            )?;
            return Ok(MessageData {
                message_type: MessageType::try_from(message_data.r#type)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid message type".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                fid: message_data.fid,
                network: FarcasterNetwork::try_from(message_data.network)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid network".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                timestamp: message_data.timestamp,
                cast_add_body: None,
                cast_remove_body: None,
                reaction_body: None,
                verification_add_address_body: Some(result),
                verification_remove_body: None,
                user_data_body: None,
                link_body: None,
                username_proof_body: None,
                frame_action_body: None,
                link_compact_state_body: None,
            });
        }
        Some(Body::VerificationRemoveBody(verification_remove_body)) => {
            let result = map_proto_verification_remove_body_to_json_verification_remove_body(
                verification_remove_body,
            )?;
            return Ok(MessageData {
                message_type: MessageType::try_from(message_data.r#type)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid message type".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                fid: message_data.fid,
                network: FarcasterNetwork::try_from(message_data.network)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid network".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                timestamp: message_data.timestamp,
                cast_add_body: None,
                cast_remove_body: None,
                reaction_body: None,
                verification_add_address_body: None,
                verification_remove_body: Some(result),
                user_data_body: None,
                link_body: None,
                username_proof_body: None,
                frame_action_body: None,
                link_compact_state_body: None,
            });
        }
        None => Err(ErrorResponse {
            error: "No message data".to_string(),
            error_detail: None,
        }),
    }
}

fn map_proto_message_to_json_message(message: proto::Message) -> Result<Message, ErrorResponse> {
    match message.data {
        Some(message_data) => {
            let result = map_proto_message_data_to_json_message_data(message_data)?;
            Ok(Message {
                data: result,
                hash: format!("0x{}", hex::encode(message.hash)),
                hash_scheme: HashScheme::try_from(message.hash_scheme)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid hash scheme".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                signature: message.signature,
                signature_scheme: SignatureScheme::try_from(message.signature_scheme)
                    .map_err(|_| ErrorResponse {
                        error: "Invalid signature scheme".to_string(),
                        error_detail: None,
                    })?
                    .as_str_name()
                    .to_owned(),
                signer: format!("0x{}", hex::encode(message.signer)),
            })
        }
        None => Err(ErrorResponse {
            error: "No message data".to_string(),
            error_detail: None,
        }),
    }
}

fn map_proto_messages_response_to_json_paged_response(
    messages_response: proto::MessagesResponse,
) -> Result<PagedResponse, ErrorResponse> {
    Ok(PagedResponse {
        messages: messages_response
            .messages
            .iter()
            .map(|m| map_proto_message_to_json_message(m)?)
            .collect(),
        next_page_token: messages_response.next_page_token,
    })
}

// Service trait for type-safe request handling
#[async_trait]
pub trait HubHttpService {
    async fn get_cast_by_id(&self, req: IdRequest) -> Result<Message, ErrorResponse>;
    async fn get_casts_by_fid(&self, req: FidRequest) -> Result<PagedResponse, ErrorResponse>;
    async fn get_casts_by_mention(&self, req: FidRequest) -> Result<PagedResponse, ErrorResponse>;
    async fn get_reaction_by_id(&self, req: IdRequest) -> Result<Message, ErrorResponse>;
    async fn get_reactions_by_fid(&self, req: FidRequest) -> Result<PagedResponse, ErrorResponse>;
    async fn get_reactions_by_cast(
        &self,
        req: ReactionsByTargetRequest,
    ) -> Result<PagedResponse, ErrorResponse>;
    async fn get_reactions_by_target(
        &self,
        req: ReactionsByTargetRequest,
    ) -> Result<PagedResponse, ErrorResponse>;
    async fn get_link_by_id(&self, req: IdRequest) -> Result<Message, ErrorResponse>;
    async fn get_links_by_fid(&self, req: FidRequest) -> Result<PagedResponse, ErrorResponse>;
    async fn get_links_by_target_fid(
        &self,
        req: FidTimestampRequest,
    ) -> Result<PagedResponse, ErrorResponse>;
    async fn get_user_data_by_fid(&self, req: FidRequest) -> Result<PagedResponse, ErrorResponse>;
    async fn get_storage_limits_by_fid(
        &self,
        req: FidRequest,
    ) -> Result<StorageLimitsResponse, ErrorResponse>;
    async fn get_user_name_proof_by_name(
        &self,
        req: UsernameProofRequest,
    ) -> Result<UserNameProof, ErrorResponse>;
    async fn get_user_name_proofs_by_fid(
        &self,
        req: FidRequest,
    ) -> Result<UsernameProofsResponse, ErrorResponse>;
    async fn get_verifications_by_fid(
        &self,
        req: FidRequest,
    ) -> Result<PagedResponse, ErrorResponse>;
    async fn get_on_chain_signers_by_fid(
        &self,
        req: FidRequest,
    ) -> Result<OnChainEventResponse, ErrorResponse>;
    async fn get_on_chain_events_by_fid(
        &self,
        req: OnChainEventRequest,
    ) -> Result<OnChainEventResponse, ErrorResponse>;
}

#[async_trait]
impl HubHttpService for HubHttpServiceImpl {
    async fn get_cast_by_id(&self, req: IdRequest) -> Result<Message, ErrorResponse> {
        let fid = req.fid.parse::<u64>().map_err(|e| ErrorResponse {
            error: "Invalid fid".to_string(),
            error_detail: Some(e.to_string()),
        })?;

        let hash = hex::decode(&req.hash).map_err(|e| ErrorResponse {
            error: "Invalid hash".to_string(),
            error_detail: Some(e.to_string()),
        })?;

        let service = &self.service;
        let response = service
            .get_cast(tonic::Request::<proto::CastId>::new(proto::CastId {
                fid,
                hash: hash.into(),
            }))
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get cast".to_string(),
                error_detail: Some(e.to_string()),
            })?;

        let message = response.into_inner();
        return map_proto_message_to_json_message(message);
    }

    async fn get_casts_by_fid(&self, req: FidRequest) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let response = service
            .get_all_cast_messages_by_fid(tonic::Request::<FidTimestampRequest>::new(
                FidTimestampRequest {
                    fid: req.fid,
                    page_size: req.page_size,
                    page_token: req.page_token,
                    reverse: req.reverse,
                    start_timestamp: req.start_timestamp,
                    stop_timestamp: req.stop_timestamp,
                },
            ))
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get casts".to_string(),
                error_detail: Some(e.to_string()),
            })?;

        let response_body = response.into_inner();
        return map_proto_messages_response_to_json_paged_response(response_body);
    }

    async fn get_casts_by_mention(&self, req: FidRequest) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;

        let grpc_req = tonic::Request::new(proto::FidRequest {
            fid: req.fid,
            page_size: req.page_size,
            page_token: req.page_token,
            reverse: req.reverse,
        });
        let response = service
            .get_casts_by_mention(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get casts by mention".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto_resp = response.into_inner();
        map_proto_messages_response_to_json_paged_response(proto_resp)
    }

    async fn get_reaction_by_id(&self, req: IdRequest) -> Result<Message, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_reaction(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get reaction".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto_msg = response.into_inner();
        map_proto_message_to_json_message(proto_msg)
    }

    /// GET /v1/reactionsByFid
    async fn get_reactions_by_fid(&self, req: FidRequest) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_reactions_by_fid(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get reactions by fid".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto_resp = response.into_inner();
        map_proto_messages_response_to_json_paged_response(proto_resp)
    }

    /// GET /v1/reactionsByCast
    async fn get_reactions_by_cast(
        &self,
        req: ReactionsByTargetRequest,
    ) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response =
            service
                .get_reactions_by_cast(grpc_req)
                .await
                .map_err(|e| ErrorResponse {
                    error: "Failed to get reactions by cast".to_string(),
                    error_detail: Some(e.to_string()),
                })?;
        let proto_resp = response.into_inner();
        map_proto_messages_response_to_json_paged_response(proto_resp)
    }

    /// GET /v1/reactionsByTarget
    async fn get_reactions_by_target(
        &self,
        req: ReactionsByTargetRequest,
    ) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_reactions_by_target(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get reactions by target".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto_resp = response.into_inner();
        map_proto_messages_response_to_json_paged_response(proto_resp)
    }

    /// GET /v1/linkById
    async fn get_link_by_id(&self, req: IdRequest) -> Result<Message, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_link(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get link".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto_msg = response.into_inner();
        map_proto_message_to_json_message(proto_msg)
    }

    /// GET /v1/linksByFid
    async fn get_links_by_fid(&self, req: FidRequest) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_links_by_fid(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get links by fid".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto_resp = response.into_inner();
        map_proto_messages_response_to_json_paged_response(proto_resp)
    }

    /// GET /v1/linksByTargetFid
    /// (Assumes that this endpoint uses FidTimestampRequest to retrieve all link messages for a given target fid.)
    async fn get_links_by_target_fid(
        &self,
        req: FidTimestampRequest,
    ) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_all_link_messages_by_fid(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get links by target fid".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto_resp = response.into_inner();
        map_proto_messages_response_to_json_paged_response(proto_resp)
    }

    /// GET /v1/userDataByFid
    async fn get_user_data_by_fid(&self, req: FidRequest) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_user_data_by_fid(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get user data by fid".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto_resp = response.into_inner();
        map_proto_messages_response_to_json_paged_response(proto_resp)
    }

    /// GET /v1/storageLimitsByFid
    async fn get_storage_limits_by_fid(
        &self,
        req: FidRequest,
    ) -> Result<StorageLimitsResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_current_storage_limits_by_fid(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get storage limits".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        Ok(response.into_inner())
    }

    /// GET /v1/userNameProofByName
    async fn get_user_name_proof_by_name(
        &self,
        req: UsernameProofRequest,
    ) -> Result<UserNameProof, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_username_proof(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get username proof".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        Ok(response.into_inner())
    }

    /// GET /v1/userNameProofsByFid
    async fn get_user_name_proofs_by_fid(
        &self,
        req: FidRequest,
    ) -> Result<UsernameProofsResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_user_name_proofs_by_fid(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get username proofs".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        Ok(response.into_inner())
    }

    /// GET /v1/verificationsByFid
    async fn get_verifications_by_fid(
        &self,
        req: FidRequest,
    ) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_verifications_by_fid(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get verifications by fid".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto_resp = response.into_inner();
        map_proto_messages_response_to_json_paged_response(proto_resp)
    }

    /// GET /v1/onChainSignersByFid
    async fn get_on_chain_signers_by_fid(
        &self,
        req: FidRequest,
    ) -> Result<OnChainEventResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_on_chain_signers_by_fid(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get on chain signers".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        Ok(response.into_inner())
    }

    /// GET /v1/onChainEventsByFid
    async fn get_on_chain_events_by_fid(
        &self,
        req: OnChainEventRequest,
    ) -> Result<OnChainEventResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .get_on_chain_events(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get on chain events".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        Ok(response.into_inner())
    }
}

// Router implementation
pub struct Router {
    service: Arc<HubHttpServiceImpl>,
}

impl Router {
    pub fn new(service: HubHttpServiceImpl) -> Self {
        Self {
            service: Arc::new(service),
        }
    }

    pub async fn handle(&self, req: Request<Bytes>) -> Response<Bytes> {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/v1/castById") => {
                self.handle_request::<IdRequest, Message, _>(req, |service, req| {
                    Box::pin(async move { service.get_cast_by_id(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/castsByFid") => {
                self.handle_request::<FidRequest, PagedResponse, _>(req, move |service, req| {
                    Box::pin(async move { service.get_casts_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/castsByMention") => {
                self.handle_request::<FidRequest, PagedResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_casts_by_mention(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/reactionById") => {
                self.handle_request::<IdRequest, Message, _>(req, |service, req| {
                    Box::pin(async move { service.get_reaction(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/reactionsByFid") => {
                self.handle_request::<FidRequest, PagedResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_reactions_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/reactionsByCast") => {
                self.handle_request::<ReactionsByTargetRequest, PagedResponse, _>(
                    req,
                    |service, req| {
                        Box::pin(async move { service.get_reactions_by_cast(req).await })
                    },
                )
                .await
            }
            (&Method::GET, "/v1/reactionsByTarget") => {
                self.handle_request::<ReactionsByTargetRequest, PagedResponse, _>(
                    req,
                    |service, req| {
                        Box::pin(async move { service.get_reactions_by_target(req).await })
                    },
                )
                .await
            }
            (&Method::GET, "/v1/linkById") => {
                self.handle_request::<IdRequest, Message, _>(req, |service, req| {
                    Box::pin(async move { service.get_link(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/linksByFid") => {
                self.handle_request::<FidRequest, PagedResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_links_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/linksByTargetFid") => {
                // For linksByTargetFid we assume that the service uses a FidTimestampRequest
                // (similar to castsByFid) to return all link messages for a target fid.
                self.handle_request::<FidTimestampRequest, PagedResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_all_link_messages_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/userDataByFid") => {
                self.handle_request::<FidRequest, PagedResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_user_data_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/storageLimitsByFid") => {
                self.handle_request::<FidRequest, StorageLimitsResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_current_storage_limits_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/userNameProofByName") => {
                self.handle_request::<UsernameProofRequest, UserNameProof, _>(
                    req,
                    |service, req| Box::pin(async move { service.get_username_proof(req).await }),
                )
                .await
            }
            (&Method::GET, "/v1/userNameProofsByFid") => {
                self.handle_request::<FidRequest, UsernameProofsResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_user_name_proofs_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/verificationsByFid") => {
                self.handle_request::<FidRequest, PagedResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_verifications_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/onChainSignersByFid") => {
                self.handle_request::<FidRequest, OnChainEventResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_on_chain_signers_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/onChainEventsByFid") => {
                self.handle_request::<OnChainEventRequest, OnChainEventResponse, _>(
                    req,
                    |service, req| Box::pin(async move { service.get_on_chain_events(req).await }),
                )
                .await
            }
            _ => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Bytes::from("Not Found"))
                .unwrap(),
        }
    }

    async fn handle_request<Req, Resp, F>(
        &self,
        req: Request<Bytes>,
        handler: impl FnOnce(Arc<HubHttpServiceImpl>, Req) -> F,
    ) -> Response<Bytes>
    where
        Req: DeserializeOwned,
        Resp: Serialize,
        F: Future<Output = Result<Resp, ErrorResponse>>,
    {
        // Parse request
        let req_obj = match self.parse_request::<Req>(req).await {
            Ok(req) => req,
            Err(resp) => return resp,
        };

        // Handle request
        match handler(self.service.clone(), req_obj).await {
            Ok(resp) => Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Bytes::from(serde_json::to_vec(&resp).unwrap()))
                .unwrap(),
            Err(err) => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(Bytes::from(serde_json::to_vec(&err).unwrap()))
                .unwrap(),
        }
    }

    async fn parse_request<T: DeserializeOwned>(
        &self,
        req: Request<Bytes>,
    ) -> Result<T, Response<Bytes>> {
        // For GET requests, parse from query string
        if req.method() == Method::GET {
            let query = req.uri().query().unwrap_or("");
            return serde_qs::from_str(query).map_err(|e| {
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Bytes::from(format!("Invalid query parameters: {}", e)))
                    .unwrap()
            });
        }

        // For POST/PUT requests, parse body
        let body_bytes = req.into_body();

        match serde_json::from_slice(&body_bytes.slice(..)) {
            Ok(parsed) => Ok(parsed),
            Err(e) => Err(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Bytes::from(format!("Invalid request format: {}", e)))
                .unwrap()),
        }
    }
}
