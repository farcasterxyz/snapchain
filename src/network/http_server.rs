use base64::prelude::*;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::header::HeaderValue;
use hyper::{body::Bytes, Method};
use hyper::{HeaderMap, Request, Response, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::convert::Infallible;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use tonic::async_trait;
use tonic::metadata::MetadataValue;

use crate::proto::{
    self, embed, hub_service_server::HubService, link_body::Target, message_data::Body, CastType,
    FarcasterNetwork, HashScheme, MessageType, ReactionType, SignatureScheme, UserDataType,
    UserNameType,
};
use crate::proto::{
    casts_by_parent_request, hub_event, link_request, links_by_target_request, on_chain_event,
    reaction_request, reactions_by_target_request, Protocol,
};
use crate::storage::store::account::message_decode;

use super::server::MyHubService;

mod serdebase64 {
    use base64::prelude::*;

    use serde::{Deserialize, Serialize};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        let base64 = BASE64_STANDARD.encode(v);
        String::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let base64 = String::deserialize(d)?;
        BASE64_STANDARD
            .decode(base64.as_bytes())
            .map_err(|e| serde::de::Error::custom(e))
    }
}

mod serdebase64opt {
    use base64::prelude::*;

    use serde::{Deserialize, Serialize};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        let default = vec![];
        let v = v.as_ref().unwrap_or(&default);
        let base64 = BASE64_STANDARD.encode(v);
        String::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        let base64 = String::deserialize(d)?.replace(" ", "+");
        if base64.len() == 0 {
            Ok(None)
        } else {
            let decoded = BASE64_STANDARD
                .decode(base64.as_bytes())
                .map_err(|e| serde::de::Error::custom(e))?;
            Ok(Some(decoded))
        }
    }
}

mod serdehex {
    use hex;
    use serde::Deserialize;
    use serde::{de::Error, Deserializer, Serializer};
    /// Serialize a byte vector to a "0x"‑prefixed hex string.
    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        // hex::encode turns &[u8] → lowercase hex (no prefix)
        let mut prefixed = String::with_capacity(v.len() * 2 + 2);
        prefixed.push_str("0x");
        prefixed.push_str(&hex::encode(v));
        s.serialize_str(&prefixed)
    }
    /// Deserialize from either a "0x"‑prefixed or raw hex string into Vec<u8>.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        // strip optional "0x"
        let hex_str = s.strip_prefix("0x").unwrap_or(&s);
        hex::decode(hex_str).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message {
    pub data: MessageData,
    pub hash: String,
    #[serde(rename = "hashScheme")]
    pub hash_scheme: String,
    #[serde(with = "serdebase64")]
    pub signature: Vec<u8>,
    #[serde(rename = "signatureScheme")]
    pub signature_scheme: String,
    pub signer: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MessageData {
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
pub enum EmbedUrlOrCastId {
    Url(EmbedUrl),
    CastId(EmbedCastId),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EmbedUrl {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EmbedCastId {
    #[serde(rename = "castId")]
    pub cast_id: CastId,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CastAddBody {
    #[serde(rename = "embedsDeprecated")]
    pub embeds_deprecated: Vec<String>,
    pub mentions: Vec<u64>,
    #[serde(rename = "parentCastId")]
    pub parent_cast_id: Option<CastId>,
    #[serde(rename = "parentUrl")]
    pub parent_url: Option<String>,
    pub text: String,
    pub embeds: Vec<EmbedUrlOrCastId>,
    #[serde(rename = "mentionsPositions")]
    pub mentions_positions: Vec<u32>,
    #[serde(rename = "type")]
    pub cast_type: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CastRemoveBody {
    #[serde(rename = "targetHash", with = "serdebase64")]
    pub target_hash: Vec<u8>,
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
    #[serde(rename = "claimSignature", with = "serdebase64")]
    pub claim_signature: Vec<u8>,
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
    #[serde(with = "serdebase64")]
    pub signature: Vec<u8>,
    pub fid: u64,
    #[serde(rename = "type")]
    pub username_proof_type: String,
}

// Serialize as base64 strings for compatibility with hubs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameActionBody {
    #[serde(with = "serdebase64")]
    pub url: Vec<u8>,
    #[serde(rename = "buttonIndex")]
    pub button_index: u32,
    #[serde(rename = "castId", skip_serializing_if = "Option::is_none")]
    pub cast_id: Option<CastId>,
    #[serde(with = "serdebase64", rename = "inputText")]
    pub input_text: Vec<u8>,
    #[serde(with = "serdebase64")]
    pub state: Vec<u8>,
    #[serde(with = "serdebase64", rename = "transactionId")]
    pub transaction_id: Vec<u8>,
    #[serde(with = "serdehex")]
    pub address: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkCompactStateBody {
    #[serde(rename = "type")]
    pub link_compact_type: String,
    #[serde(rename = "targetFids")]
    pub target_fids: Vec<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CastId {
    pub fid: u64,
    pub hash: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PagedResponse {
    pub messages: Vec<Message>,
    #[serde(rename = "nextPageToken", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IdRequest {
    pub fid: String,
    pub hash: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InfoRequest {} // Doesn't take dbstats not sure if issue

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InfoResponse {
    #[serde(rename = "dbStats", skip_serializing_if = "Option::is_none")]
    pub db_stats: Option<DbStats>,
    #[serde(rename = "numShards")]
    pub num_shards: u32,
    #[serde(rename = "shardInfos")]
    pub shard_infos: Vec<ShardInfo>,
    pub version: String,
    #[serde(rename = "peer_id")]
    pub peer_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DbStats {
    #[serde(rename = "numMessages")]
    pub num_messages: u64,
    #[serde(rename = "numFidRegistrations")]
    pub num_fid_registrations: u64,
    #[serde(rename = "approxSize")]
    pub approx_size: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShardInfo {
    #[serde(rename = "shardId")]
    pub shard_id: u32,
    #[serde(rename = "maxHeight")]
    pub max_height: u64,
    #[serde(rename = "numMessages")]
    pub num_messages: u64,
    #[serde(rename = "numFidRegistrations")]
    pub num_fid_registrations: u64,
    #[serde(rename = "approxSize")]
    pub approx_size: u64,
    #[serde(rename = "blockDelay")]
    pub block_delay: u64,
    #[serde(rename = "mempoolSize")]
    pub mempool_size: u64,
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GetFidsRequest {
    pub shard_id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub page_token: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverse: Option<bool>,

    // For backwards compatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pageSize: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub pageToken: Option<Vec<u8>>,
}

impl GetFidsRequest {
    pub fn to_proto(self) -> proto::FidsRequest {
        proto::FidsRequest {
            shard_id: self.shard_id,
            page_size: self.page_size.or(self.pageSize),
            page_token: self.page_token.or(self.pageToken),
            reverse: self.reverse,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GetFidsResponse {
    pub fids: Vec<u64>,
    #[serde(rename = "nextPageToken", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FidRequest {
    pub fid: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub page_token: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverse: Option<bool>,

    // For backwards compatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pageSize: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub pageToken: Option<Vec<u8>>,
}

impl FidRequest {
    pub fn to_proto(self) -> proto::FidRequest {
        proto::FidRequest {
            fid: self.fid,
            page_size: self.page_size.or(self.pageSize),
            page_token: self.page_token.or(self.pageToken),
            reverse: self.reverse,
        }
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FidTimestampRequest {
    pub fid: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub page_token: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverse: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_timestamp: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_timestamp: Option<u64>,

    // For backwards compatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pageSize: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub pageToken: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub startTimestamp: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopTimestamp: Option<u64>,
}

impl FidTimestampRequest {
    pub fn to_proto(self) -> proto::FidTimestampRequest {
        proto::FidTimestampRequest {
            fid: self.fid,
            page_size: self.page_size.or(self.pageSize),
            page_token: self.page_token.or(self.pageToken),
            start_timestamp: self.start_timestamp.or(self.startTimestamp),
            stop_timestamp: self.stop_timestamp.or(self.stopTimestamp),
            reverse: self.reverse,
        }
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CastsByParentRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fid: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub page_token: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverse: Option<bool>,

    // For backwards compatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pageSize: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub pageToken: Option<Vec<u8>>,
}

impl CastsByParentRequest {
    pub fn to_proto(
        self,
        parent: Option<casts_by_parent_request::Parent>,
    ) -> proto::CastsByParentRequest {
        proto::CastsByParentRequest {
            parent,
            page_size: self.page_size.or(self.pageSize),
            page_token: self.page_token.or(self.pageToken),
            reverse: self.reverse,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReactionRequest {
    fid: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_fid: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_hash: Option<String>,
    reaction_type: ReactionType,
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReactionsByFidRequest {
    fid: u64,
    reaction_type: ReactionType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    page_token: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reverse: Option<bool>,

    // For backwards compatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pageSize: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub pageToken: Option<Vec<u8>>,
}

impl ReactionsByFidRequest {
    pub fn to_proto(self) -> proto::ReactionsByFidRequest {
        proto::ReactionsByFidRequest {
            fid: self.fid,
            reaction_type: Some(self.reaction_type as i32),
            page_size: self.page_size.or(self.pageSize),
            page_token: self.page_token.or(self.pageToken),
            reverse: self.reverse,
        }
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReactionsByCastRequest {
    pub target_fid: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reaction_type: Option<ReactionType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub page_token: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverse: Option<bool>,

    // For backwards compatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pageSize: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub pageToken: Option<Vec<u8>>,
}

impl ReactionsByCastRequest {
    pub fn to_proto(
        self,
        target: reactions_by_target_request::Target,
    ) -> proto::ReactionsByTargetRequest {
        proto::ReactionsByTargetRequest {
            target: Some(target),
            reaction_type: self.reaction_type.map(|r| r as i32),
            page_size: self.page_size.or(self.pageSize),
            page_token: self.page_token.or(self.pageToken),
            reverse: self.reverse,
        }
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReactionsByTargetRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_cast_id: Option<CastId>,
    #[serde(default, rename = "url", skip_serializing_if = "Option::is_none")]
    pub target_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reaction_type: Option<ReactionType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub page_token: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverse: Option<bool>,

    // For backwards compatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pageSize: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub pageToken: Option<Vec<u8>>,
}

impl ReactionsByTargetRequest {
    pub fn to_proto(
        self,
        target: reactions_by_target_request::Target,
    ) -> proto::ReactionsByTargetRequest {
        proto::ReactionsByTargetRequest {
            target: Some(target),
            reaction_type: self.reaction_type.map(|r| r as i32),
            page_size: self.page_size.or(self.pageSize),
            page_token: self.page_token.or(self.pageToken),
            reverse: self.reverse,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinkRequest {
    fid: u64,
    link_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_fid: Option<u64>,
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinksByFidRequest {
    fid: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    link_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    page_token: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reverse: Option<bool>,

    // For backwards compatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pageSize: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub pageToken: Option<Vec<u8>>,
}

impl LinksByFidRequest {
    pub fn to_proto(self) -> proto::LinksByFidRequest {
        proto::LinksByFidRequest {
            fid: self.fid,
            link_type: self.link_type,
            page_size: self.page_size.or(self.pageSize),
            page_token: self.page_token.or(self.pageToken),
            reverse: self.reverse,
        }
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinksByTargetRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_fid: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    link_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    page_token: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reverse: Option<bool>,

    // For backwards compatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pageSize: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub pageToken: Option<Vec<u8>>,
}

impl LinksByTargetRequest {
    pub fn to_proto(self, target: links_by_target_request::Target) -> proto::LinksByTargetRequest {
        proto::LinksByTargetRequest {
            target: Some(target),
            link_type: self.link_type,
            page_size: self.page_size.or(self.pageSize),
            page_token: self.page_token.or(self.pageToken),
            reverse: self.reverse,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum StorageUnitType {
    UnitTypeLegacy = 0,
    UnitType2024 = 1,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageUnitDetails {
    #[serde(rename = "unitType")]
    unit_type: StorageUnitType,
    #[serde(rename = "unitSize")]
    unit_size: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum StoreType {
    None = 0,
    Casts = 1,
    Links = 2,
    Reactions = 3,
    UserData = 4,
    Verifications = 5,
    UsernameProofs = 6,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageLimit {
    #[serde(rename = "storeType")]
    pub store_type: StoreType,
    pub name: String,
    pub limit: u64,
    pub used: u64,
    #[serde(rename = "earliestTimestamp")]
    pub earliest_timestamp: u64,
    #[serde(rename = "earliestHash")]
    pub earliest_hash: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageLimitsResponse {
    pub limits: Vec<StorageLimit>,
    pub units: u32,
    #[serde(rename = "unitDetails")]
    pub unit_details: Vec<StorageUnitDetails>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UsernameProofRequest {
    name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UserNameProof {
    pub timestamp: u64,
    pub name: String,
    pub owner: String,
    #[serde(with = "serdebase64")]
    pub signature: Vec<u8>,
    pub fid: u64,
    pub r#type: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UsernameProofsResponse {
    pub proofs: Vec<UserNameProof>,
}
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum OnChainEventType {
    EVENT_TYPE_NONE = 0,
    EVENT_TYPE_SIGNER = 1,
    EVENT_TYPE_SIGNER_MIGRATED = 2,
    EVENT_TYPE_ID_REGISTER = 3,
    EVENT_TYPE_STORAGE_RENT = 4,
}
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum SignerEventType {
    SIGNER_EVENT_TYPE_NONE = 0,
    SIGNER_EVENT_TYPE_ADD = 1,
    SIGNER_EVENT_TYPE_REMOVE = 2,
    SIGNER_EVENT_TYPE_ADMIN_RESET = 3,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum IdRegisterEventType {
    None = 0,
    Register = 1,
    Transfer = 2,
    ChangeRecovery = 3,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SignerEventBody {
    #[serde(with = "serdehex")]
    pub key: Vec<u8>,
    #[serde(rename = "keyType")]
    pub key_type: u32,
    #[serde(rename = "eventType")]
    pub event_type: SignerEventType,
    #[serde(with = "serdebase64")]
    pub metadata: Vec<u8>,
    #[serde(rename = "metadataType")]
    pub metadata_type: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SignerMigratedEventBody {
    #[serde(rename = "migratedAt")]
    pub migrated_at: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IdRegisterEventBody {
    #[serde(with = "serdehex")]
    pub to: Vec<u8>,
    #[serde(rename = "eventType")]
    pub event_type: IdRegisterEventType,
    #[serde(with = "serdehex")]
    pub from: Vec<u8>,
    #[serde(with = "serdehex", rename = "recoveryAddress")]
    pub recovery_address: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageRentEventBody {
    #[serde(with = "serdehex")]
    pub payer: Vec<u8>,
    pub units: u32,
    pub expiry: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OnChainEvent {
    pub r#type: OnChainEventType,
    #[serde(rename = "chainId")]
    pub chain_id: u32,
    #[serde(rename = "blockNumber")]
    pub block_number: u32,
    #[serde(with = "serdehex", rename = "blockHash")]
    pub block_hash: Vec<u8>,
    #[serde(rename = "blockTimestamp")]
    pub block_timestamp: u64,
    #[serde(with = "serdehex", rename = "transactionHash")]
    pub transaction_hash: Vec<u8>,
    #[serde(rename = "logIndex")]
    pub log_index: u32,
    pub fid: u64,
    #[serde(rename = "signerEventBody", skip_serializing_if = "Option::is_none")]
    pub signer_event_body: Option<SignerEventBody>,
    #[serde(
        rename = "signerMigratedEventBody",
        skip_serializing_if = "Option::is_none"
    )]
    pub signer_migrated_event_body: Option<SignerMigratedEventBody>,
    #[serde(
        rename = "idRegisterEventBody",
        skip_serializing_if = "Option::is_none"
    )]
    pub id_register_event_body: Option<IdRegisterEventBody>,
    #[serde(
        rename = "storageRentEventBody",
        skip_serializing_if = "Option::is_none"
    )]
    pub storage_rent_event_body: Option<StorageRentEventBody>,
    #[serde(rename = "txIndex")]
    pub tx_index: u32,
    pub version: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OnChainEventResponse {
    pub events: Vec<OnChainEvent>,
    #[serde(rename = "nextPageToken", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OnChainEventRequest {
    fid: u64,
    event_type: OnChainEventType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    page_token: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reverse: Option<bool>,

    // For backwards compatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pageSize: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub pageToken: Option<Vec<u8>>,
}

impl OnChainEventRequest {
    pub fn to_proto(self) -> proto::OnChainEventRequest {
        proto::OnChainEventRequest {
            fid: self.fid,
            event_type: self.event_type as i32,
            page_size: self.page_size.or(self.pageSize),
            page_token: self.page_token.or(self.pageToken),
            reverse: self.reverse,
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum HubEventType {
    HUB_EVENT_TYPE_NONE = 0,
    HUB_EVENT_TYPE_MERGE_MESSAGE = 1,
    HUB_EVENT_TYPE_PRUNE_MESSAGE = 2,
    HUB_EVENT_TYPE_REVOKE_MESSAGE = 3,
    HUB_EVENT_TYPE_MERGE_USERNAME_PROOF = 6,
    HUB_EVENT_TYPE_MERGE_ON_CHAIN_EVENT = 9,
    HUB_EVENT_TYPE_MERGE_FAILURE = 10,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MergeMessageBody {
    pub message: Message,
    #[serde(rename = "deletedMessages")]
    pub deleted_messages: Vec<Message>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MergeFailureBody {
    pub message: Message,
    pub code: String,
    pub reason: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PruneMessageBody {
    pub message: Message,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RevokeMessageBody {
    pub message: Message,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MergeOnChainEventBody {
    #[serde(rename = "onChainEvent")]
    pub on_chain_event: OnChainEvent,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MergeUsernameProofBody {
    #[serde(rename = "usernameProof", skip_serializing_if = "Option::is_none")]
    pub username_proof: Option<UsernameProofBody>,
    #[serde(
        rename = "deletedUsernameProof",
        skip_serializing_if = "Option::is_none"
    )]
    pub deleted_username_proof: Option<UsernameProofBody>,
    #[serde(
        rename = "usernameProofMessage",
        skip_serializing_if = "Option::is_none"
    )]
    pub username_proof_message: Option<Message>,
    #[serde(
        rename = "deletedUsernameProofMessage",
        skip_serializing_if = "Option::is_none"
    )]
    pub deleted_username_proof_message: Option<Message>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HubEvent {
    #[serde(rename = "type")]
    pub hub_event_type: String,
    pub id: u64,
    #[serde(rename = "mergeMessageBody", skip_serializing_if = "Option::is_none")]
    pub merge_message_body: Option<MergeMessageBody>,
    #[serde(rename = "pruneMessageBody", skip_serializing_if = "Option::is_none")]
    pub prune_message_body: Option<PruneMessageBody>,
    #[serde(rename = "revokeMessageBody", skip_serializing_if = "Option::is_none")]
    pub revoke_message_body: Option<RevokeMessageBody>,
    #[serde(
        rename = "mergeUsernameProofBody",
        skip_serializing_if = "Option::is_none"
    )]
    pub merge_username_proof_body: Option<MergeUsernameProofBody>,
    #[serde(
        rename = "mergeOnChainEventBody",
        skip_serializing_if = "Option::is_none"
    )]
    pub merge_on_chain_event_body: Option<MergeOnChainEventBody>,
    #[serde(rename = "mergeFailureBody", skip_serializing_if = "Option::is_none")]
    pub merge_failure_body: Option<MergeFailureBody>,
    #[serde(rename = "blockNumber")]
    pub block_number: u64,
    #[serde(rename = "shardIndex")]
    pub shard_index: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventsResponse {
    pub events: Vec<HubEvent>,
    // TODO: What's the best way to support next page token with multiple shards?
    // #[serde(rename = "nextPageToken", skip_serializing_if = "Option::is_none")]
    // pub next_page_token: Option<String>,
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventsRequest {
    #[serde(default, rename = "from_event_id")] // To keep it consistent with hubble
    start_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    shard_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stop_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    page_size: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    page_token: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reverse: Option<bool>,

    // For backwards compatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pageSize: Option<u32>,
    #[serde(
        default,
        with = "serdebase64opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub pageToken: Option<Vec<u8>>,
}

impl EventsRequest {
    pub fn to_proto(self) -> proto::EventsRequest {
        proto::EventsRequest {
            start_id: self.start_id,
            shard_index: self.shard_index,
            stop_id: self.stop_id,
            page_size: self.page_size.or(self.pageSize),
            page_token: self.page_token.or(self.pageToken),
            reverse: self.reverse,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ValidationResult {
    pub valid: bool,
    pub message: Option<Message>,
}

// Common error response
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub error_detail: Option<String>,
}

// Implementation struct
#[derive(Clone)]
pub struct HubHttpServiceImpl {
    pub service: Arc<MyHubService>,
}

fn map_get_info_response_to_json_info_response(
    info_response: proto::GetInfoResponse,
) -> Result<InfoResponse, ErrorResponse> {
    Ok(InfoResponse {
        db_stats: info_response.db_stats.map(|db_stats| DbStats {
            num_messages: db_stats.num_messages,
            num_fid_registrations: db_stats.num_fid_registrations,
            approx_size: db_stats.approx_size,
        }),
        num_shards: info_response.num_shards,
        peer_id: info_response.peer_id,
        version: info_response.version,
        shard_infos: info_response
            .shard_infos
            .iter()
            .map(|shard_info| ShardInfo {
                shard_id: shard_info.shard_id,
                max_height: shard_info.max_height,
                num_messages: shard_info.num_messages,
                num_fid_registrations: shard_info.num_fid_registrations,
                approx_size: shard_info.approx_size,
                block_delay: shard_info.block_delay,
                mempool_size: shard_info.mempool_size,
            })
            .collect(),
    })
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
        signature: username_proof_body.signature,
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
        claim_signature: verification_add_address_body.claim_signature,
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
                target_hash: cast_remove_body.target_hash,
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
        Some(Body::FrameActionBody(frame_action_body)) => {
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
                frame_action_body: Some(FrameActionBody {
                    url: frame_action_body.url,
                    button_index: frame_action_body.button_index,
                    cast_id: frame_action_body.cast_id.map(|cast_id| CastId {
                        fid: cast_id.fid,
                        hash: format!("0x{}", hex::encode(cast_id.hash)),
                    }),
                    input_text: frame_action_body.input_text,
                    state: frame_action_body.state,
                    transaction_id: frame_action_body.transaction_id,
                    address: frame_action_body.address,
                }),
                link_compact_state_body: None,
            });
        }
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
                reaction_body: Some(result),
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
            .map(|m| map_proto_message_to_json_message(m.clone()).unwrap())
            .collect(),
        next_page_token: Some(
            messages_response
                .next_page_token
                .map(|t| BASE64_STANDARD.encode(t))
                .unwrap_or_else(|| "".to_string()),
        ),
    })
}

fn map_proto_on_chain_event_to_json_on_chain_event(
    onchain_event: proto::OnChainEvent,
) -> Result<OnChainEvent, ErrorResponse> {
    let mut signer_event_body: Option<SignerEventBody> = None;
    let mut signer_migrated_event_body: Option<SignerMigratedEventBody> = None;
    let mut id_register_event_body: Option<IdRegisterEventBody> = None;
    let mut storage_rent_event_body: Option<StorageRentEventBody> = None;
    match &onchain_event.body {
        None => {}
        Some(on_chain_event::Body::SignerEventBody(body)) => {
            signer_event_body = Some(SignerEventBody {
                key: body.key.clone(),
                key_type: body.key_type,
                event_type: match body.event_type {
                    1 => SignerEventType::SIGNER_EVENT_TYPE_ADD,
                    2 => SignerEventType::SIGNER_EVENT_TYPE_REMOVE,
                    3 => SignerEventType::SIGNER_EVENT_TYPE_ADMIN_RESET,
                    _ => SignerEventType::SIGNER_EVENT_TYPE_NONE,
                },
                metadata: body.metadata.clone(),
                metadata_type: body.metadata_type,
            });
        }
        Some(on_chain_event::Body::SignerMigratedEventBody(body)) => {
            signer_migrated_event_body = Some(SignerMigratedEventBody {
                migrated_at: body.migrated_at,
            });
        }
        Some(on_chain_event::Body::IdRegisterEventBody(body)) => {
            id_register_event_body = Some(IdRegisterEventBody {
                to: body.to.clone(),
                event_type: match body.event_type {
                    1 => IdRegisterEventType::Register,
                    2 => IdRegisterEventType::Transfer,
                    3 => IdRegisterEventType::ChangeRecovery,
                    _ => IdRegisterEventType::None,
                },
                from: body.from.clone(),
                recovery_address: body.recovery_address.clone(),
            });
        }
        Some(on_chain_event::Body::StorageRentEventBody(body)) => {
            storage_rent_event_body = Some(StorageRentEventBody {
                payer: body.payer.clone(),
                units: body.units,
                expiry: body.expiry,
            });
        }
    }
    Ok(OnChainEvent {
        r#type: match onchain_event.r#type {
            1 => OnChainEventType::EVENT_TYPE_SIGNER,
            2 => OnChainEventType::EVENT_TYPE_SIGNER_MIGRATED,
            3 => OnChainEventType::EVENT_TYPE_ID_REGISTER,
            4 => OnChainEventType::EVENT_TYPE_STORAGE_RENT,
            _ => OnChainEventType::EVENT_TYPE_NONE,
        },
        chain_id: onchain_event.chain_id,
        block_number: onchain_event.block_number,
        block_hash: onchain_event.block_hash.clone(),
        block_timestamp: onchain_event.block_timestamp,
        transaction_hash: onchain_event.transaction_hash.clone(),
        log_index: onchain_event.log_index,
        fid: onchain_event.fid,
        tx_index: onchain_event.tx_index,
        version: onchain_event.version,
        signer_event_body,
        signer_migrated_event_body,
        id_register_event_body,
        storage_rent_event_body,
    })
}

fn map_proto_hub_event_to_json_hub_event(
    hub_event: proto::HubEvent,
) -> Result<HubEvent, ErrorResponse> {
    let mut merge_message_body: Option<MergeMessageBody> = None;
    let mut prune_message_body: Option<PruneMessageBody> = None;
    let mut revoke_message_body: Option<RevokeMessageBody> = None;
    let mut merge_username_proof_body: Option<MergeUsernameProofBody> = None;
    let mut merge_on_chain_event_body: Option<MergeOnChainEventBody> = None;
    let mut merge_failure_body: Option<MergeFailureBody> = None;
    match &hub_event.body {
        None => {}
        Some(hub_event::Body::MergeMessageBody(body)) => {
            merge_message_body = body.message.clone().map(|msg| MergeMessageBody {
                message: map_proto_message_to_json_message(msg).unwrap(),
                deleted_messages: body
                    .deleted_messages
                    .iter()
                    .map(|m| map_proto_message_to_json_message(m.clone()).unwrap())
                    .collect(),
            });
        }
        Some(hub_event::Body::PruneMessageBody(body)) => {
            prune_message_body = body.message.clone().map(|msg| PruneMessageBody {
                message: map_proto_message_to_json_message(msg).unwrap(),
            })
        }
        Some(hub_event::Body::RevokeMessageBody(body)) => {
            revoke_message_body = body.message.clone().map(|msg| RevokeMessageBody {
                message: map_proto_message_to_json_message(msg).unwrap(),
            })
        }
        Some(hub_event::Body::MergeUsernameProofBody(body)) => {
            let username_proof_body = body
                .username_proof
                .clone()
                .map(|u| map_proto_username_proof_body_to_json_username_proof_body(u).unwrap());
            let username_proof_message = body
                .username_proof_message
                .clone()
                .map(|msg| map_proto_message_to_json_message(msg).unwrap());
            let deleted_username_proof_body = body
                .deleted_username_proof
                .clone()
                .map(|u| map_proto_username_proof_body_to_json_username_proof_body(u).unwrap());
            let deleted_username_proof_message = body
                .deleted_username_proof_message
                .clone()
                .map(|msg| map_proto_message_to_json_message(msg).unwrap());

            merge_username_proof_body = Some(MergeUsernameProofBody {
                username_proof: username_proof_body,
                username_proof_message,
                deleted_username_proof: deleted_username_proof_body,
                deleted_username_proof_message,
            });
        }
        Some(hub_event::Body::MergeOnChainEventBody(body)) => {
            merge_on_chain_event_body =
                body.on_chain_event.clone().map(|e| MergeOnChainEventBody {
                    on_chain_event: map_proto_on_chain_event_to_json_on_chain_event(e).unwrap(),
                });
        }
        Some(hub_event::Body::MergeFailure(body)) => {
            merge_failure_body = body.message.clone().map(|msg| MergeFailureBody {
                message: map_proto_message_to_json_message(msg).unwrap(),
                code: body.code.clone(),
                reason: body.reason.clone(),
            });
        }
    }

    Ok(HubEvent {
        hub_event_type: proto::HubEventType::try_from(hub_event.r#type)
            .map_err(|_| ErrorResponse {
                error: "Invalid hub event type".to_string(),
                error_detail: None,
            })?
            .as_str_name()
            .to_owned(),
        id: hub_event.id,
        merge_message_body,
        prune_message_body,
        revoke_message_body,
        merge_username_proof_body,
        merge_on_chain_event_body,
        merge_failure_body,
        block_number: hub_event.block_number,
        shard_index: hub_event.shard_index,
    })
}

// Service trait for type-safe request handling
#[async_trait]
pub trait HubHttpService {
    async fn get_info(&self, req: InfoRequest) -> Result<InfoResponse, ErrorResponse>;
    async fn get_fids(&self, req: GetFidsRequest) -> Result<GetFidsResponse, ErrorResponse>;
    async fn get_cast_by_id(&self, req: IdRequest) -> Result<Message, ErrorResponse>;
    async fn get_casts_by_fid(
        &self,
        req: FidTimestampRequest,
    ) -> Result<PagedResponse, ErrorResponse>;
    async fn get_casts_by_mention(&self, req: FidRequest) -> Result<PagedResponse, ErrorResponse>;
    async fn get_casts_by_parent(
        &self,
        req: CastsByParentRequest,
    ) -> Result<PagedResponse, ErrorResponse>;
    async fn get_reaction_by_id(&self, req: ReactionRequest) -> Result<Message, ErrorResponse>;
    async fn get_reactions_by_fid(
        &self,
        req: ReactionsByFidRequest,
    ) -> Result<PagedResponse, ErrorResponse>;
    async fn get_reactions_by_cast(
        &self,
        req: ReactionsByCastRequest,
    ) -> Result<PagedResponse, ErrorResponse>;
    async fn get_reactions_by_target(
        &self,
        req: ReactionsByTargetRequest,
    ) -> Result<PagedResponse, ErrorResponse>;
    async fn get_link_by_id(&self, req: LinkRequest) -> Result<Message, ErrorResponse>;
    async fn get_links_by_fid(
        &self,
        req: LinksByFidRequest,
    ) -> Result<PagedResponse, ErrorResponse>;
    async fn get_links_by_target_fid(
        &self,
        req: LinksByTargetRequest,
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
    async fn validate_message(
        &self,
        req: proto::Message,
    ) -> Result<ValidationResult, ErrorResponse>;
    async fn submit_message(
        &self,
        req: proto::Message,
        headers: HeaderMap<HeaderValue>,
    ) -> Result<Message, ErrorResponse>;
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
    async fn get_events(&self, req: EventsRequest) -> Result<EventsResponse, ErrorResponse>;
}

#[async_trait]
impl HubHttpService for HubHttpServiceImpl {
    async fn get_info(&self, _request: InfoRequest) -> Result<InfoResponse, ErrorResponse> {
        let response = self
            .service
            .get_info(tonic::Request::<proto::GetInfoRequest>::new(
                proto::GetInfoRequest {},
            ))
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get info".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto = response.into_inner();
        map_get_info_response_to_json_info_response(proto)
    }

    async fn get_fids(&self, request: GetFidsRequest) -> Result<GetFidsResponse, ErrorResponse> {
        let response = self
            .service
            .get_fids(tonic::Request::<proto::FidsRequest>::new(
                request.to_proto(),
            ))
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get fids".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto = response.into_inner();

        Ok(GetFidsResponse {
            fids: proto.fids,
            next_page_token: proto.next_page_token.map(|t| BASE64_STANDARD.encode(t)),
        })
    }

    async fn get_cast_by_id(&self, req: IdRequest) -> Result<Message, ErrorResponse> {
        let fid = req.fid.parse::<u64>().map_err(|e| ErrorResponse {
            error: "Invalid fid".to_string(),
            error_detail: Some(e.to_string()),
        })?;

        let hash = hex::decode(&req.hash.replace("0x", "")).map_err(|e| ErrorResponse {
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

    async fn get_casts_by_fid(
        &self,
        req: FidTimestampRequest,
    ) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let response = service
            .get_all_cast_messages_by_fid(tonic::Request::<proto::FidTimestampRequest>::new(
                req.to_proto(),
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

        let grpc_req = tonic::Request::new(req.to_proto());
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

    async fn get_casts_by_parent(
        &self,
        req: CastsByParentRequest,
    ) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let parent = if req.hash.is_some() && req.fid.is_some() {
            Ok(Some(casts_by_parent_request::Parent::ParentCastId(
                proto::CastId {
                    fid: req.fid.unwrap(),
                    hash: hex::decode(req.hash.clone().unwrap().replace("0x", "")).map_err(
                        |e| ErrorResponse {
                            error: "Invalid request".to_string(),
                            error_detail: Some(e.to_string()),
                        },
                    )?,
                },
            )))
        } else if req.url.is_some() {
            Ok(Some(casts_by_parent_request::Parent::ParentUrl(
                req.url.clone().unwrap(),
            )))
        } else {
            Err(ErrorResponse {
                error: "Invalid request".to_string(),
                error_detail: Some(
                    "fid and hash must be specified or url must be specified".to_string(),
                ),
            })
        }?;

        let grpc_req = tonic::Request::new(req.to_proto(parent));
        let response = service
            .get_casts_by_parent(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get casts by mention".to_string(),
                error_detail: Some(e.to_string()),
            })?;

        let proto_resp = response.into_inner();
        map_proto_messages_response_to_json_paged_response(proto_resp)
    }

    async fn get_reaction_by_id(&self, req: ReactionRequest) -> Result<Message, ErrorResponse> {
        let service = &self.service;
        let target = if req.target_fid.is_some() {
            let hash = if let Some(hash_str) = req.target_hash.as_deref() {
                hex::decode(hash_str.trim_start_matches("0x")).map_err(|e| ErrorResponse {
                    error: "Invalid hash".to_string(),
                    error_detail: Some(e.to_string()),
                })?
            } else {
                Vec::new()
            };

            reaction_request::Target::TargetCastId(proto::CastId {
                fid: req.target_fid.expect("target fid not specified"),
                hash,
            })
        } else if req.target_url.is_some() {
            reaction_request::Target::TargetUrl(req.target_url.unwrap())
        } else {
            return Err(ErrorResponse {
                error: "target not specified".to_string(),
                error_detail: None,
            });
        };
        let grpc_req = tonic::Request::new(proto::ReactionRequest {
            fid: req.fid,
            reaction_type: req.reaction_type as i32,
            target: Some(target),
        });
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
    async fn get_reactions_by_fid(
        &self,
        req: ReactionsByFidRequest,
    ) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req.to_proto());
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
        req: ReactionsByCastRequest,
    ) -> Result<PagedResponse, ErrorResponse> {
        let hash = if let Some(hash_str) = req.target_hash.as_deref() {
            hex::decode(hash_str.trim_start_matches("0x")).map_err(|e| ErrorResponse {
                error: "Invalid hash".to_string(),
                error_detail: Some(e.to_string()),
            })?
        } else {
            Vec::new()
        };

        let target = reactions_by_target_request::Target::TargetCastId(proto::CastId {
            fid: req.target_fid,
            hash,
        });

        let grpc_req = tonic::Request::new(req.to_proto(target));

        let response = self
            .service
            .get_reactions_by_target(grpc_req)
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
        let target = if req.target_cast_id.is_some() {
            let cast_id = req.target_cast_id.clone().unwrap();
            let hash = hex::decode(cast_id.hash.replace("0x", ""));
            if hash.is_err() {
                return Err(ErrorResponse {
                    error: hash.unwrap_err().to_string(),
                    error_detail: None,
                });
            }
            reactions_by_target_request::Target::TargetCastId(proto::CastId {
                fid: cast_id.fid,
                hash: hash.unwrap(),
            })
        } else if req.target_url.is_some() {
            reactions_by_target_request::Target::TargetUrl(req.target_url.clone().unwrap())
        } else {
            return Err(ErrorResponse {
                error: "target not specified".to_string(),
                error_detail: None,
            });
        };
        let grpc_req = tonic::Request::new(req.to_proto(target));
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
    async fn get_link_by_id(&self, req: LinkRequest) -> Result<Message, ErrorResponse> {
        let service = &self.service;
        let target = if req.target_fid.is_some() {
            link_request::Target::TargetFid(req.target_fid.unwrap())
        } else {
            return Err(ErrorResponse {
                error: "target not specified".to_string(),
                error_detail: None,
            });
        };
        let grpc_req = tonic::Request::new(proto::LinkRequest {
            fid: req.fid,
            link_type: req.link_type,
            target: Some(target),
        });
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
    async fn get_links_by_fid(
        &self,
        req: LinksByFidRequest,
    ) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req.to_proto());
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
        req: LinksByTargetRequest,
    ) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let target = if req.link_type.is_some() {
            links_by_target_request::Target::TargetFid(req.target_fid.unwrap())
        } else {
            return Err(ErrorResponse {
                error: "target not specified".to_string(),
                error_detail: None,
            });
        };
        let grpc_req = tonic::Request::new(req.to_proto(target));
        let response = service
            .get_links_by_target(grpc_req)
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
        let grpc_req = tonic::Request::new(req.to_proto());
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
        let grpc_req = tonic::Request::new(req.to_proto());
        let response = service
            .get_current_storage_limits_by_fid(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get storage limits".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let limits = response.into_inner();
        Ok(StorageLimitsResponse {
            limits: limits
                .limits
                .iter()
                .map(|l: &proto::StorageLimit| StorageLimit {
                    store_type: match l.store_type {
                        1 => StoreType::Casts,
                        2 => StoreType::Links,
                        3 => StoreType::Reactions,
                        4 => StoreType::UserData,
                        5 => StoreType::Verifications,
                        6 => StoreType::UsernameProofs,
                        _ => StoreType::None,
                    },
                    name: l.name.clone(),
                    limit: l.limit,
                    used: l.used,
                    earliest_timestamp: l.earliest_timestamp,
                    earliest_hash: l.earliest_hash.clone(),
                })
                .collect(),
            units: limits.units,
            unit_details: limits
                .unit_details
                .iter()
                .map(|u: &proto::StorageUnitDetails| StorageUnitDetails {
                    unit_size: u.unit_size,
                    unit_type: match u.unit_type {
                        1 => StorageUnitType::UnitType2024,
                        _ => StorageUnitType::UnitTypeLegacy,
                    },
                })
                .collect(),
        })
    }

    /// GET /v1/userNameProofByName
    async fn get_user_name_proof_by_name(
        &self,
        req: UsernameProofRequest,
    ) -> Result<UserNameProof, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(proto::UsernameProofRequest {
            name: req.name.into(),
        });
        let response = service
            .get_username_proof(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get username proof".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proof = response.into_inner();
        let proof_type = proof.r#type().as_str_name().to_owned();
        Ok(UserNameProof {
            timestamp: proof.timestamp,
            name: std::str::from_utf8(&proof.name.as_slice())
                .unwrap()
                .to_string(),
            owner: format!("0x{}", hex::encode(&proof.owner)),
            signature: proof.signature,
            fid: proof.fid,
            r#type: proof_type,
        })
    }

    /// GET /v1/userNameProofsByFid
    async fn get_user_name_proofs_by_fid(
        &self,
        req: FidRequest,
    ) -> Result<UsernameProofsResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req.to_proto());
        let response = service
            .get_user_name_proofs_by_fid(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get username proofs".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proof = response.into_inner();
        Ok(UsernameProofsResponse {
            proofs: proof
                .proofs
                .iter()
                .map(|p| UserNameProof {
                    timestamp: p.timestamp,
                    name: std::str::from_utf8(&p.name.as_slice()).unwrap().to_string(),
                    owner: format!("0x{}", hex::encode(&p.owner)),
                    signature: p.signature.clone(),
                    fid: p.fid,
                    r#type: p.r#type().as_str_name().to_owned(),
                })
                .collect(),
        })
    }

    /// POST /v1/validateMessage
    async fn validate_message(
        &self,
        req: proto::Message,
    ) -> Result<ValidationResult, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req);
        let response = service
            .validate_message(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to validate message".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto_resp = response.into_inner();
        return Ok(ValidationResult {
            valid: proto_resp.valid,
            message: Some(map_proto_message_to_json_message(
                proto_resp.message.unwrap(),
            )?),
        });
    }

    // POST /v1/submitMessage
    async fn submit_message(
        &self,
        req: proto::Message,
        headers: HeaderMap<HeaderValue>,
    ) -> Result<Message, ErrorResponse> {
        let service = &self.service;
        let mut grpc_req = tonic::Request::new(req);

        if let Some(auth) = headers.get("authorization") {
            match auth.to_str() {
                Err(err) => {
                    return Err(ErrorResponse {
                        error: "Invalid auth header".to_string(),
                        error_detail: Some(err.to_string()),
                    })
                }
                Ok(auth) => {
                    grpc_req
                        .metadata_mut()
                        .append("authorization", MetadataValue::from_str(auth).unwrap());
                }
            }
        };

        let response = service
            .submit_message(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to submit message".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let proto_resp = response.into_inner();
        map_proto_message_to_json_message(proto_resp)
    }

    /// GET /v1/verificationsByFid
    async fn get_verifications_by_fid(
        &self,
        req: FidRequest,
    ) -> Result<PagedResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req.to_proto());
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
        let grpc_req = tonic::Request::new(req.to_proto());
        let response = service
            .get_on_chain_signers_by_fid(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get on chain signers".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let onchain_event_response = response.into_inner();
        Ok(OnChainEventResponse {
            events: onchain_event_response
                .events
                .iter()
                .map(|e| map_proto_on_chain_event_to_json_on_chain_event(e.clone()).unwrap())
                .collect(),
            next_page_token: onchain_event_response
                .next_page_token
                .map(|t| BASE64_STANDARD.encode(t)),
        })
    }

    /// GET /v1/onChainEventsByFid
    async fn get_on_chain_events_by_fid(
        &self,
        req: OnChainEventRequest,
    ) -> Result<OnChainEventResponse, ErrorResponse> {
        let service = &self.service;
        let grpc_req = tonic::Request::new(req.to_proto());
        let response = service
            .get_on_chain_events(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get on chain events".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let onchain_event_response = response.into_inner();
        Ok(OnChainEventResponse {
            events: onchain_event_response
                .events
                .iter()
                .map(|e| map_proto_on_chain_event_to_json_on_chain_event(e.clone()).unwrap())
                .collect(),
            next_page_token: onchain_event_response
                .next_page_token
                .map(|t| BASE64_STANDARD.encode(t)),
        })
    }

    async fn get_events(&self, req: EventsRequest) -> Result<EventsResponse, ErrorResponse> {
        let service = &self.service;

        let grpc_req = tonic::Request::new(req.to_proto());
        let response = service
            .get_events(grpc_req)
            .await
            .map_err(|e| ErrorResponse {
                error: "Failed to get events".to_string(),
                error_detail: Some(e.to_string()),
            })?;
        let events_response = response.into_inner();
        Ok(EventsResponse {
            events: events_response
                .events
                .iter()
                .map(|e| map_proto_hub_event_to_json_hub_event(e.clone()).unwrap())
                .collect(),
        })
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

    pub async fn handle(
        &self,
        req: Request<hyper::body::Incoming>,
    ) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/v1/info") => {
                self.handle_request::<InfoRequest, InfoResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_info(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/fids") => {
                self.handle_request::<GetFidsRequest, GetFidsResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_fids(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/castById") => {
                self.handle_request::<IdRequest, Message, _>(req, |service, req| {
                    Box::pin(async move { service.get_cast_by_id(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/castsByFid") => {
                self.handle_request::<FidTimestampRequest, PagedResponse, _>(
                    req,
                    move |service, req| {
                        Box::pin(async move { service.get_casts_by_fid(req).await })
                    },
                )
                .await
            }
            (&Method::GET, "/v1/castsByMention") => {
                self.handle_request::<FidRequest, PagedResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_casts_by_mention(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/castsByParent") => {
                self.handle_request::<CastsByParentRequest, PagedResponse, _>(
                    req,
                    |service, req| Box::pin(async move { service.get_casts_by_parent(req).await }),
                )
                .await
            }
            (&Method::GET, "/v1/reactionById") => {
                self.handle_request::<ReactionRequest, Message, _>(req, |service, req| {
                    Box::pin(async move { service.get_reaction_by_id(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/reactionsByFid") => {
                self.handle_request::<ReactionsByFidRequest, PagedResponse, _>(
                    req,
                    |service, req| Box::pin(async move { service.get_reactions_by_fid(req).await }),
                )
                .await
            }
            (&Method::GET, "/v1/reactionsByCast") => {
                self.handle_request::<ReactionsByCastRequest, PagedResponse, _>(
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
                self.handle_request::<LinkRequest, Message, _>(req, |service, req| {
                    Box::pin(async move { service.get_link_by_id(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/linksByFid") => {
                self.handle_request::<LinksByFidRequest, PagedResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_links_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/linksByTargetFid") => {
                // For linksByTargetFid we assume that the service uses a FidTimestampRequest
                // (similar to castsByFid) to return all link messages for a target fid.
                self.handle_request::<LinksByTargetRequest, PagedResponse, _>(
                    req,
                    |service, req| {
                        Box::pin(async move { service.get_links_by_target_fid(req).await })
                    },
                )
                .await
            }
            // missing user_data_type
            (&Method::GET, "/v1/userDataByFid") => {
                self.handle_request::<FidRequest, PagedResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_user_data_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/storageLimitsByFid") => {
                self.handle_request::<FidRequest, StorageLimitsResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_storage_limits_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/userNameProofByName") => {
                self.handle_request::<UsernameProofRequest, UserNameProof, _>(
                    req,
                    |service, req| {
                        Box::pin(async move { service.get_user_name_proof_by_name(req).await })
                    },
                )
                .await
            }
            (&Method::GET, "/v1/userNameProofsByFid") => {
                self.handle_request::<FidRequest, UsernameProofsResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_user_name_proofs_by_fid(req).await })
                })
                .await
            }
            (&Method::POST, "/v1/validateMessage") => {
                self.handle_protobuf_request::<ValidationResult, _>(
                    req,
                    |service, _headers, req| {
                        Box::pin(async move { service.validate_message(req).await })
                    },
                )
                .await
            }
            (&Method::POST, "/v1/submitMessage") => {
                self.handle_protobuf_request::<Message, _>(req, |service, headers, req| {
                    Box::pin(async move { service.submit_message(req, headers).await })
                })
                .await
            }
            // Missing address
            (&Method::GET, "/v1/verificationsByFid") => {
                self.handle_request::<FidRequest, PagedResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_verifications_by_fid(req).await })
                })
                .await
            }
            // Missing signer
            (&Method::GET, "/v1/onChainSignersByFid") => {
                self.handle_request::<FidRequest, OnChainEventResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_on_chain_signers_by_fid(req).await })
                })
                .await
            }
            (&Method::GET, "/v1/onChainEventsByFid") => {
                self.handle_request::<OnChainEventRequest, OnChainEventResponse, _>(
                    req,
                    |service, req| {
                        Box::pin(async move { service.get_on_chain_events_by_fid(req).await })
                    },
                )
                .await
            }
            (&Method::GET, "/v1/events") => {
                self.handle_request::<EventsRequest, EventsResponse, _>(req, |service, req| {
                    Box::pin(async move { service.get_events(req).await })
                })
                .await
            }
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("Not Found")).boxed())
                .unwrap()),
        }
    }

    async fn handle_protobuf_request<Resp, F>(
        &self,
        req: Request<hyper::body::Incoming>,
        handler: impl FnOnce(Arc<HubHttpServiceImpl>, HeaderMap<HeaderValue>, proto::Message) -> F,
    ) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible>
    where
        Resp: Serialize,
        F: Future<Output = Result<Resp, ErrorResponse>>,
    {
        let content_type = req.headers().get("content-type");
        if content_type.is_none() {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("Missing content type")).boxed())
                .unwrap());
        } else {
            let content_type_str = content_type.unwrap().to_str();
            if content_type_str.is_err() {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Missing content type")).boxed())
                    .unwrap());
            }

            let content_type_str = content_type_str.unwrap();
            if content_type_str != "application/octet-stream" {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(
                        Full::new(Bytes::from(format!(
                            "Invalid content type: {}",
                            content_type_str
                        )))
                        .boxed(),
                    )
                    .unwrap());
            }
        }

        let headers = req.headers().clone();

        // Parse request
        let req_obj = match self.parse_protobuf_request(req).await {
            Ok(req) => req,
            Err(resp) => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(resp.into_body()).boxed())
                    .unwrap())
            }
        };

        // Handle request
        match handler(self.service.clone(), headers, req_obj).await {
            Ok(resp) => Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(serde_json::to_vec(&resp).unwrap())).boxed())
                .unwrap()),
            Err(err) => Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(serde_json::to_vec(&err).unwrap())).boxed())
                .unwrap()),
        }
    }

    async fn handle_request<Req, Resp, F>(
        &self,
        req: Request<hyper::body::Incoming>,
        handler: impl FnOnce(Arc<HubHttpServiceImpl>, Req) -> F,
    ) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible>
    where
        Req: DeserializeOwned,
        Resp: Serialize,
        F: Future<Output = Result<Resp, ErrorResponse>>,
    {
        // Parse request
        let req_obj = match self.parse_request::<Req>(req).await {
            Ok(req) => req,
            Err(resp) => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(resp.into_body()).boxed())
                    .unwrap())
            }
        };

        // Handle request
        match handler(self.service.clone(), req_obj).await {
            Ok(resp) => Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(serde_json::to_vec(&resp).unwrap())).boxed())
                .unwrap()),
            Err(err) => Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(serde_json::to_vec(&err).unwrap())).boxed())
                .unwrap()),
        }
    }

    async fn parse_protobuf_request(
        &self,
        req: Request<hyper::body::Incoming>,
    ) -> Result<proto::Message, Response<Bytes>> {
        // For POST/PUT requests, parse body
        let body_bytes = req.collect().await;
        if body_bytes.is_err() {
            return Err(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Bytes::from(format!(
                    "Internal server error: {}",
                    body_bytes.unwrap_err().to_string()
                )))
                .unwrap());
        }

        match message_decode(&body_bytes.unwrap().to_bytes().slice(..)) {
            Ok(parsed) => Ok(parsed),
            Err(e) => Err(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Bytes::from(format!("Invalid protobuf data: {}", e)))
                .unwrap()),
        }
    }

    async fn parse_request<T: DeserializeOwned>(
        &self,
        req: Request<hyper::body::Incoming>,
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
        let body_bytes = req.collect().await;
        if body_bytes.is_err() {
            return Err(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Bytes::from(format!(
                    "Internal server error: {}",
                    body_bytes.unwrap_err().to_string()
                )))
                .unwrap());
        }

        match serde_json::from_slice(&body_bytes.unwrap().to_bytes().slice(..)) {
            Ok(parsed) => Ok(parsed),
            Err(e) => Err(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Bytes::from(format!("Invalid request format: {}", e)))
                .unwrap()),
        }
    }
}
