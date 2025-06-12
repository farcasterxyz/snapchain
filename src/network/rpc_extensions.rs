use crate::core::error::HubError;
use crate::proto;
use crate::proto::{
    CastsByParentRequest, FidRequest, FidTimestampRequest, LinksByFidRequest, ReactionsByFidRequest,
};
use crate::storage::db::PageOptions;
use crate::storage::store::account::MessagesPage;
use base64::Engine;
use std::collections::HashMap;
use tonic::{Request, Response, Status};

// Extension traits to maps GRPC structs to internal structs
pub trait AsMessagesResponse {
    fn as_response(&self) -> Result<Response<proto::MessagesResponse>, Status>;
}

impl AsMessagesResponse for Result<MessagesPage, HubError> {
    fn as_response(&self) -> Result<Response<proto::MessagesResponse>, Status> {
        match self {
            Ok(page) => Ok(Response::new(proto::MessagesResponse {
                messages: page.messages.clone(),
                next_page_token: page.next_page_token.clone(),
            })),
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }
}

pub trait AsSingleMessageResponse {
    fn as_response(&self) -> Result<Response<proto::Message>, Status>;
}

impl AsSingleMessageResponse for Result<Option<proto::Message>, HubError> {
    fn as_response(&self) -> Result<Response<proto::Message>, Status> {
        match self {
            Ok(Some(message)) => Ok(Response::new(message.clone())),
            Ok(None) => Err(Status::not_found("cast not found")),
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }
}

impl AsSingleMessageResponse for Result<proto::Message, HubError> {
    fn as_response(&self) -> Result<Response<proto::Message>, Status> {
        match self {
            Ok(message) => Ok(Response::new(message.clone())),
            Err(err) => {
                if &err.code == "not_found" {
                    Err(Status::not_found(&err.message))
                } else {
                    Err(Status::internal(&err.to_string()))
                }
            }
        }
    }
}

fn page_options(
    page_size: Option<u32>,
    page_token: Option<Vec<u8>>,
    reverse: Option<bool>,
) -> PageOptions {
    let page_size = match page_size {
        Some(size) => Some(size as usize),
        None => None,
    };
    let reverse = reverse.unwrap_or(false);
    let token = match page_token {
        None => None,
        Some(token) if token.is_empty() => None, // Make sure we don't use the empty key for pagination
        Some(token) => Some(token.clone()),
    };
    PageOptions {
        page_size,
        page_token: token,
        reverse,
    }
}

impl FidRequest {
    pub fn page_options(&self) -> PageOptions {
        page_options(self.page_size, self.page_token.clone(), self.reverse)
    }
}

impl FidTimestampRequest {
    pub fn page_options(&self) -> PageOptions {
        page_options(self.page_size, self.page_token.clone(), self.reverse)
    }

    pub fn timestamps(&self) -> (Option<u32>, Option<u32>) {
        let start_timestamp = match self.start_timestamp {
            Some(ts) => Some(ts as u32),
            None => None,
        };
        let stop_timestamp = match self.stop_timestamp {
            Some(ts) => Some(ts as u32),
            None => None,
        };
        (start_timestamp, stop_timestamp)
    }
}

impl CastsByParentRequest {
    pub fn page_options(&self) -> PageOptions {
        page_options(self.page_size, self.page_token.clone(), self.reverse)
    }
}

impl ReactionsByFidRequest {
    pub fn page_options(&self) -> PageOptions {
        page_options(self.page_size, self.page_token.clone(), self.reverse)
    }
}

impl LinksByFidRequest {
    pub fn page_options(&self) -> PageOptions {
        page_options(self.page_size, self.page_token.clone(), self.reverse)
    }
}

pub fn authenticate_request<T>(
    request: &Request<T>,
    allowed_users: &HashMap<String, String>,
) -> Result<(), Status> {
    if allowed_users.is_empty() {
        return Ok(());
    }

    let metadata_map = request.metadata();
    if let Some(auth) = metadata_map.get("authorization") {
        let auth = auth
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization header is not a valid string"))?;
        let parts: Vec<&str> = auth.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(Status::unauthenticated("invalid authorization header"));
        }
        if parts[0] != "Basic" {
            return Err(Status::unauthenticated("invalid authorization header"));
        }
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(parts[1])
            .map_err(|_| {
                Status::unauthenticated("authorization header is not a valid base64 string")
            })?;
        let decoded = String::from_utf8(decoded).map_err(|_| {
            Status::unauthenticated("authorization header is not a valid utf8 string")
        })?;
        let parts: Vec<&str> = decoded.split(":").collect();
        if parts.len() != 2 {
            return Err(Status::unauthenticated("invalid authorization header"));
        }
        if let Some(password) = allowed_users.get(parts[0]) {
            if password == parts[1] {
                Ok(())
            } else {
                Err(Status::unauthenticated("invalid username or password"))
            }
        } else {
            Err(Status::unauthenticated("invalid username or password"))
        }
    } else {
        Err(Status::unauthenticated("missing authorization header"))
    }
}
