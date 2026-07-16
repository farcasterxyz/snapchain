use super::{make_user_key, Store, StoreDef, StoreEventHandler};
use crate::core::error::HubError;
use crate::proto::{message_data::Body, Message, MessageType, SignatureScheme};
use crate::storage::constants::UserPostfix;
use crate::storage::db::RocksDB;
use std::sync::Arc;

#[derive(Clone)]
pub struct ChannelUpdateStoreDef {
    prune_size_limit: u32,
}

#[derive(Clone)]
pub struct ChannelMemberStoreDef {
    prune_size_limit: u32,
}

#[derive(Clone)]
pub struct ChannelPinStoreDef {
    prune_size_limit: u32,
}

#[derive(Clone)]
pub struct ChannelModerateStoreDef {
    prune_size_limit: u32,
}

fn invalid_body(store_name: &str) -> HubError {
    HubError::validation_failure(&format!("invalid {store_name} body"))
}

fn unsupported(store_name: &str) -> HubError {
    HubError::invalid_parameter(&format!("{store_name} does not support this operation"))
}

fn is_channel_message(message: &Message, expected_type: MessageType) -> bool {
    let Some(data) = message.data.as_ref() else {
        return false;
    };
    if message.signature_scheme != SignatureScheme::Ed25519 as i32
        || data.r#type != expected_type as i32
    {
        return false;
    }

    matches!(
        (expected_type, data.body.as_ref()),
        (MessageType::ChannelUpdate, Some(Body::ChannelUpdateBody(_)))
            | (MessageType::ChannelMember, Some(Body::ChannelMemberBody(_)))
            | (MessageType::ChannelPin, Some(Body::ChannelPinBody(_)))
            | (
                MessageType::ChannelModerate,
                Some(Body::ChannelModerateBody(_))
            )
    )
}

fn author_slot_key(
    message: &Message,
    index_postfix: UserPostfix,
    slot_suffix: &[u8],
) -> Result<Vec<u8>, HubError> {
    let data = message
        .data
        .as_ref()
        .ok_or_else(|| HubError::validation_failure("message data is missing"))?;
    let mut key = make_user_key(data.fid);
    key.push(index_postfix.as_u8());
    key.extend_from_slice(slot_suffix);
    Ok(key)
}

fn update_slot_suffix(message: &Message) -> Result<Vec<u8>, HubError> {
    match message.data.as_ref().and_then(|data| data.body.as_ref()) {
        Some(Body::ChannelUpdateBody(body)) => Ok(body.channel_id.clone()),
        _ => Err(invalid_body("ChannelUpdate")),
    }
}

fn member_slot_suffix(message: &Message) -> Result<Vec<u8>, HubError> {
    match message.data.as_ref().and_then(|data| data.body.as_ref()) {
        Some(Body::ChannelMemberBody(body)) => {
            let mut suffix = body.channel_id.clone();
            let target_fid = u32::try_from(body.fid)
                .map_err(|_| HubError::invalid_parameter("channel member fid exceeds u32"))?;
            suffix.extend_from_slice(&target_fid.to_be_bytes());
            Ok(suffix)
        }
        _ => Err(invalid_body("ChannelMember")),
    }
}

fn pin_slot_suffix(message: &Message) -> Result<Vec<u8>, HubError> {
    match message.data.as_ref().and_then(|data| data.body.as_ref()) {
        Some(Body::ChannelPinBody(body)) => Ok(body.channel_id.clone()),
        _ => Err(invalid_body("ChannelPin")),
    }
}

fn moderate_slot_suffix(message: &Message) -> Result<Vec<u8>, HubError> {
    match message.data.as_ref().and_then(|data| data.body.as_ref()) {
        Some(Body::ChannelModerateBody(body)) => {
            let mut suffix = body.channel_id.clone();
            suffix.extend_from_slice(&body.cast_hash);
            Ok(suffix)
        }
        _ => Err(invalid_body("ChannelModerate")),
    }
}

macro_rules! impl_channel_store_def {
    (
        $def:ty,
        $store_name:literal,
        $message_postfix:expr,
        $index_postfix:expr,
        $message_type:expr,
        $slot_suffix:ident
    ) => {
        impl StoreDef for $def {
            fn postfix(&self) -> u8 {
                $message_postfix.as_u8()
            }

            fn add_message_type(&self) -> u8 {
                $message_type as u8
            }

            fn remove_message_type(&self) -> u8 {
                MessageType::None as u8
            }

            fn compact_state_message_type(&self) -> u8 {
                MessageType::None as u8
            }

            fn is_add_type(&self, message: &Message) -> bool {
                is_channel_message(message, $message_type)
            }

            fn is_remove_type(&self, _message: &Message) -> bool {
                false
            }

            fn is_compact_state_type(&self, _message: &Message) -> bool {
                false
            }

            fn make_add_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
                author_slot_key(message, $index_postfix, &$slot_suffix(message)?)
            }

            fn make_remove_key(&self, _message: &Message) -> Result<Vec<u8>, HubError> {
                Err(unsupported($store_name))
            }

            fn make_compact_state_add_key(&self, _message: &Message) -> Result<Vec<u8>, HubError> {
                Err(unsupported($store_name))
            }

            fn make_compact_state_prefix(&self, _fid: u64) -> Result<Vec<u8>, HubError> {
                Err(unsupported($store_name))
            }

            fn get_prune_size_limit(&self) -> u32 {
                self.prune_size_limit
            }
        }
    };
}

impl_channel_store_def!(
    ChannelUpdateStoreDef,
    "ChannelUpdateStore",
    UserPostfix::ChannelUpdateMessage,
    UserPostfix::ChannelUpdateAdds,
    MessageType::ChannelUpdate,
    update_slot_suffix
);
impl_channel_store_def!(
    ChannelMemberStoreDef,
    "ChannelMemberStore",
    UserPostfix::ChannelMemberMessage,
    UserPostfix::ChannelMemberAdds,
    MessageType::ChannelMember,
    member_slot_suffix
);
impl_channel_store_def!(
    ChannelPinStoreDef,
    "ChannelPinStore",
    UserPostfix::ChannelPinMessage,
    UserPostfix::ChannelPinAdds,
    MessageType::ChannelPin,
    pin_slot_suffix
);
impl_channel_store_def!(
    ChannelModerateStoreDef,
    "ChannelModerateStore",
    UserPostfix::ChannelModerateMessage,
    UserPostfix::ChannelModerateAdds,
    MessageType::ChannelModerate,
    moderate_slot_suffix
);

macro_rules! define_channel_store {
    ($store:ident, $def:ident) => {
        pub struct $store;

        impl $store {
            pub fn new(
                db: Arc<RocksDB>,
                store_event_handler: Arc<StoreEventHandler>,
                prune_size_limit: u32,
            ) -> Store<$def> {
                Store::new_with_store_def(db, store_event_handler, $def { prune_size_limit })
            }
        }
    };
}

define_channel_store!(ChannelUpdateStore, ChannelUpdateStoreDef);
define_channel_store!(ChannelMemberStore, ChannelMemberStoreDef);
define_channel_store!(ChannelPinStore, ChannelPinStoreDef);
define_channel_store!(ChannelModerateStore, ChannelModerateStoreDef);
