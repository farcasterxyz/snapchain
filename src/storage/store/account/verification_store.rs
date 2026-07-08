use super::{
    make_fid_key, make_user_key, read_fid_key,
    store::{Store, StoreDef},
    MessagesPage, StoreEventHandler, FID_BYTES, TS_HASH_LENGTH,
};
use crate::storage::util::increment_vec_u8;
use crate::{
    core::error::HubError,
    proto::{Protocol, SignatureScheme, VerificationAddAddressBody, VerificationRemoveBody},
    storage::store::account::StoreOptions,
};
use crate::{proto::message_data::Body, storage::db::PageOptions};
use crate::{
    proto::MessageData,
    storage::constants::{RootPrefix, UserPostfix},
};
use crate::{
    proto::{Message, MessageType},
    storage::db::{RocksDB, RocksDbTransactionBatch},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct VerificationStoreDef {
    prune_size_limit: u32,
}

impl StoreDef for VerificationStoreDef {
    #[inline]
    fn postfix(&self) -> u8 {
        UserPostfix::VerificationMessage as u8
    }

    #[inline]
    fn add_message_type(&self) -> u8 {
        MessageType::VerificationAddEthAddress as u8
    }

    #[inline]
    fn remove_message_type(&self) -> u8 {
        MessageType::VerificationRemove as u8
    }

    #[inline]
    fn compact_state_message_type(&self) -> u8 {
        MessageType::None as u8
    }

    #[inline]
    fn is_compact_state_type(&self, _message: &Message) -> bool {
        false
    }

    #[inline]
    fn is_add_type(&self, message: &Message) -> bool {
        if message.data.is_none() {
            return false;
        }
        let data = message.data.as_ref().unwrap();
        message.signature_scheme == SignatureScheme::Ed25519 as i32
            && data.r#type == MessageType::VerificationAddEthAddress as i32
            && data.body.is_some()
            && match data.body.as_ref().unwrap() {
                Body::VerificationAddAddressBody(body) => {
                    body.protocol == Protocol::Ethereum as i32
                        || body.protocol == Protocol::Solana as i32
                }
                _ => false,
            }
    }

    #[inline]
    fn is_remove_type(&self, message: &Message) -> bool {
        if message.data.is_none() {
            return false;
        }
        let data = message.data.as_ref().unwrap();
        message.signature_scheme == SignatureScheme::Ed25519 as i32
            && data.r#type == MessageType::VerificationRemove as i32
            && data.body.is_some()
            && match data.body.as_ref().unwrap() {
                Body::VerificationRemoveBody(body) => {
                    body.protocol == Protocol::Ethereum as i32
                        || body.protocol == Protocol::Solana as i32
                }
                _ => false,
            }
    }

    #[inline]
    fn build_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        _ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        let address = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::VerificationAddAddressBody(body) => &body.address,
            _ => {
                return Err(HubError {
                    code: "bad_request.invalid_param".to_string(),
                    message: "address empty".to_string(),
                })
            }
        };

        if address.is_empty() {
            return Err(HubError {
                code: "bad_request.invalid_param".to_string(),
                message: "address empty".to_string(),
            });
        }

        let by_address_key =
            Self::make_verification_by_address_key(address, message.data.as_ref().unwrap().fid);
        txn.put(by_address_key, _ts_hash.to_vec());

        Ok(())
    }

    #[inline]
    fn delete_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        _ts_hash: &[u8; TS_HASH_LENGTH],
        message: &Message,
    ) -> Result<(), HubError> {
        let address = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::VerificationAddAddressBody(body) => &body.address,
            _ => {
                return Err(HubError {
                    code: "bad_request.invalid_param".to_string(),
                    message: "address empty".to_string(),
                })
            }
        };

        if address.is_empty() {
            return Err(HubError {
                code: "bad_request.invalid_param".to_string(),
                message: "address empty".to_string(),
            });
        }

        let by_address_key =
            Self::make_verification_by_address_key(address, message.data.as_ref().unwrap().fid);
        txn.delete(by_address_key);

        Ok(())
    }

    #[inline]
    fn make_add_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        let address = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::VerificationAddAddressBody(body) => &body.address,
            Body::VerificationRemoveBody(body) => &body.address,
            _ => {
                return Err(HubError {
                    code: "bad_request.validation_failure".to_string(),
                    message: "Invalid verification body".to_string(),
                })
            }
        };

        Ok(Self::make_verification_adds_key(
            message.data.as_ref().unwrap().fid,
            address,
        ))
    }

    #[inline]
    fn make_remove_key(&self, message: &Message) -> Result<Vec<u8>, HubError> {
        let address = match message.data.as_ref().unwrap().body.as_ref().unwrap() {
            Body::VerificationAddAddressBody(body) => &body.address,
            Body::VerificationRemoveBody(body) => &body.address,
            _ => {
                return Err(HubError {
                    code: "bad_request.validation_failure".to_string(),
                    message: "Invalid verification body".to_string(),
                })
            }
        };

        Ok(Self::make_verification_removes_key(
            message.data.as_ref().unwrap().fid,
            address,
        ))
    }

    #[inline]
    fn make_compact_state_add_key(&self, _message: &Message) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "Verification Store doesn't support compact state".to_string(),
        })
    }

    #[inline]
    fn make_compact_state_prefix(&self, _fid: u64) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "Verification Store doesn't support compact state".to_string(),
        })
    }

    #[inline]
    fn get_prune_size_limit(&self) -> u32 {
        self.prune_size_limit
    }
}

impl VerificationStoreDef {
    #[inline]
    pub fn make_verification_by_address_prefix(address: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + address.len());

        key.push(RootPrefix::VerificationByAddress as u8);
        key.extend_from_slice(address);
        key
    }

    #[inline]
    pub fn make_verification_by_address_key(address: &[u8], fid: u64) -> Vec<u8> {
        let mut key = Self::make_verification_by_address_prefix(address);
        key.extend_from_slice(&make_fid_key(fid));
        key
    }

    #[inline]
    pub fn make_verification_adds_key(fid: u64, address: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(33 + 1 + address.len());
        key.extend_from_slice(&make_user_key(fid));
        key.push(UserPostfix::VerificationAdds as u8);
        key.extend_from_slice(address);
        key
    }

    #[inline]
    pub fn make_verification_removes_key(fid: u64, address: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(33 + 1 + address.len());
        key.extend_from_slice(&make_user_key(fid));
        key.push(UserPostfix::VerificationRemoves as u8);
        key.extend_from_slice(address);
        key
    }
}

pub struct VerificationStore {}

impl VerificationStore {
    pub fn new(
        db: Arc<RocksDB>,
        store_event_handler: Arc<StoreEventHandler>,
        prune_size_limit: u32,
    ) -> Store<VerificationStoreDef> {
        Store::new_with_store_def(
            db,
            store_event_handler,
            VerificationStoreDef { prune_size_limit },
        )
    }

    pub fn new_with_opts(
        db: Arc<RocksDB>,
        store_event_handler: Arc<StoreEventHandler>,
        prune_size_limit: u32,
        store_opts: StoreOptions,
    ) -> Store<VerificationStoreDef> {
        Store::new_with_store_def_opts(
            db,
            store_event_handler,
            VerificationStoreDef { prune_size_limit },
            store_opts,
        )
    }

    pub fn get_verification_add(
        store: &Store<VerificationStoreDef>,
        fid: u64,
        address: &[u8],
        maybe_txn: Option<&RocksDbTransactionBatch>,
    ) -> Result<Option<Message>, HubError> {
        let partial_message = Message {
            data: Some(MessageData {
                fid,
                r#type: MessageType::VerificationAddEthAddress.into(),
                body: Some(Body::VerificationAddAddressBody(
                    VerificationAddAddressBody {
                        address: address.to_vec(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }),
            ..Default::default()
        };

        store.get_add(&partial_message, maybe_txn)
    }

    pub fn get_verification_remove(
        store: &Store<VerificationStoreDef>,
        fid: u64,
        address: &[u8],
    ) -> Result<Option<Message>, HubError> {
        let partial_message = Message {
            data: Some(MessageData {
                fid,
                r#type: MessageType::VerificationRemove.into(),
                body: Some(Body::VerificationRemoveBody(VerificationRemoveBody {
                    address: address.to_vec(),
                    ..Default::default()
                })),
                ..Default::default()
            }),
            ..Default::default()
        };

        store.get_remove(&partial_message)
    }

    pub fn get_verifications_by_address(
        store: &Store<VerificationStoreDef>,
        address: &[u8],
    ) -> Result<Vec<(u64, [u8; TS_HASH_LENGTH])>, HubError> {
        let prefix = VerificationStoreDef::make_verification_by_address_prefix(address);
        let stop_prefix = increment_vec_u8(&prefix);
        let mut entries = Vec::new();

        store.db().for_each_iterator_by_prefix(
            Some(prefix.clone()),
            Some(stop_prefix),
            &PageOptions::default(),
            |key, value| {
                if key.len() != prefix.len() + FID_BYTES {
                    return Err(HubError::internal_db_error(&format!(
                        "invalid verification by address key length: {}",
                        key.len()
                    )));
                }

                if value.len() != TS_HASH_LENGTH {
                    return Err(HubError::internal_db_error(&format!(
                        "invalid verification by address value length: {}",
                        value.len()
                    )));
                }

                let fid = read_fid_key(key, prefix.len());
                let mut ts_hash = [0u8; TS_HASH_LENGTH];
                ts_hash.copy_from_slice(value);
                entries.push((fid, ts_hash));

                Ok(false)
            },
        )?;

        Ok(entries)
    }

    #[inline]
    pub fn get_verification_adds_by_fid(
        store: &Store<VerificationStoreDef>,
        fid: u64,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        store.get_adds_by_fid::<fn(&Message) -> bool>(fid, page_options, None)
    }

    #[inline]
    pub fn get_verification_removes_by_fid(
        store: &Store<VerificationStoreDef>,
        fid: u64,
        page_options: &PageOptions,
    ) -> Result<MessagesPage, HubError> {
        store.get_removes_by_fid::<fn(&Message) -> bool>(fid, page_options, None)
    }
}
