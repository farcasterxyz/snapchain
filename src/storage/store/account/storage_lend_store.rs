use super::{
    make_user_key,
    store::{Store, StoreDef},
    StoreEventHandler,
};
use crate::storage::{constants::UserPostfix, db::PageOptions, store::account::StorageSlot};
use crate::{
    core::error::HubError,
    proto::SignatureScheme,
    storage::{
        constants::RootPrefix,
        store::account::{make_fid_key, TRUE_VALUE, TS_HASH_LENGTH},
    },
};
use crate::{proto::message_data::Body, storage::store::account::OnchainEventStore};
use crate::{
    proto::MessageType,
    storage::db::{RocksDB, RocksDbTransactionBatch},
};
use crate::{
    proto::{self},
    storage::store::account::StoreOptions,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct StorageLendStoreDef {
    prune_size_limit: u32,
}

impl StoreDef for StorageLendStoreDef {
    #[inline]
    fn postfix(&self) -> u8 {
        UserPostfix::LendStorageMessage as u8
    }

    #[inline]
    fn add_message_type(&self) -> u8 {
        MessageType::LendStorage as u8
    }

    #[inline]
    fn remove_message_type(&self) -> u8 {
        MessageType::None as u8
    }

    #[inline]
    fn is_add_type(&self, message: &proto::Message) -> bool {
        if message.data.is_none() {
            return false;
        }
        let data = message.data.as_ref().unwrap();
        message.signature_scheme == SignatureScheme::Ed25519 as i32
            && data.r#type == MessageType::LendStorage as i32
            && data.body.is_some()
    }

    #[inline]
    fn is_remove_type(&self, _message: &proto::Message) -> bool {
        false
    }

    #[inline]
    fn compact_state_message_type(&self) -> u8 {
        MessageType::None as u8
    }

    #[inline]
    fn is_compact_state_type(&self, _message: &proto::Message) -> bool {
        false
    }

    #[inline]
    fn make_add_key(&self, message: &proto::Message) -> Result<Vec<u8>, HubError> {
        let to_fid = match &message.data.as_ref().unwrap().body {
            Some(Body::LendStorageBody(lend_storage_body)) => lend_storage_body.to_fid,
            _ => {
                return Err(HubError {
                    code: "bad_request.invalid_param".to_string(),
                    message: "LendStorage message missing body".to_string(),
                })
            }
        };
        let key = Self::make_storage_lend_primary_key(message.data.as_ref().unwrap().fid, to_fid);
        Ok(key)
    }

    fn build_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        _ts_hash: &[u8; TS_HASH_LENGTH],
        message: &proto::Message,
    ) -> Result<(), HubError> {
        if let Ok(to_fid_key) = Self::by_to_fid_secondary_index_key(message) {
            txn.put(to_fid_key, vec![TRUE_VALUE]);
        }
        Ok(())
    }

    fn delete_secondary_indices(
        &self,
        txn: &mut RocksDbTransactionBatch,
        _ts_hash: &[u8; TS_HASH_LENGTH],
        message: &proto::Message,
    ) -> Result<(), HubError> {
        let to_fid_key = Self::by_to_fid_secondary_index_key(message);

        if let Ok(to_fid_key) = to_fid_key {
            txn.delete(to_fid_key);
        }

        Ok(())
    }

    #[inline]
    fn make_remove_key(&self, _message: &proto::Message) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "removes not supported".to_string(),
        })
    }

    #[inline]
    fn make_compact_state_add_key(&self, _message: &proto::Message) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "UserDataStore doesn't support compact state".to_string(),
        })
    }

    #[inline]
    fn make_compact_state_prefix(&self, _fid: u64) -> Result<Vec<u8>, HubError> {
        Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: "UserDataStore doesn't support compact state".to_string(),
        })
    }

    #[inline]
    fn get_prune_size_limit(&self) -> u32 {
        self.prune_size_limit
    }
}

impl StorageLendStoreDef {
    #[inline]
    fn make_storage_lend_primary_key(from_fid: u64, to_fid: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(33 + 1);

        key.extend_from_slice(&make_user_key(from_fid));
        key.push(UserPostfix::UserDataAdds as u8);
        // TODO(aditi): Maybe we want to use ts hash
        key.extend_from_slice(&make_fid_key(to_fid));

        key
    }

    fn by_to_fid_secondary_index_key(message: &proto::Message) -> Result<Vec<u8>, HubError> {
        let to_fid = match &message.data.as_ref().unwrap().body {
            Some(Body::LendStorageBody(lend_storage_body)) => lend_storage_body.to_fid,
            _ => {
                return Err(HubError {
                    code: "bad_request.invalid_param".to_string(),
                    message: "LendStorage message missing body".to_string(),
                })
            }
        };

        // TODO(aditi): Maybe we want to use ts hash
        let mut key = Vec::with_capacity(1 + 28 + 24 + 4);
        key.push(RootPrefix::LendStorageByRecipient as u8);
        key.extend_from_slice(&make_fid_key(to_fid));
        key.extend_from_slice(&make_fid_key(message.fid()));
        Ok(key)
    }
}

pub struct StorageLendStore {}

impl StorageLendStore {
    pub fn new(
        db: Arc<RocksDB>,
        store_event_handler: Arc<StoreEventHandler>,
        prune_size_limit: u32,
    ) -> Store<StorageLendStoreDef> {
        Store::new_with_store_def(
            db,
            store_event_handler,
            StorageLendStoreDef { prune_size_limit },
        )
    }

    pub fn new_with_opts(
        db: Arc<RocksDB>,
        store_event_handler: Arc<StoreEventHandler>,
        prune_size_limit: u32,
        options: StoreOptions,
    ) -> Store<StorageLendStoreDef> {
        Store::new_with_store_def_opts(
            db,
            store_event_handler,
            StorageLendStoreDef { prune_size_limit },
            options,
        )
    }

    pub fn get_lent_from_storage(
        store: &Store<StorageLendStoreDef>,
        fid: u64,
    ) -> Result<StorageSlot, HubError> {
        let mut next_page_token = None;
        let mut storage_slot = StorageSlot::new(0, 0, 0, 0);
        loop {
            let page = store.get_adds_by_fid::<fn(&proto::Message) -> bool>(
                fid,
                &PageOptions {
                    page_size: None,
                    page_token: next_page_token,
                    reverse: false,
                },
                None,
            )?;

            for storage_lend in page.messages {
                match &storage_lend.data.as_ref().unwrap().body.as_ref().unwrap() {
                    Body::LendStorageBody(lend_storage_body) => {
                        storage_slot.merge(&StorageSlot::from_storage_lend(lend_storage_body));
                    }
                    _ => (),
                }
            }

            if page.next_page_token.is_none() {
                break;
            }

            next_page_token = page.next_page_token
        }

        Ok(storage_slot)
    }
}
