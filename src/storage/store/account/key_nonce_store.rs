//! Nonce counters for gasless-key messages (KEY_ADD / KEY_REMOVE replay protection).
//!
//! Distinct from on-chain signer events, which do not use this store. Two independent counter
//! namespaces live under `RootPrefix::GaslessKey`:
//!
//! * **User nonce** — scoped by the acting FID. Used by `KEY_ADD` and custody-signed
//!   `KEY_REMOVE` (signature_type = 1). A single signer rotation sequence shares this counter.
//! * **App nonce** — scoped by the app's FID (the verified `requestFid` recovered from
//!   `SignedKeyRequestMetadata`). Used by self-revocation `KEY_REMOVE` (signature_type = 2) so an
//!   app can rotate/revoke its own authorized keys without consuming the user's custody nonce.
//!
//! Rejection rule is identical for both namespaces: the incoming nonce must be **strictly greater
//! than** the stored counter. A missing key is treated as stored = 0, so the first accepted nonce
//! must be > 0.
//!
//! Storage layout:
//! ```text
//! [RootPrefix::GaslessKey (1B)] [UserPostfix::GaslessKey{User|App}Nonce (1B)] [FID (4B, BE)]
//!     -> u32 nonce (4B big-endian)
//! ```

use super::{get_from_db_or_txn, make_fid_key, FID_BYTES};
use crate::core::error::HubError;
use crate::storage::{
    constants::{RootPrefix, UserPostfix},
    db::{RocksDB, RocksDbTransactionBatch},
};

// Byte sizes for the key layout. `RootPrefix` and `UserPostfix` are `repr(u8)` discriminators so
// each contributes one byte. FID is downcast to `u32` and written big-endian via `make_fid_key`.
const ROOT_PREFIX_BYTES: usize = 1;
const USER_POSTFIX_BYTES: usize = 1;
const NONCE_KEY_BYTES: usize = ROOT_PREFIX_BYTES + USER_POSTFIX_BYTES + FID_BYTES;

// Value is a single big-endian u32.
const NONCE_VALUE_BYTES: usize = 4;

fn make_nonce_key(postfix: UserPostfix, fid: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(NONCE_KEY_BYTES);
    key.push(RootPrefix::GaslessKey as u8);
    key.push(postfix as u8);
    key.extend_from_slice(&make_fid_key(fid));
    key
}

pub fn make_user_nonce_key(fid: u64) -> Vec<u8> {
    make_nonce_key(UserPostfix::GaslessKeyUserNonce, fid)
}

pub fn make_app_nonce_key(app_fid: u64) -> Vec<u8> {
    make_nonce_key(UserPostfix::GaslessKeyAppNonce, app_fid)
}

fn get_nonce(
    db: &RocksDB,
    txn: &RocksDbTransactionBatch,
    key: &[u8],
) -> Result<Option<u32>, HubError> {
    match get_from_db_or_txn(db, txn, key)? {
        None => Ok(None),
        Some(bytes) => {
            if bytes.len() != NONCE_VALUE_BYTES {
                return Err(HubError {
                    code: "internal_error".to_string(),
                    message: format!(
                        "corrupt gasless-key nonce value: expected {} bytes, got {}",
                        NONCE_VALUE_BYTES,
                        bytes.len()
                    ),
                });
            }
            let mut buf = [0u8; NONCE_VALUE_BYTES];
            buf.copy_from_slice(&bytes);
            Ok(Some(u32::from_be_bytes(buf)))
        }
    }
}

pub fn get_user_nonce(
    db: &RocksDB,
    txn: &RocksDbTransactionBatch,
    fid: u64,
) -> Result<Option<u32>, HubError> {
    get_nonce(db, txn, &make_user_nonce_key(fid))
}

pub fn get_app_nonce(
    db: &RocksDB,
    txn: &RocksDbTransactionBatch,
    app_fid: u64,
) -> Result<Option<u32>, HubError> {
    get_nonce(db, txn, &make_app_nonce_key(app_fid))
}

fn check_and_set_nonce(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    key: Vec<u8>,
    new_nonce: u32,
) -> Result<(), HubError> {
    let stored = get_nonce(db, txn, &key)?.unwrap_or(0);
    if new_nonce <= stored {
        return Err(HubError {
            code: "bad_request.conflict".to_string(),
            message: format!(
                "gasless-key nonce {} is not greater than stored nonce {}",
                new_nonce, stored
            ),
        });
    }
    txn.put(key, new_nonce.to_be_bytes().to_vec());
    Ok(())
}

/// Validates `new_nonce > stored user nonce for fid` and stages the update on `txn`. The caller
/// commits the txn batch when the surrounding message is successfully merged; on rollback the
/// counter remains unchanged.
pub fn check_and_set_user_nonce(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    fid: u64,
    new_nonce: u32,
) -> Result<(), HubError> {
    check_and_set_nonce(db, txn, make_user_nonce_key(fid), new_nonce)
}

/// Same CAS semantics as `check_and_set_user_nonce` but against the app-nonce namespace, scoped
/// by the verified `requestFid` from `SignedKeyRequestMetadata`.
pub fn check_and_set_app_nonce(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    app_fid: u64,
    new_nonce: u32,
) -> Result<(), HubError> {
    check_and_set_nonce(db, txn, make_app_nonce_key(app_fid), new_nonce)
}
