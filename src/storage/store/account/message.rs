use super::PAGE_SIZE_MAX;
use crate::core::error::HubError;
use crate::core::types::FidOnDisk;
use crate::storage::constants::{RootPrefix, UserPostfix};
use crate::storage::db::{PageOptions, RocksdbError};
use crate::storage::util::increment_vec_u8;
use crate::version::version::{EngineVersion, ProtocolFeature};
use crate::{
    proto::{message_data::Body, CastId, Message as MessageProto, MessageData, MessageType},
    storage::db::{RocksDB, RocksDbTransactionBatch},
};
use prost::encoding::{
    decode_key, decode_varint, encode_key, encode_varint, skip_field, DecodeContext, WireType,
};
use prost::{DecodeError, Message as _};

pub const FID_BYTES: usize = 4;

pub const TS_HASH_LENGTH: usize = 24;
pub const HASH_LENGTH: usize = 20;

pub const TRUE_VALUE: u8 = 1;

/** A page of messages returned from various APIs */
pub struct MessagesPage {
    pub messages: Vec<MessageProto>,
    pub next_page_token: Option<Vec<u8>>,
}

pub trait IntoU8 {
    fn into_u8(self) -> u8;
}
impl IntoU8 for MessageType {
    fn into_u8(self) -> u8 {
        self as u8
    }
}

pub trait IntoI32 {
    fn into_i32(self) -> i32;
}

impl IntoI32 for MessageType {
    fn into_i32(self) -> i32 {
        self as i32
    }
}

/** Convert a specific message type (CastAdd / CastRemove) to a class of message (CastMessage) */
pub fn type_to_set_postfix(message_type: MessageType) -> Result<UserPostfix, HubError> {
    if message_type == MessageType::CastAdd || message_type == MessageType::CastRemove {
        return Ok(UserPostfix::CastMessage);
    }

    if message_type == MessageType::ReactionAdd || message_type == MessageType::ReactionRemove {
        return Ok(UserPostfix::ReactionMessage);
    }

    if message_type == MessageType::VerificationAddEthAddress
        || message_type == MessageType::VerificationRemove
    {
        return Ok(UserPostfix::VerificationMessage);
    }

    if message_type == MessageType::UserDataAdd {
        return Ok(UserPostfix::UserDataMessage);
    }

    if message_type == MessageType::LinkAdd || message_type == MessageType::LinkRemove {
        return Ok(UserPostfix::LinkMessage);
    }

    if message_type == MessageType::UsernameProof {
        return Ok(UserPostfix::UsernameProofMessage);
    }

    if message_type == MessageType::LendStorage {
        return Ok(UserPostfix::LendStorageMessage);
    }

    match message_type {
        MessageType::ChannelUpdate => return Ok(UserPostfix::ChannelUpdateMessage),
        MessageType::ChannelMember => return Ok(UserPostfix::ChannelMemberMessage),
        MessageType::ChannelPin => return Ok(UserPostfix::ChannelPinMessage),
        MessageType::ChannelModerate => return Ok(UserPostfix::ChannelModerateMessage),
        _ => {}
    }

    return Err(HubError {
        code: "internal_error".to_string(),
        message: format!(
            "unable to convert message type to set postfix: {}",
            message_type.as_str_name()
        )
        .to_string(),
    });
}

#[inline]
pub fn make_ts_hash(timestamp: u32, hash: &Vec<u8>) -> Result<[u8; TS_HASH_LENGTH], HubError> {
    // No need to check if timestamp > 2^32 because it's already a u32

    if hash.len() != HASH_LENGTH {
        return Err(HubError {
            code: "internal_error".to_string(),
            message: "hash length is not 20".to_string(),
        });
    }

    let mut ts_hash = [0u8; 24];
    // Store the timestamp as big-endian in the first 4 bytes
    ts_hash[0..4].copy_from_slice(&timestamp.to_be_bytes());
    // Store the hash in the remaining 20 bytes
    ts_hash[4..24].copy_from_slice(&hash[0..HASH_LENGTH]);

    Ok(ts_hash)
}

#[allow(dead_code)]
pub fn unpack_ts_hash(ts_hash: &[u8; TS_HASH_LENGTH]) -> (u32, [u8; HASH_LENGTH]) {
    let mut timestamp_bytes = [0u8; 4];
    timestamp_bytes.copy_from_slice(&ts_hash[0..4]);
    let timestamp = u32::from_be_bytes(timestamp_bytes);

    let mut hash = [0u8; HASH_LENGTH];
    hash.copy_from_slice(&ts_hash[4..24]);

    (timestamp, hash)
}

#[inline]
pub fn make_fid_key(fid: u64) -> Vec<u8> {
    // Downcast to u32, since on disk, we only assume 4 bytes for the fid to save space
    (fid as FidOnDisk).to_be_bytes().to_vec()
}

#[inline]
pub fn read_fid_key(key: &[u8], offset: usize) -> u64 {
    let mut fid_bytes = [0u8; FID_BYTES];
    fid_bytes.copy_from_slice(&key[offset..offset + FID_BYTES]);
    // Upcast to u64 so we are always dealing with the same type everywhere
    u32::from_be_bytes(fid_bytes) as u64
}

#[inline]
pub fn read_ts_hash(key: &[u8], offset: usize) -> [u8; TS_HASH_LENGTH] {
    let mut ts_hash = [0u8; TS_HASH_LENGTH];
    ts_hash.copy_from_slice(&key[offset..offset + TS_HASH_LENGTH]);
    ts_hash
}

#[inline]
pub fn make_user_key(fid: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 4);
    key.push(RootPrefix::User as u8);

    key.extend_from_slice(&make_fid_key(fid));

    key
}

#[inline]
pub fn make_message_primary_key(
    fid: u64,
    set: u8,
    ts_hash: Option<&[u8; TS_HASH_LENGTH]>,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 4 + 1 + TS_HASH_LENGTH);
    key.extend_from_slice(&make_user_key(fid));
    key.push(set);
    if ts_hash.is_some() {
        key.extend_from_slice(ts_hash.unwrap());
    }

    key
}

#[inline]
pub fn make_cast_id_key(cast_id: &CastId) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + HASH_LENGTH);
    key.extend_from_slice(&make_fid_key(cast_id.fid));
    key.extend_from_slice(&cast_id.hash);

    key
}

#[inline]
pub fn get_message(
    db: &RocksDB,
    txn: &RocksDbTransactionBatch,
    fid: u64,
    set: u8,
    ts_hash: &[u8; TS_HASH_LENGTH],
) -> Result<Option<MessageProto>, HubError> {
    let key = make_message_primary_key(fid, set, Some(ts_hash));
    get_message_by_key(db, txn, &key)
}

// We don't commit to the db until the end of the transaction, so, for cases where we might be handling conflicting messages within the same transaction,
// We need to check against the transaction batch first. e.g. A cast add and a cast remove for the same cast_id in the same transaction should not both be merged
pub fn get_from_db_or_txn(
    db: &RocksDB,
    txn: &RocksDbTransactionBatch,
    key: &[u8],
) -> Result<Option<Vec<u8>>, HubError> {
    if let Some(value) = txn.batch.get(key) {
        Ok(value.clone())
    } else {
        Ok(db.get(key)?)
    }
}

/// Rejects a `page_token` that does not sit inside `prefix`.
///
/// `RocksDB::get_iterator_options` uses the token as the scan's lower bound
/// (or, when reversed, its upper bound) *instead of* the prefix, so a token from
/// outside the prefix widens the range rather than narrowing it. The channel and
/// channel-follow enumerators identify a row by key length alone, and every
/// channel's keys share a length — so without this check a token minted for one
/// channel would return a different channel's rows under the requested channel
/// id — or, on a by-fid index, another fid's rows. An empty token is
/// out-of-prefix by the same test: it makes the scan start
/// at the front of the column family. Callers that mean "first page" must pass
/// `None`; the RPC layer normalizes an empty token to `None` before it gets here.
pub(crate) fn require_page_token_in_prefix(
    prefix: &[u8],
    page_options: &PageOptions,
) -> Result<(), HubError> {
    match &page_options.page_token {
        Some(token) if !token.starts_with(prefix) => Err(HubError::invalid_parameter(
            "page token does not belong to the requested index",
        )),
        _ => Ok(()),
    }
}

pub fn get_message_by_key(
    db: &RocksDB,
    txn: &RocksDbTransactionBatch,
    key: &[u8],
) -> Result<Option<MessageProto>, HubError> {
    match get_from_db_or_txn(db, txn, &key)? {
        Some(bytes) => match message_decode(&bytes) {
            Ok(message) => Ok(Some(message)),
            Err(e) => Err(e.into()),
        },
        None => Ok(None),
    }
}

/** Read many messages.
 * Note that if a message is not found, that corresponding entry in the result will be None.
 * This is different from the behaviour of get_message, which returns an error.
 */
pub fn get_many_messages(
    db: &RocksDB,
    primary_keys: Vec<Vec<u8>>,
) -> Result<Vec<MessageProto>, HubError> {
    let mut messages = Vec::new();

    for key in primary_keys {
        if let Ok(Some(value)) = db.get(&key) {
            match message_decode(&value) {
                Ok(message) => messages.push(message),
                Err(e) => Err(HubError::from(e))?,
            }
        } else {
            return Err(HubError::not_found(
                format!("could not get message with key: {:?}", key).as_str(),
            ));
        }
    }

    Ok(messages)
}

pub fn get_messages_page_by_prefix<F>(
    db: &RocksDB,
    prefix: &[u8],
    page_options: &PageOptions,
    filter: F,
) -> Result<MessagesPage, HubError>
where
    F: Fn(&MessageProto) -> bool,
{
    let mut messages = Vec::new();
    let mut last_key = vec![];

    db.for_each_iterator_by_prefix(
        Some(prefix.to_vec()),
        Some(increment_vec_u8(&prefix.to_vec())),
        page_options,
        |key, value| {
            match message_decode(value) {
                Ok(message) => {
                    if filter(&message) {
                        messages.push(message);

                        if messages.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                            last_key = key.to_vec();
                            return Ok(true); // Stop iterating
                        }
                    }

                    Ok(false) // Continue iterating
                }
                Err(e) => Err(HubError::from(e)),
            }
        },
    )?;

    let next_page_token = if !last_key.is_empty() {
        Some(last_key.to_vec())
    } else {
        None
    };

    Ok(MessagesPage {
        messages,
        next_page_token,
    })
}

#[inline]
pub fn message_encode(message: &MessageProto) -> Vec<u8> {
    if message.data_bytes.as_ref().is_some_and(|b| !b.is_empty()) {
        // Clone the message
        let mut cloned = message.clone();
        cloned.data = None;

        cloned.encode_to_vec()
    } else {
        message.encode_to_vec()
    }
}

#[inline]
pub fn message_bytes_decode(msg: &mut MessageProto) {
    if msg.data_bytes.as_ref().is_some_and(|b| !b.is_empty()) {
        if let Ok(msg_data) = decode_message_data(msg.data_bytes.as_ref().unwrap(), None) {
            msg.data = Some(msg_data);
        } else {
            msg.data = None;
        }
    }
}

/// The feature that defines `field` inside the `MessageData` body in field `body`, for fields added
/// after v0.14.3. Older nodes skip such a field as unknown, so they accept and store any bytes in
/// it. Add an arm for each field added to a message body, and clear the field in
/// `drop_unknown_data_fields`.
fn added_field_feature(body: u32, field: u32) -> Option<ProtocolFeature> {
    match (body, field) {
        // `cast_add_body.format_version` and `cast_add_body.chain`. TODO: store reads can stop
        // dropping them once a scan after ChainedCasts activation finds no stored cast that needs
        // it, and no node holds state from before that scan. Validation keeps dropping them, so
        // that pre-activation blocks replay the same.
        (5, 9) | (5, 10) => Some(ProtocolFeature::ChainedCasts),
        _ => None,
    }
}

/// An added field that `version` does not know; with `None` (store reads), every added field.
fn is_unknown_field(body: u32, field: u32, version: Option<EngineVersion>) -> bool {
    added_field_feature(body, field)
        .is_some_and(|feature| version.is_none_or(|version| !version.is_enabled(feature)))
}

/// An unknown added field whose bytes do not decode. Decoded alone, at its depth in its body, since
/// a field decodes in a message exactly when it decodes alone.
fn is_bad_unknown_field(
    body: u32,
    field: u32,
    bytes: &[u8],
    version: Option<EngineVersion>,
) -> bool {
    is_unknown_field(body, field, version) && {
        let mut data = Vec::new();
        encode_key(body, WireType::LengthDelimited, &mut data);
        encode_varint(bytes.len() as u64, &mut data);
        data.extend_from_slice(bytes);
        MessageData::decode(data.as_slice()).is_err()
    }
}

/// Decodes `MessageData`. If that fails, it drops the added fields that `version` does not know and
/// that do not decode, as an older node skips them, and decodes again. Validation passes the
/// block's version; store reads pass `None`, as stored messages include ones accepted before the
/// field's feature was active.
pub fn decode_message_data(
    bytes: &[u8],
    version: Option<EngineVersion>,
) -> Result<MessageData, DecodeError> {
    MessageData::decode(bytes).or_else(|err| match drop_unknown_fields(bytes, version) {
        Some(stripped) => MessageData::decode(stripped.as_slice()),
        None => Err(err),
    })
}

/// `MessageData` bytes without the added fields that `version` does not know and that do not
/// decode, or `None` if there are none to drop.
fn drop_unknown_fields(bytes: &[u8], version: Option<EngineVersion>) -> Option<Vec<u8>> {
    // prost's recursion budget: 100 for top-level fields, 99 inside a nested message.
    let mut out = Vec::with_capacity(bytes.len());
    let mut dropped = false;
    for (tag, wire_type, field) in protobuf_fields(bytes, 100)? {
        let mut payload = field;
        let body = (wire_type == WireType::LengthDelimited)
            .then(|| {
                decode_key(&mut payload).ok()?;
                decode_varint(&mut payload).ok()?;
                protobuf_fields(payload, 99)
            })
            .flatten();
        match body {
            Some(body)
                if body
                    .iter()
                    .any(|(f, _, field)| is_bad_unknown_field(tag, *f, field, version)) =>
            {
                let kept: Vec<u8> = body
                    .into_iter()
                    .filter(|(f, _, field)| !is_bad_unknown_field(tag, *f, field, version))
                    .flat_map(|(_, _, field)| field.iter().copied())
                    .collect();
                encode_key(tag, WireType::LengthDelimited, &mut out);
                encode_varint(kept.len() as u64, &mut out);
                out.extend_from_slice(&kept);
                dropped = true;
            }
            // A body that doesn't split into fields stays as it is, and the decode decides.
            _ => out.extend_from_slice(field),
        }
    }
    dropped.then_some(out)
}

/// The same drop for a decoded `MessageData`, using the same arms: for `data` sent without
/// `data_bytes`, and for the `data` of a block's messages.
pub fn drop_unknown_data_fields(data: &mut MessageData, version: EngineVersion) {
    if let Some(Body::CastAddBody(body)) = &mut data.body {
        if is_unknown_field(5, 9, Some(version)) {
            body.format_version = 0;
        }
        if is_unknown_field(5, 10, Some(version)) {
            body.chain = None;
        }
    }
}

/// Splits protobuf bytes into (tag, wire type, field bytes including the key).
fn protobuf_fields(mut buf: &[u8], depth: u32) -> Option<Vec<(u32, WireType, &[u8])>> {
    let mut fields = Vec::new();
    while !buf.is_empty() {
        let start = buf;
        let (tag, wire_type) = decode_key(&mut buf).ok()?;
        skip_protobuf_field(tag, wire_type, &mut buf, depth)?;
        fields.push((tag, wire_type, &start[..start.len() - buf.len()]));
    }
    Some(fields)
}

/// prost's `skip_field`, with the recursion budget passed in so groups nest exactly as deep as
/// prost allows at that level (its `DecodeContext` cannot be lowered from outside the crate).
fn skip_protobuf_field(tag: u32, wire_type: WireType, buf: &mut &[u8], depth: u32) -> Option<()> {
    if depth == 0 {
        return None;
    }
    if wire_type != WireType::StartGroup {
        return skip_field(wire_type, tag, buf, DecodeContext::default()).ok();
    }
    loop {
        let (inner_tag, inner_wire_type) = decode_key(buf).ok()?;
        if inner_wire_type == WireType::EndGroup {
            return (inner_tag == tag).then_some(());
        }
        skip_protobuf_field(inner_tag, inner_wire_type, buf, depth - 1)?;
    }
}

#[inline]
pub fn message_decode(bytes: &[u8]) -> Result<MessageProto, RocksdbError> {
    if let Ok(mut msg) = MessageProto::decode(bytes) {
        message_bytes_decode(&mut msg);
        Ok(msg)
    } else {
        Err(RocksdbError::DecodeError)
    }
}

pub fn put_message_transaction(
    txn: &mut RocksDbTransactionBatch,
    message: &MessageProto,
) -> Result<(), HubError> {
    let data = message.data.as_ref().unwrap();
    let ts_hash = make_ts_hash(data.timestamp, &message.hash)?;

    let primary_key = make_message_primary_key(
        data.fid,
        type_to_set_postfix(MessageType::try_from(data.r#type).unwrap())? as u8,
        Some(&ts_hash),
    );
    txn.put(primary_key, message_encode(&message));

    Ok(())
}

pub fn delete_message_transaction(
    txn: &mut RocksDbTransactionBatch,
    message: &MessageProto,
) -> Result<(), HubError> {
    let data = message.data.as_ref().unwrap();
    let ts_hash = make_ts_hash(data.timestamp, &message.hash)?;

    let primary_key = make_message_primary_key(
        data.fid,
        type_to_set_postfix(MessageType::try_from(data.r#type).unwrap())? as u8,
        Some(&ts_hash),
    );
    txn.delete(primary_key);

    Ok(())
}

pub fn is_message_in_time_range(
    start_time: Option<u32>,
    stop_time: Option<u32>,
    message: &MessageProto,
) -> bool {
    let start_time = start_time.unwrap_or(std::u32::MIN);
    let stop_time = stop_time.unwrap_or(std::u32::MAX);
    match &message.data {
        None => {
            // We expect all valid messages to have data
            return false;
        }
        Some(data) => return data.timestamp >= start_time && data.timestamp <= stop_time,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{message_data::Body, CastAddBody, CastType};

    const BAD_FORMAT_VERSION: [u8; 4] = [0x4a, 0x02, 0xff, 0xff]; // field 9 as bytes
    const BAD_CHAIN: [u8; 2] = [0x50, 0x01]; // field 10 as a varint
    const UNKNOWN_FIELD: [u8; 2] = [0x78, 0x01]; // field 15, unknown to every schema

    /// `MessageData` bytes with `field` between the cast's text and type, and the timestamp after
    /// the cast body, so a fallback that loses fields on either side changes the result.
    fn cast_data_bytes(field: &[u8]) -> Vec<u8> {
        let mut body = CastAddBody {
            text: "part".to_string(),
            ..Default::default()
        }
        .encode_to_vec();
        body.extend_from_slice(field);
        body.extend_from_slice(
            &CastAddBody {
                r#type: CastType::LongCast as i32,
                ..Default::default()
            }
            .encode_to_vec(),
        );
        let mut bytes = MessageData {
            fid: 1234,
            ..Default::default()
        }
        .encode_to_vec();
        encode_key(5, WireType::LengthDelimited, &mut bytes);
        encode_varint(body.len() as u64, &mut bytes);
        bytes.extend_from_slice(&body);
        bytes.extend_from_slice(
            &MessageData {
                timestamp: 5678,
                ..Default::default()
            }
            .encode_to_vec(),
        );
        bytes
    }

    /// `depth` nested groups under `tag`, with a varint in the innermost one.
    fn nested_groups(tag: u32, depth: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        for _ in 0..depth {
            encode_key(tag, WireType::StartGroup, &mut bytes);
        }
        bytes.extend_from_slice(&UNKNOWN_FIELD);
        for _ in 0..depth {
            encode_key(tag, WireType::EndGroup, &mut bytes);
        }
        bytes
    }

    #[test]
    fn decode_drops_bad_added_fields_only_where_they_are_unknown() {
        for bad in [
            BAD_FORMAT_VERSION.to_vec(),
            BAD_CHAIN.to_vec(),
            [BAD_FORMAT_VERSION.as_slice(), &BAD_CHAIN].concat(),
        ] {
            let bytes = cast_data_bytes(&bad);
            // From ChainedCasts on, fields 9 and 10 are known, so a bad one fails the decode.
            assert!(decode_message_data(&bytes, Some(EngineVersion::V22)).is_err());
            assert!(decode_message_data(&bytes, Some(EngineVersion::V21)).is_ok());

            // Same result as v0.14.3, which skipped them as it skips any unknown field.
            let data = decode_message_data(&bytes, None).unwrap();
            assert_eq!(
                data,
                MessageData::decode(cast_data_bytes(&UNKNOWN_FIELD).as_slice()).unwrap()
            );
            let Some(Body::CastAddBody(body)) = &data.body else {
                panic!("cast body lost");
            };
            assert_eq!(
                (data.fid, data.timestamp, body.text.as_str(), body.r#type),
                (1234, 5678, "part", CastType::LongCast as i32)
            );

            // Store reads always drop them, so a stored cast keeps its `data` and can be pruned.
            let mut message = MessageProto {
                data_bytes: Some(bytes),
                ..Default::default()
            };
            message_bytes_decode(&mut message);
            assert_eq!(message.data, Some(data));
        }

        // Only a field that does not decode is dropped: a good field 9 beside a bad field 10 stays.
        let bytes = cast_data_bytes(&[[0x48, 0x01].as_slice(), &BAD_CHAIN].concat());
        assert!(decode_message_data(&bytes, Some(EngineVersion::V22)).is_err());
        for version in [Some(EngineVersion::V21), None] {
            let data = decode_message_data(&bytes, version).unwrap();
            let Some(Body::CastAddBody(body)) = data.body else {
                panic!("cast body lost");
            };
            assert_eq!((body.format_version, body.chain), (1, None));
        }
    }

    #[test]
    fn decode_fallback_skips_groups_exactly_as_deep_as_prost() {
        // prost's own skip of an unknown field is the oracle for what v0.14.3 accepted, for groups
        // in a bad field 9 or 10 (cast body) and in an unknown field beside it (top level).
        for depth in [1, 97, 98, 99, 100, 101] {
            for field in [9, 10] {
                assert_eq!(
                    decode_message_data(&cast_data_bytes(&nested_groups(field, depth)), None)
                        .is_ok(),
                    MessageData::decode(cast_data_bytes(&nested_groups(15, depth)).as_slice())
                        .is_ok(),
                    "cast body, field {field}, depth {depth}"
                );
            }
            let top_level = |chain_field: &[u8]| {
                let mut bytes = cast_data_bytes(chain_field);
                bytes.extend_from_slice(&nested_groups(999, depth));
                bytes
            };
            assert_eq!(
                decode_message_data(&top_level(&BAD_CHAIN), None).is_ok(),
                MessageData::decode(top_level(&UNKNOWN_FIELD).as_slice()).is_ok(),
                "top level, depth {depth}"
            );
        }
        // The series straddles prost's limit, so the comparisons above are not vacuous.
        assert!(MessageData::decode(cast_data_bytes(&nested_groups(15, 98)).as_slice()).is_ok());
        assert!(MessageData::decode(cast_data_bytes(&nested_groups(15, 99)).as_slice()).is_err());
    }
}
