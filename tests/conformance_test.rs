//! L0 golden determinism / conformance vectors (NEYN-12044, snapchain#917).
//!
//! This test owns a versioned conformance corpus that pins snapchain's byte-exact output for a
//! fixed set of inputs. There are two layers:
//!
//!   * **Crypto layer (cross-client, protocol-canonical):** the encoded `MessageData` bytes, the
//!     BLAKE3-160 `hash`, the Ed25519 `signature`, the `signer`, and the full encoded `Message`.
//!     These rest only on protobuf + BLAKE3 + Ed25519 (RFC 8032, deterministic), so any client in
//!     any language can reproduce them. The TS verifier in `client_parity_tests/conformance.test.ts`
//!     re-checks this layer against the same corpus.
//!   * **State-root layer (snapchain-internal for now):** the post-state merkle root after applying a vector
//!     to a fresh `ShardEngine`. snapchain's sharded trie diverges from the (stale) trie spec in
//!     `farcasterxyz/protocol` §4.2, so these roots are reference-impl-authoritative until that spec
//!     is reconciled (tracked as a follow-up on epic NEYN-12026). Not every vector carries a root:
//!     types needing extra on-chain preconditions to merge (key_add/key_remove, verification with a
//!     real claim signature) ship crypto-layer vectors only, with `expected_state` omitted.
//!
//! The `vectors()` table is the single source of truth: both the generator (`generate_corpus`,
//! `#[ignore]`) and the verifier (`conformance_golden`) build messages from it, so they cannot drift.
//!
//! Regenerate after an intentional protocol change:
//!     cargo test --test conformance_test generate_corpus -- --ignored --nocapture

use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use serde_json::json;
use snapchain::core::util::{calculate_message_hash, FarcasterTime};
use snapchain::proto::{self, MessageType, ReactionType};
use snapchain::storage::store::engine::ShardEngine;
use snapchain::storage::store::test_helper;
use snapchain::utils::factory::messages_factory;
use std::fs;
use std::path::{Path, PathBuf};

/// Bump when the corpus format or its pinned protocol semantics change.
const CORPUS_VERSION: u32 = 1;
/// Fixed Farcaster-epoch timestamp (2024-01-01T00:00:00Z) so output is reproducible.
const FIXED_TIMESTAMP: u32 = 94_608_000;
/// Network the engine runs under for state-root vectors. Devnet enables all protocol features.
const CORPUS_NETWORK: proto::FarcasterNetwork = proto::FarcasterNetwork::Devnet;

fn corpus_dir() -> PathBuf {
    // Integration tests run with CWD = crate root.
    Path::new("tests/conformance/corpus").join(format!("v{}", CORPUS_VERSION))
}

// ---------------------------------------------------------------------------
// Vector table — the single source of truth.
// ---------------------------------------------------------------------------

/// A deterministic conformance vector. `build` returns the exact `proto::Message` snapshotted into
/// the corpus; it is handed the registered Ed25519 signer and the fixed timestamp.
struct VectorSpec {
    id: &'static str,
    description: &'static str,
    message_type: MessageType,
    fid: u64,
    /// Human-readable semantic input, for non-Rust clients reconstructing the message.
    input: serde_json::Value,
    /// Whether a state-root vector is computed (the message merges under `register_user`).
    state_root: bool,
    build: Box<dyn Fn(&SigningKey, u32) -> proto::Message>,
}

/// FID used across all vectors. Re-registered on a fresh engine per vector for state-root vectors.
const VECTOR_FID: u64 = 1234;

fn vectors() -> Vec<VectorSpec> {
    vec![
        VectorSpec {
            id: "cast_add_basic",
            description: "CastAdd with simple text body.",
            message_type: MessageType::CastAdd,
            fid: VECTOR_FID,
            input: json!({ "text": "Hello, Farcaster!", "type": "CAST" }),
            state_root: true,
            build: Box::new(|signer, ts| {
                messages_factory::casts::create_cast_add(
                    VECTOR_FID,
                    "Hello, Farcaster!",
                    Some(ts),
                    Some(signer),
                )
            }),
        },
        VectorSpec {
            id: "cast_remove",
            description: "CastRemove targeting a fixed cast hash.",
            message_type: MessageType::CastRemove,
            fid: VECTOR_FID,
            input: json!({ "target_hash": "01".repeat(20) }),
            state_root: true,
            build: Box::new(|signer, ts| {
                messages_factory::casts::create_cast_remove(
                    VECTOR_FID,
                    &vec![0x01u8; 20],
                    Some(ts),
                    Some(signer),
                )
            }),
        },
        VectorSpec {
            id: "reaction_add_like",
            description: "ReactionAdd (LIKE) targeting a fixed cast id.",
            message_type: MessageType::ReactionAdd,
            fid: VECTOR_FID,
            input: json!({ "reaction_type": "LIKE", "target_cast": { "fid": 321, "hash": "02".repeat(20) } }),
            state_root: true,
            build: Box::new(|signer, ts| {
                messages_factory::reactions::create_reaction_add(
                    VECTOR_FID,
                    ReactionType::Like,
                    proto::reaction_body::Target::TargetCastId(proto::CastId {
                        fid: 321,
                        hash: vec![0x02u8; 20],
                    }),
                    Some(ts),
                    Some(signer),
                )
            }),
        },
        VectorSpec {
            id: "reaction_remove_like",
            description: "ReactionRemove (LIKE) targeting a fixed cast id.",
            message_type: MessageType::ReactionRemove,
            fid: VECTOR_FID,
            input: json!({ "reaction_type": "LIKE", "target_cast": { "fid": 321, "hash": "02".repeat(20) } }),
            state_root: true,
            build: Box::new(|signer, ts| {
                messages_factory::reactions::create_reaction_remove(
                    VECTOR_FID,
                    ReactionType::Like,
                    proto::reaction_body::Target::TargetCastId(proto::CastId {
                        fid: 321,
                        hash: vec![0x02u8; 20],
                    }),
                    Some(ts),
                    Some(signer),
                )
            }),
        },
        VectorSpec {
            id: "link_add_follow",
            description: "LinkAdd (follow) targeting a fixed fid.",
            message_type: MessageType::LinkAdd,
            fid: VECTOR_FID,
            input: json!({ "type": "follow", "target_fid": 456 }),
            state_root: true,
            build: Box::new(|signer, ts| {
                messages_factory::links::create_link_add(
                    VECTOR_FID,
                    "follow",
                    456,
                    Some(ts),
                    Some(signer),
                )
            }),
        },
        VectorSpec {
            id: "link_remove_follow",
            description: "LinkRemove (follow) targeting a fixed fid.",
            message_type: MessageType::LinkRemove,
            fid: VECTOR_FID,
            input: json!({ "type": "follow", "target_fid": 456 }),
            state_root: true,
            build: Box::new(|signer, ts| {
                messages_factory::links::create_link_remove(
                    VECTOR_FID,
                    "follow",
                    456,
                    Some(ts),
                    Some(signer),
                )
            }),
        },
        VectorSpec {
            id: "link_add_block",
            description:
                "LinkAdd (block) targeting a fixed fid. Per FIP-block-links, a block is a \
                          Link with type=\"block\"; it rides the existing Link CRDT with no new \
                          validation or merge logic. Same (fid, target) as link_add_follow with a \
                          different type, demonstrating link types are independent.",
            message_type: MessageType::LinkAdd,
            fid: VECTOR_FID,
            input: json!({ "type": "block", "target_fid": 456 }),
            state_root: true,
            build: Box::new(|signer, ts| {
                messages_factory::links::create_link_add(
                    VECTOR_FID,
                    "block",
                    456,
                    Some(ts),
                    Some(signer),
                )
            }),
        },
        VectorSpec {
            id: "link_remove_block",
            description: "LinkRemove (block) targeting a fixed fid. Removes (unblocks) the \
                          type=\"block\" link via the same Link remove semantics as follows.",
            message_type: MessageType::LinkRemove,
            fid: VECTOR_FID,
            input: json!({ "type": "block", "target_fid": 456 }),
            state_root: true,
            build: Box::new(|signer, ts| {
                messages_factory::links::create_link_remove(
                    VECTOR_FID,
                    "block",
                    456,
                    Some(ts),
                    Some(signer),
                )
            }),
        },
        VectorSpec {
            id: "link_compact_state_follow",
            description: "LinkCompactState (follow) listing a fixed target fid. Compaction is the \
                          mechanism that, under FIP-block-links, must stay type-scoped: a follow \
                          compact state only deletes follow links. This vector pins the message \
                          encoding cross-client; the type-scoping behavior is exercised by \
                          conformance_link_compaction_type_scoped.",
            message_type: MessageType::LinkCompactState,
            fid: VECTOR_FID,
            input: json!({ "type": "follow", "target_fids": [456] }),
            state_root: true,
            build: Box::new(|signer, ts| {
                messages_factory::links::create_link_compact_state(
                    VECTOR_FID,
                    "follow",
                    vec![456],
                    Some(ts),
                    Some(signer),
                )
            }),
        },
        VectorSpec {
            id: "user_data_add_display",
            description: "UserDataAdd setting the DISPLAY name.",
            message_type: MessageType::UserDataAdd,
            fid: VECTOR_FID,
            input: json!({ "type": "DISPLAY", "value": "Test User" }),
            state_root: true,
            build: Box::new(|signer, ts| {
                messages_factory::user_data::create_user_data_add(
                    VECTOR_FID,
                    proto::UserDataType::Display,
                    &"Test User".to_string(),
                    Some(ts),
                    Some(signer),
                )
            }),
        },
        VectorSpec {
            id: "verification_add_eth",
            description: "VerificationAddEthAddress with fixed (non-validating) claim fields. \
                          Crypto-layer only: a real claim signature is required to merge.",
            message_type: MessageType::VerificationAddEthAddress,
            fid: VECTOR_FID,
            input: json!({ "verification_type": 0, "address": "03".repeat(20),
                           "block_hash": "04".repeat(32), "chain_id": 0, "protocol": 0 }),
            state_root: false,
            build: Box::new(|signer, ts| {
                messages_factory::verifications::create_verification_add(
                    VECTOR_FID,
                    0,
                    vec![0x03u8; 20],
                    vec![0x05u8; 65],
                    vec![0x04u8; 32],
                    Some(ts),
                    Some(signer),
                )
            }),
        },
        VectorSpec {
            id: "key_add_ed25519",
            description:
                "KeyAdd (off-chain signer) with deterministic EIP-712 custody + metadata \
                          signatures. Crypto-layer only: custody registration is required to merge.",
            message_type: MessageType::KeyAdd,
            fid: VECTOR_FID,
            input: json!({
                "key_type": 1, "scopes": ["CAST_ADD"], "ttl": 0, "nonce": 1, "deadline": 99_999_999,
                "fid_custody_private_key": "11".repeat(32), "app_custody_private_key": "22".repeat(32),
                "request_fid": VECTOR_FID
            }),
            state_root: false,
            build: Box::new(|signer, ts| {
                use alloy_signer_local::PrivateKeySigner;
                let fid_custody = PrivateKeySigner::from_slice(&[0x11u8; 32]).unwrap();
                let app_custody = PrivateKeySigner::from_slice(&[0x22u8; 32]).unwrap();
                messages_factory::keys::create_key_add(
                    VECTOR_FID,
                    &fid_custody,
                    VECTOR_FID,
                    &app_custody,
                    signer,
                    vec![MessageType::CastAdd],
                    0,
                    1,
                    99_999_999,
                    Some(ts),
                )
            }),
        },
        VectorSpec {
            id: "key_remove_custody",
            description: "KeyRemove (custody revocation) with a deterministic EIP-712 signature. \
                          Crypto-layer only: custody registration is required to merge.",
            message_type: MessageType::KeyRemove,
            fid: VECTOR_FID,
            input: json!({
                "signature_type": 1, "target_key": "06".repeat(32), "nonce": 2, "deadline": 99_999_999,
                "fid_custody_private_key": "11".repeat(32)
            }),
            state_root: false,
            build: Box::new(|signer, ts| {
                use alloy_signer_local::PrivateKeySigner;
                let fid_custody = PrivateKeySigner::from_slice(&[0x11u8; 32]).unwrap();
                messages_factory::keys::create_key_remove_custody(
                    VECTOR_FID,
                    &fid_custody,
                    signer,
                    &[0x06u8; 32],
                    2,
                    99_999_999,
                    Some(ts),
                )
            }),
        },
    ]
}

// ---------------------------------------------------------------------------
// Manifest schema (serialized to manifest.json).
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct Corpus {
    corpus_version: u32,
    protocol: ProtocolPin,
    vectors: Vec<VectorEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProtocolPin {
    network: String,
    engine_version: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct VectorEntry {
    id: String,
    description: String,
    message_type: String,
    fid: u64,
    timestamp: u32,
    signer_private_key: String,
    input: serde_json::Value,
    message_file: String,
    expected: Expected,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_state: Option<ExpectedState>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Expected {
    /// Encoded `MessageData` (the signing pre-image source).
    data_bytes: String,
    /// BLAKE3-160 of `data_bytes`.
    hash: String,
    /// Ed25519 signature over `hash`.
    signature: String,
    /// Ed25519 public key of the signer.
    signer: String,
    /// Full encoded `Message` (identical to the bytes in `message_file`).
    message_bytes: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ExpectedState {
    shard_id: u32,
    /// snapchain shard trie root after registering the fid and merging this message.
    post_state_root: String,
}

// ---------------------------------------------------------------------------
// Shared helpers.
// ---------------------------------------------------------------------------

/// The fixed Ed25519 key every vector is signed by, also the registered signer for state-root vectors.
fn signer() -> SigningKey {
    test_helper::default_signer()
}

/// Attach the canonical signing pre-image as `data_bytes` (proto field 7), per the Farcaster spec
/// (SPECIFICATION.md §2 Hashing): "MessageData serialized to bytes if using protobuf serialization
/// other than ts-proto". snapchain hashes prost's encoding of `MessageData`; carrying those exact
/// bytes makes every vector self-verifying by any client — a ts-proto client cannot reproduce
/// prost's encoding from the semantic fields (e.g. empty `repeated` fields differ), so it must
/// verify the hash against the carried pre-image. This does not change `hash` or `signature`.
fn with_canonical_data_bytes(mut msg: proto::Message) -> proto::Message {
    let data_bytes = msg.data.as_ref().expect("message has data").encode_to_vec();
    debug_assert_eq!(
        calculate_message_hash(&data_bytes),
        msg.hash,
        "factory hash must be over prost(data)"
    );
    msg.data_bytes = Some(data_bytes);
    msg
}

/// Build a vector's message from the table and finalize it into its canonical, portable form.
fn build_message(spec: &VectorSpec, key: &SigningKey) -> proto::Message {
    with_canonical_data_bytes((spec.build)(key, FIXED_TIMESTAMP))
}

async fn new_engine() -> (ShardEngine, tempfile::TempDir) {
    let options = test_helper::EngineOptions {
        network: Some(CORPUS_NETWORK),
        shard_id: 1,
        ..Default::default()
    };
    test_helper::new_engine_with_options(options).await
}

/// Register a fid (storage + id-register + signer events) **deterministically**.
///
/// The default `test_helper::register_user` builds on-chain events with `rand::random()`
/// transaction hashes. Because the merkle trie keys on-chain events by `transaction_hash` and a
/// leaf's hash is `blake3_20(key)` (value-independent), random tx hashes would make the shard root
/// non-reproducible. We pin the tx hashes to fixed values; the random block number/timestamp/payer
/// fields live only in the event *value* and do not affect the root.
async fn register_user_deterministic(engine: &mut ShardEngine, fid: u64) {
    use snapchain::utils::factory::events_factory;

    let mut storage = test_helper::default_storage_event(fid);
    storage.transaction_hash = vec![0xa1; 32];
    storage.log_index = 0;
    test_helper::commit_event(engine, &storage).await;

    let mut id_register = events_factory::create_id_register_event(
        fid,
        proto::IdRegisterEventType::Register,
        test_helper::default_custody_address(),
        None,
    );
    id_register.transaction_hash = vec![0xa2; 32];
    id_register.log_index = 0;
    test_helper::commit_event(engine, &id_register).await;

    let mut signer_event =
        events_factory::create_signer_event(fid, signer(), proto::SignerEventType::Add, None, None);
    signer_event.transaction_hash = vec![0xa3; 32];
    signer_event.log_index = 0;
    test_helper::commit_event(engine, &signer_event).await;
}

/// Register the vector's fid on a fresh engine, merge the message, and return the resulting shard
/// trie root.
async fn compute_post_state_root(msg: &proto::Message) -> Vec<u8> {
    let (mut engine, _tmp) = new_engine().await;
    register_user_deterministic(&mut engine, msg.fid()).await;
    let chunk = test_helper::commit_message_at(
        &mut engine,
        msg,
        &FarcasterTime::new(FIXED_TIMESTAMP as u64),
    )
    .await;
    chunk.header.as_ref().unwrap().shard_root.clone()
}

// ---------------------------------------------------------------------------
// Golden text rendering — a zero-dependency protobuf wire disassembler.
//
// Produces a deterministic, human-reviewable view of a message's wire bytes so corpus changes are
// auditable in a PR diff (a raw `.pb` blob is invisible in review). The `.pb` bytes + the manifest
// hex remain the authoritative artifacts; this text is a review aid, regenerated by the generator.
// It is schema-light: the stable `Message` and `MessageData` levels are annotated with field names;
// deeper fields are rendered structurally (field number + wire type + value).
// ---------------------------------------------------------------------------
mod golden {
    /// Field-naming context for one level of the message tree.
    #[derive(Clone, Copy)]
    enum Level {
        Message,
        MessageData,
        Generic,
    }

    fn field_name(level: Level, n: u64) -> &'static str {
        match level {
            Level::Message => match n {
                1 => "data",
                2 => "hash",
                3 => "hash_scheme",
                4 => "signature",
                5 => "signature_scheme",
                6 => "signer",
                7 => "data_bytes",
                _ => "",
            },
            Level::MessageData => match n {
                1 => "type",
                2 => "fid",
                3 => "timestamp",
                4 => "network",
                5 => "cast_add_body",
                6 => "cast_remove_body",
                7 => "reaction_body",
                9 => "verification_add_address_body",
                10 => "verification_remove_body",
                12 => "user_data_body",
                14 => "link_body",
                15 => "username_proof_body",
                16 => "frame_action_body",
                17 => "link_compact_state_body",
                18 => "lend_storage_body",
                19 => "key_add_body",
                20 => "key_remove_body",
                _ => "",
            },
            Level::Generic => "",
        }
    }

    /// If field `n` at `level` is a length-delimited submessage we should recurse into, the child
    /// level to use. At the named levels we know the schema, which prevents misrendering byte
    /// fields (hash/signature/signer) that might coincidentally parse as a message. Deeper, we fall
    /// back to a parse heuristic (`is_message`).
    fn child_level(level: Level, n: u64) -> Option<Level> {
        match level {
            Level::Message if n == 1 => Some(Level::MessageData), // data
            Level::Message => None, // hash/sig/signer/data_bytes: bytes
            Level::MessageData if (5..=20).contains(&n) => Some(Level::Generic), // body oneof
            Level::MessageData => None,
            Level::Generic => Some(Level::Generic),
        }
    }

    fn read_varint(buf: &[u8], i: &mut usize) -> Option<u64> {
        let mut v = 0u64;
        let mut shift = 0u32;
        loop {
            let b = *buf.get(*i)?;
            *i += 1;
            v |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Some(v);
            }
            shift += 7;
            if shift >= 64 {
                return None;
            }
        }
    }

    /// Whether `buf` parses cleanly and fully as a protobuf message.
    fn is_message(buf: &[u8]) -> bool {
        if buf.is_empty() {
            return false;
        }
        let mut i = 0;
        while i < buf.len() {
            let Some(tag) = read_varint(buf, &mut i) else {
                return false;
            };
            if tag >> 3 == 0 {
                return false;
            }
            match tag & 7 {
                0 => {
                    if read_varint(buf, &mut i).is_none() {
                        return false;
                    }
                }
                1 => i += 8,
                5 => i += 4,
                2 => {
                    let Some(len) = read_varint(buf, &mut i) else {
                        return false;
                    };
                    i += len as usize;
                }
                _ => return false,
            }
            if i > buf.len() {
                return false;
            }
        }
        true
    }

    fn is_printable(buf: &[u8]) -> bool {
        !buf.is_empty() && buf.iter().all(|&b| (0x20..=0x7e).contains(&b))
    }

    fn render_level(buf: &[u8], level: Level, depth: usize, out: &mut String) {
        use std::fmt::Write as _;
        let indent = "  ".repeat(depth);
        let mut i = 0;
        while i < buf.len() {
            let tag = read_varint(buf, &mut i).expect("valid tag in committed message");
            let field = tag >> 3;
            let name = field_name(level, field);
            let label = if name.is_empty() {
                field.to_string()
            } else {
                format!("{field} {name}")
            };
            match tag & 7 {
                0 => {
                    let v = read_varint(buf, &mut i).expect("varint value");
                    let _ = writeln!(out, "{indent}{label}: {v}");
                }
                1 => {
                    let bytes = &buf[i..i + 8];
                    i += 8;
                    let _ = writeln!(out, "{indent}{label}: 0x{} (fixed64)", hex::encode(bytes));
                }
                5 => {
                    let bytes = &buf[i..i + 4];
                    i += 4;
                    let _ = writeln!(out, "{indent}{label}: 0x{} (fixed32)", hex::encode(bytes));
                }
                2 => {
                    let len = read_varint(buf, &mut i).expect("length") as usize;
                    let sub = &buf[i..i + len];
                    i += len;
                    match child_level(level, field).filter(|_| is_message(sub)) {
                        Some(child) => {
                            let _ = writeln!(out, "{indent}{label} {{  # {len} bytes");
                            render_level(sub, child, depth + 1, out);
                            let _ = writeln!(out, "{indent}}}");
                        }
                        None if is_printable(sub) => {
                            let _ = writeln!(
                                out,
                                "{indent}{label}: {:?}  # string, {len} bytes",
                                String::from_utf8_lossy(sub)
                            );
                        }
                        None => {
                            let _ = writeln!(
                                out,
                                "{indent}{label}: 0x{}  # {len} bytes",
                                hex::encode(sub)
                            );
                        }
                    }
                }
                wire => {
                    let _ = writeln!(out, "{indent}{label}: <unsupported wire type {wire}>");
                }
            }
        }
    }

    /// Render the wire bytes of a `Message` to deterministic, reviewable text.
    pub fn render(message_bytes: &[u8]) -> String {
        let mut out =
            String::from("# Wire-format disassembly of the Message (review aid; see README).\n");
        out.push_str("Message {\n");
        render_level(message_bytes, Level::Message, 1, &mut out);
        out.push_str("}\n");
        out
    }
}

// ---------------------------------------------------------------------------
// Generator.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "regenerates the checked-in corpus; run explicitly with --ignored"]
async fn generate_corpus() {
    let dir = corpus_dir();
    let messages_dir = dir.join("messages");
    fs::create_dir_all(&messages_dir).expect("create corpus dir");

    let key = signer();
    let mut entries = Vec::new();

    for spec in vectors() {
        let msg = build_message(&spec, &key);
        let data = msg.data.as_ref().expect("message has data");
        let data_bytes = data.encode_to_vec();
        let message_bytes = msg.encode_to_vec();

        let message_file = format!("messages/{}.pb", spec.id);
        fs::write(dir.join(&message_file), &message_bytes).expect("write .pb");
        // Human-reviewable wire disassembly committed alongside the binary.
        fs::write(
            dir.join(format!("messages/{}.pb.txt", spec.id)),
            golden::render(&message_bytes),
        )
        .expect("write .pb.txt");

        let expected_state = if spec.state_root {
            let root = compute_post_state_root(&msg).await;
            Some(ExpectedState {
                shard_id: 1,
                post_state_root: hex::encode(&root),
            })
        } else {
            None
        };

        entries.push(VectorEntry {
            id: spec.id.to_string(),
            description: spec.description.to_string(),
            message_type: format!("{:?}", spec.message_type),
            fid: spec.fid,
            timestamp: FIXED_TIMESTAMP,
            signer_private_key: hex::encode(key.to_bytes()),
            input: spec.input,
            message_file,
            expected: Expected {
                data_bytes: hex::encode(&data_bytes),
                hash: hex::encode(&msg.hash),
                signature: hex::encode(&msg.signature),
                signer: hex::encode(&msg.signer),
                message_bytes: hex::encode(&message_bytes),
            },
            expected_state,
        });
    }

    let corpus = Corpus {
        corpus_version: CORPUS_VERSION,
        protocol: ProtocolPin {
            network: format!("{:?}", CORPUS_NETWORK),
            engine_version: format!("{:?}", snapchain::version::version::EngineVersion::latest()),
        },
        vectors: entries,
    };

    let mut json = serde_json::to_string_pretty(&corpus).expect("serialize manifest");
    json.push('\n');
    fs::write(dir.join("manifest.json"), json).expect("write manifest");
    println!(
        "Wrote {} vectors to {}",
        corpus.vectors.len(),
        dir.display()
    );
}

// ---------------------------------------------------------------------------
// Verifiers.
// ---------------------------------------------------------------------------

fn load_corpus() -> Corpus {
    let path = corpus_dir().join("manifest.json");
    let data = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {} (run generate_corpus first)", path.display(), e));
    serde_json::from_str(&data).expect("parse manifest.json")
}

/// Golden test: rebuild every vector from the table and assert it is byte-identical to the
/// checked-in corpus across encoding, hash, signature, signer, and (where present) state root.
#[tokio::test]
async fn conformance_golden() {
    let corpus = load_corpus();
    let key = signer();
    let specs = vectors();

    assert_eq!(
        corpus.vectors.len(),
        specs.len(),
        "manifest vector count drifted from the table; regenerate the corpus"
    );

    for (entry, spec) in corpus.vectors.iter().zip(specs.iter()) {
        assert_eq!(entry.id, spec.id, "vector order drifted from the table");

        let msg = build_message(spec, &key);
        let data_bytes = msg.data.as_ref().unwrap().encode_to_vec();
        let message_bytes = msg.encode_to_vec();

        // Crypto layer: byte-exact encoding / hash / signature / signer.
        assert_eq!(
            hex::encode(&data_bytes),
            entry.expected.data_bytes,
            "[{}] data_bytes",
            spec.id
        );
        assert_eq!(
            hex::encode(&message_bytes),
            entry.expected.message_bytes,
            "[{}] message_bytes",
            spec.id
        );
        assert_eq!(
            hex::encode(&msg.hash),
            entry.expected.hash,
            "[{}] hash",
            spec.id
        );
        assert_eq!(
            hex::encode(&msg.signature),
            entry.expected.signature,
            "[{}] signature",
            spec.id
        );
        assert_eq!(
            hex::encode(&msg.signer),
            entry.expected.signer,
            "[{}] signer",
            spec.id
        );

        // The .pb on disk must match message_bytes exactly.
        let pb = fs::read(corpus_dir().join(&entry.message_file)).expect("read .pb");
        assert_eq!(pb, message_bytes, "[{}] .pb != message_bytes", spec.id);

        // The committed wire disassembly must match what the renderer produces (golden-file flow:
        // regenerate with `generate_corpus -- --ignored` to update).
        let golden_path = corpus_dir().join(format!("messages/{}.pb.txt", spec.id));
        let committed_golden = fs::read_to_string(&golden_path)
            .unwrap_or_else(|e| panic!("read {}: {}", golden_path.display(), e));
        assert_eq!(
            golden::render(&message_bytes),
            committed_golden,
            "[{}] wire disassembly drifted; regenerate the corpus",
            spec.id
        );

        // Independently recompute the hash and verify the signature, not just diff the hex.
        assert_eq!(
            calculate_message_hash(&data_bytes),
            msg.hash,
            "[{}] recomputed BLAKE3-160 hash mismatch",
            spec.id
        );
        let signer_bytes: [u8; 32] = msg.signer.as_slice().try_into().expect("32-byte signer");
        let vk = VerifyingKey::from_bytes(&signer_bytes).expect("valid signer key");
        let sig = Signature::from_slice(&msg.signature).expect("valid signature bytes");
        vk.verify_strict(&msg.hash, &sig)
            .unwrap_or_else(|e| panic!("[{}] Ed25519 verify failed: {}", spec.id, e));

        // State-root layer: post-state root, where the corpus pins one.
        match (&entry.expected_state, spec.state_root) {
            (Some(state), true) => {
                let root = compute_post_state_root(&msg).await;
                assert_eq!(
                    hex::encode(&root),
                    state.post_state_root,
                    "[{}] post_state_root mismatch",
                    spec.id
                );
            }
            (None, false) => {}
            (s, flag) => panic!(
                "[{}] expected_state present={} but table state_root={}; regenerate",
                spec.id,
                s.is_some(),
                flag
            ),
        }
    }
}

/// Two independent engines fed identical input must compute identical state roots after every
/// commit. Exercises the cross-node determinism requirement of L0.
#[tokio::test]
async fn conformance_cross_node() {
    let key = signer();
    let mergeable: Vec<proto::Message> = vectors()
        .iter()
        .filter(|s| s.state_root)
        .map(|s| build_message(s, &key))
        .collect();
    assert!(!mergeable.is_empty(), "no mergeable vectors to cross-check");

    let (mut node_a, _a) = new_engine().await;
    let (mut node_b, _b) = new_engine().await;
    register_user_deterministic(&mut node_a, VECTOR_FID).await;
    register_user_deterministic(&mut node_b, VECTOR_FID).await;

    let ts = FarcasterTime::new(FIXED_TIMESTAMP as u64);
    for msg in &mergeable {
        let root_a = test_helper::commit_message_at(&mut node_a, msg, &ts).await;
        let root_b = test_helper::commit_message_at(&mut node_b, msg, &ts).await;
        assert_eq!(
            root_a.header.as_ref().unwrap().shard_root,
            root_b.header.as_ref().unwrap().shard_root,
            "cross-node state root diverged after merging a {:?} message",
            msg.msg_type()
        );
    }
}

/// Type-scoped link compaction (FIP-block-links, V19) must be cross-node deterministic. On devnet
/// (V19, the corpus network) a "follow" LinkCompactState sweeps the fid's non-listed follow links
/// but leaves "block" links intact. Two independent engines fed the identical block-add /
/// follow-add / follow-compact sequence must agree on the shard root after every commit — a
/// divergence here would fork the network, since the root is in each shard header. This complements
/// the single-engine behavioral coverage in engine_tests by pinning the determinism property.
#[tokio::test]
async fn conformance_link_compaction_type_scoped() {
    const TARGET_FID: u64 = 456;

    let key = signer();
    // A follow + a block to the same target; the later compact state lists neither target, so a
    // type-blind compaction would delete both but a type-scoped one deletes only the follow.
    let follow_add = with_canonical_data_bytes(messages_factory::links::create_link_add(
        VECTOR_FID,
        "follow",
        TARGET_FID,
        Some(FIXED_TIMESTAMP),
        Some(&key),
    ));
    let block_add = with_canonical_data_bytes(messages_factory::links::create_link_add(
        VECTOR_FID,
        "block",
        TARGET_FID,
        Some(FIXED_TIMESTAMP),
        Some(&key),
    ));
    let follow_compact =
        with_canonical_data_bytes(messages_factory::links::create_link_compact_state(
            VECTOR_FID,
            "follow",
            vec![TARGET_FID + 100],
            Some(FIXED_TIMESTAMP + 1),
            Some(&key),
        ));

    let (mut node_a, _a) = new_engine().await;
    let (mut node_b, _b) = new_engine().await;
    register_user_deterministic(&mut node_a, VECTOR_FID).await;
    register_user_deterministic(&mut node_b, VECTOR_FID).await;

    for msg in [&follow_add, &block_add, &follow_compact] {
        let ts = FarcasterTime::new(msg.data.as_ref().unwrap().timestamp as u64);
        let root_a = test_helper::commit_message_at(&mut node_a, msg, &ts).await;
        let root_b = test_helper::commit_message_at(&mut node_b, msg, &ts).await;
        assert_eq!(
            root_a.header.as_ref().unwrap().shard_root,
            root_b.header.as_ref().unwrap().shard_root,
            "cross-node state root diverged after merging a {:?} message",
            msg.msg_type()
        );
    }

    // The compaction ran (the non-listed follow is gone) but was type-scoped (the block survives),
    // identically on both nodes.
    for node in [&node_a, &node_b] {
        let active_hashes: Vec<Vec<u8>> = node
            .get_links_by_fid(VECTOR_FID)
            .expect("get_links_by_fid")
            .messages
            .iter()
            .map(|m| m.hash.clone())
            .collect();
        assert!(
            !active_hashes.contains(&follow_add.hash),
            "follow link should be swept by the follow compaction"
        );
        assert!(
            active_hashes.contains(&block_add.hash),
            "block link must survive a type-scoped follow compaction at V19"
        );
    }
}
