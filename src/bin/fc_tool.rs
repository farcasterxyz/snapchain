//! Sketch tool for submitting test messages against a snapchain HTTP API.
//!
//! Subcommands:
//!   key-add      — submit gasless KEY_ADD (generate fresh signer or reuse one with --signer-secret)
//!   key-remove   — submit KEY_REMOVE (custody-signed by default; --mode self-revoke for self-revoke)
//!   cast-add     — submit CAST_ADD signed by an existing Ed25519 key
//!   cast-remove  — submit CAST_REMOVE signed by an existing Ed25519 key
//!
//! `messages_factory::create_message_with_data` hardcodes `FarcasterNetwork::Mainnet`,
//! so every message is re-tagged via `retarget_network` before submission.
use alloy_signer_local::{coins_bip39::English, MnemonicBuilder, PrivateKeySigner};
use clap::{Parser, Subcommand, ValueEnum};
use ed25519_dalek::{Signer, SigningKey as EdSigningKey};
use prost::Message as _;
use rand::rngs::OsRng;
use rand::RngCore;
use snapchain::core::util::calculate_message_hash;
use snapchain::core::validations::key::MAX_KEY_TTL_SECONDS;
use snapchain::proto::{self, FarcasterNetwork, MessageType};
use snapchain::utils::factory::messages_factory;
use std::error::Error;

type BoxedError = Box<dyn Error>;

#[derive(Parser)]
#[command(
    name = "fc_tool",
    about = "Submit Farcaster messages against a snapchain HTTP API (testnet sketch)"
)]
struct Cli {
    /// HTTP API base URL.
    #[arg(long, default_value = "https://iris.farcaster.xyz:3381", global = true)]
    node: String,

    /// Network to tag messages with.
    #[arg(long, value_enum, default_value = "testnet", global = true)]
    network: NetworkArg,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Submit a gasless KEY_ADD. Generates a fresh signer unless --signer-secret is provided.
    KeyAdd(KeyAddArgs),
    /// Submit a KEY_REMOVE.
    KeyRemove(KeyRemoveArgs),
    /// Submit a CAST_ADD signed by an existing Ed25519 key.
    CastAdd(CastAddArgs),
    /// Submit a CAST_REMOVE signed by an existing Ed25519 key.
    CastRemove(CastRemoveArgs),
}

#[derive(clap::Args)]
struct CastAddArgs {
    #[arg(long)]
    fid: u64,

    /// Cast text body.
    #[arg(long)]
    text: String,

    /// Hex Ed25519 secret of the signing key.
    #[arg(long, env = "SIGNER_SECRET")]
    signer_secret: String,
}

#[derive(clap::Args)]
struct KeyAddArgs {
    /// Target FID (the FID owning the custody key derived from MNEMONIC).
    #[arg(long)]
    fid: u64,

    /// "App" FID for SignedKeyRequestMetadata. Defaults to --fid for self-add.
    #[arg(long)]
    request_fid: Option<u64>,

    /// User nonce; must exceed the previously stored nonce for this FID.
    #[arg(long)]
    nonce: u32,

    /// TTL in seconds (max 7,776,000 = 90 days).
    #[arg(long, default_value_t = MAX_KEY_TTL_SECONDS)]
    ttl: u32,

    /// Custody-signature deadline, in seconds from now (Farcaster epoch).
    #[arg(long, default_value_t = 600)]
    deadline_secs: u32,

    /// BIP-44 derivation path. Default = standard Ethereum account 0, address 0.
    #[arg(long, default_value = "m/44'/60'/0'/0/0")]
    path: String,

    /// Comma-separated scope list. Defaults to all valid scopes.
    /// Valid values: cast-add, cast-remove, reaction-add, reaction-remove,
    /// link-add, link-remove, verification-add, verification-remove,
    /// user-data-add, username-proof, frame-action, link-compact-state.
    #[arg(long, value_delimiter = ',', value_enum)]
    scopes: Option<Vec<ScopeArg>>,

    /// Hex Ed25519 secret (32 bytes, optional 0x prefix). If supplied, reuse this
    /// key instead of generating a fresh one — useful for re-issuing KEY_ADD with
    /// a different scope set against the same pubkey.
    #[arg(long, env = "SIGNER_SECRET")]
    signer_secret: Option<String>,

    /// Skip the y/N custody-address confirmation prompt.
    #[arg(long)]
    yes: bool,
}

#[derive(clap::Args)]
struct KeyRemoveArgs {
    #[arg(long)]
    fid: u64,

    /// KEY_REMOVE signature mode. `custody` requires MNEMONIC + an envelope signer;
    /// `self-revoke` requires only the key being revoked.
    #[arg(long, value_enum, default_value = "custody")]
    mode: KeyRemoveMode,

    /// Hex pubkey of the key to revoke (32 bytes). Required for custody mode;
    /// ignored for self-revoke (derived from --signer-secret).
    #[arg(long)]
    target_key: Option<String>,

    #[arg(long)]
    nonce: u32,

    #[arg(long, default_value_t = 600)]
    deadline_secs: u32,

    /// BIP-44 derivation path (custody mode only).
    #[arg(long, default_value = "m/44'/60'/0'/0/0")]
    path: String,

    /// Hex Ed25519 secret. In custody mode, this is the envelope signer (any active
    /// key on the FID). In self-revoke mode, this is the key being revoked.
    #[arg(long, env = "SIGNER_SECRET")]
    signer_secret: String,

    /// Skip the confirmation prompt.
    #[arg(long)]
    yes: bool,
}

#[derive(clap::Args)]
struct CastRemoveArgs {
    #[arg(long)]
    fid: u64,

    /// Target cast hash (hex, optional 0x prefix).
    #[arg(long)]
    target_hash: String,

    /// Hex Ed25519 secret of the signing key.
    #[arg(long, env = "SIGNER_SECRET")]
    signer_secret: String,
}

#[derive(ValueEnum, Clone, Copy)]
enum NetworkArg {
    Mainnet,
    Testnet,
    Devnet,
}

impl NetworkArg {
    fn as_proto(self) -> FarcasterNetwork {
        match self {
            NetworkArg::Mainnet => FarcasterNetwork::Mainnet,
            NetworkArg::Testnet => FarcasterNetwork::Testnet,
            NetworkArg::Devnet => FarcasterNetwork::Devnet,
        }
    }
}

#[derive(ValueEnum, Clone, Copy)]
enum KeyRemoveMode {
    Custody,
    SelfRevoke,
}

#[derive(ValueEnum, Clone, Copy)]
enum ScopeArg {
    CastAdd,
    CastRemove,
    ReactionAdd,
    ReactionRemove,
    LinkAdd,
    LinkRemove,
    VerificationAdd,
    VerificationRemove,
    UserDataAdd,
    UsernameProof,
    FrameAction,
    LinkCompactState,
}

impl ScopeArg {
    fn as_message_type(self) -> MessageType {
        match self {
            ScopeArg::CastAdd => MessageType::CastAdd,
            ScopeArg::CastRemove => MessageType::CastRemove,
            ScopeArg::ReactionAdd => MessageType::ReactionAdd,
            ScopeArg::ReactionRemove => MessageType::ReactionRemove,
            ScopeArg::LinkAdd => MessageType::LinkAdd,
            ScopeArg::LinkRemove => MessageType::LinkRemove,
            ScopeArg::VerificationAdd => MessageType::VerificationAddEthAddress,
            ScopeArg::VerificationRemove => MessageType::VerificationRemove,
            ScopeArg::UserDataAdd => MessageType::UserDataAdd,
            ScopeArg::UsernameProof => MessageType::UsernameProof,
            ScopeArg::FrameAction => MessageType::FrameAction,
            ScopeArg::LinkCompactState => MessageType::LinkCompactState,
        }
    }
}

fn default_scopes() -> Vec<MessageType> {
    vec![
        MessageType::CastAdd,
        MessageType::CastRemove,
        MessageType::ReactionAdd,
        MessageType::ReactionRemove,
        MessageType::LinkAdd,
        MessageType::LinkRemove,
        MessageType::VerificationAddEthAddress,
        MessageType::VerificationRemove,
        MessageType::UserDataAdd,
        MessageType::UsernameProof,
        MessageType::FrameAction,
        MessageType::LinkCompactState,
    ]
}

// ---------- helpers ----------

fn parse_hex(s: &str) -> Result<Vec<u8>, BoxedError> {
    Ok(hex::decode(s.trim().trim_start_matches("0x"))?)
}

fn parse_secret(s: &str) -> Result<EdSigningKey, BoxedError> {
    let bytes = parse_hex(s)?;
    if bytes.len() != 32 {
        return Err(format!("expected 32-byte secret, got {}", bytes.len()).into());
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(EdSigningKey::from_bytes(&arr))
}

fn parse_pubkey(s: &str) -> Result<[u8; 32], BoxedError> {
    let bytes = parse_hex(s)?;
    if bytes.len() != 32 {
        return Err(format!("expected 32-byte pubkey, got {}", bytes.len()).into());
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}

fn derive_custody(path: &str) -> Result<PrivateKeySigner, BoxedError> {
    let phrase =
        std::env::var("MNEMONIC").map_err(|_| "Set MNEMONIC env var to your recovery phrase")?;
    Ok(MnemonicBuilder::<English>::default()
        .phrase(phrase.trim())
        .derivation_path(path)?
        .build()?)
}

fn confirm(prompt: &str, skip: bool) -> Result<(), BoxedError> {
    if skip {
        return Ok(());
    }
    print!("{} [y/N]: ", prompt);
    std::io::Write::flush(&mut std::io::stdout())?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    if !matches!(answer.trim(), "y" | "Y" | "yes" | "YES") {
        return Err("aborted by user".into());
    }
    Ok(())
}

/// Re-tag a message built by the factory (which hardcodes `Mainnet`) for a different
/// network. Mutates `network`, recomputes the BLAKE3 hash, and re-signs the envelope.
fn retarget_network(msg: &mut proto::Message, network: FarcasterNetwork, signer: &EdSigningKey) {
    let data = msg.data.as_mut().expect("factory always sets data");
    data.network = network as i32;
    let data_bytes = data.encode_to_vec();
    let hash = calculate_message_hash(&data_bytes);
    msg.signature = signer.sign(&hash).to_bytes().to_vec();
    msg.hash = hash;
}

/// HTTP submit. Tolerates the KEY_ADD/KEY_REMOVE JSON-mapping TODO (NEYN-10568,
/// `src/network/http_server.rs:1933`): the merge succeeds but the response can't
/// be serialized, so the exact 400 body is treated as success.
async fn submit(node: &str, msg: &proto::Message, label: &str) -> Result<(), BoxedError> {
    let url = format!("{}/v1/submitMessage", node.trim_end_matches('/'));
    println!("Submitting {} (hash 0x{})", label, hex::encode(&msg.hash));
    let resp = reqwest::Client::new()
        .post(&url)
        .header("content-type", "application/octet-stream")
        .body(msg.encode_to_vec())
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await?;
    println!("HTTP {}: {}", status, text);
    let json_mapping_todo = status == reqwest::StatusCode::BAD_REQUEST
        && text.contains("JSON mapping not yet implemented");
    if json_mapping_todo {
        println!("(merged on-node; response-mapping TODO returns 400 — treating as success.)");
        return Ok(());
    }
    if !status.is_success() {
        return Err(format!("submit failed: {}", status).into());
    }
    Ok(())
}

fn fresh_ed25519() -> EdSigningKey {
    let mut secret = [0u8; 32];
    OsRng.fill_bytes(&mut secret);
    let key = EdSigningKey::from_bytes(&secret);
    println!();
    println!("=== NEW SIGNER (save these now) ===");
    println!("  secret:  0x{}", hex::encode(secret));
    println!(
        "  pubkey:  0x{}",
        hex::encode(key.verifying_key().to_bytes())
    );
    println!("===================================");
    println!();
    key
}

// ---------- subcommands ----------

async fn run_key_add(
    args: KeyAddArgs,
    node: &str,
    network: FarcasterNetwork,
) -> Result<(), BoxedError> {
    let custody = derive_custody(&args.path)?;
    println!("Custody address: {}", custody.address());
    println!("FID:             {}", args.fid);
    confirm(
        "Does this address match the custody on file for the FID?",
        args.yes,
    )?;

    let envelope_signer = match args.signer_secret.as_deref() {
        Some(s) => {
            let key = parse_secret(s)?;
            println!(
                "Reusing signer pubkey: 0x{}",
                hex::encode(key.verifying_key().to_bytes())
            );
            key
        }
        None => fresh_ed25519(),
    };

    let scopes: Vec<MessageType> = match args.scopes {
        Some(v) => v.into_iter().map(ScopeArg::as_message_type).collect(),
        None => default_scopes(),
    };

    let request_fid = args.request_fid.unwrap_or(args.fid);
    let now = messages_factory::farcaster_time();
    let deadline = now + args.deadline_secs;

    let mut msg = messages_factory::keys::create_key_add(
        args.fid,
        &custody,
        request_fid,
        &custody,
        &envelope_signer,
        scopes,
        args.ttl,
        args.nonce,
        deadline,
        None,
    );
    retarget_network(&mut msg, network, &envelope_signer);
    submit(node, &msg, "KEY_ADD").await
}

async fn run_key_remove(
    args: KeyRemoveArgs,
    node: &str,
    network: FarcasterNetwork,
) -> Result<(), BoxedError> {
    let signer = parse_secret(&args.signer_secret)?;
    let now = messages_factory::farcaster_time();
    let deadline = now + args.deadline_secs;

    let mut msg = match args.mode {
        KeyRemoveMode::Custody => {
            let target = args
                .target_key
                .as_deref()
                .ok_or("--target-key is required for custody mode")?;
            let target_key = parse_pubkey(target)?;
            let custody = derive_custody(&args.path)?;
            println!("Custody address: {}", custody.address());
            println!("FID:             {}", args.fid);
            println!("Removing key:    0x{}", hex::encode(target_key));
            confirm("Confirm KEY_REMOVE (custody)?", args.yes)?;
            messages_factory::keys::create_key_remove_custody(
                args.fid,
                &custody,
                &signer,
                &target_key,
                args.nonce,
                deadline,
                None,
            )
        }
        KeyRemoveMode::SelfRevoke => {
            let pk = signer.verifying_key().to_bytes();
            println!("FID:           {}", args.fid);
            println!("Self-revoking: 0x{}", hex::encode(pk));
            confirm("Confirm KEY_REMOVE (self-revoke)?", args.yes)?;
            messages_factory::keys::create_key_remove_self_revoke(
                args.fid, &signer, args.nonce, deadline, None,
            )
        }
    };
    retarget_network(&mut msg, network, &signer);
    submit(node, &msg, "KEY_REMOVE").await
}

async fn run_cast_add(
    args: CastAddArgs,
    node: &str,
    network: FarcasterNetwork,
) -> Result<(), BoxedError> {
    let signer = parse_secret(&args.signer_secret)?;
    let mut msg =
        messages_factory::casts::create_cast_add(args.fid, &args.text, None, Some(&signer));
    retarget_network(&mut msg, network, &signer);
    submit(node, &msg, "CAST_ADD").await
}

async fn run_cast_remove(
    args: CastRemoveArgs,
    node: &str,
    network: FarcasterNetwork,
) -> Result<(), BoxedError> {
    let signer = parse_secret(&args.signer_secret)?;
    let target_hash = parse_hex(&args.target_hash)?;
    let mut msg =
        messages_factory::casts::create_cast_remove(args.fid, &target_hash, None, Some(&signer));
    retarget_network(&mut msg, network, &signer);
    submit(node, &msg, "CAST_REMOVE").await
}

#[tokio::main]
async fn main() -> Result<(), BoxedError> {
    let cli = Cli::parse();
    let network = cli.network.as_proto();
    match cli.cmd {
        Cmd::KeyAdd(a) => run_key_add(a, &cli.node, network).await,
        Cmd::KeyRemove(a) => run_key_remove(a, &cli.node, network).await,
        Cmd::CastAdd(a) => run_cast_add(a, &cli.node, network).await,
        Cmd::CastRemove(a) => run_cast_remove(a, &cli.node, network).await,
    }
}
