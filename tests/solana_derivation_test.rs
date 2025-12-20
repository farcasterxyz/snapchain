// Integration test for Solana Name Service derivation
// This test can be run independently: cargo test --test test_solana_derivation

use sha2::{Digest, Sha256};

const HASH_PREFIX: &str = "SPL Name Service";
const ROOT_DOMAIN_ACCOUNT: &str = "58PwtjSDuFHuUkYjH9BYnnQKHfwo9reZhC2zMJv9JPkx";
const NAME_SERVICE_PROGRAM_ID: &str = "namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX";

fn hash_domain_name(domain: &str) -> Vec<u8> {
    let prefixed = format!("{}{}", HASH_PREFIX, domain);
    Sha256::digest(prefixed.as_bytes()).to_vec()
}

fn create_program_address(
    seeds: &[&[u8]],
    bump: u8,
    program_id: &[u8; 32],
) -> Result<[u8; 32], String> {
    use ed25519_dalek::VerifyingKey;

    let mut hasher = Sha256::new();
    for seed in seeds {
        hasher.update(seed);
    }
    hasher.update(&[bump]);
    hasher.update(program_id);
    hasher.update(b"ProgramDerivedAddress");
    let hash = hasher.finalize();

    // Check if on curve
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&hash);

    if VerifyingKey::from_bytes(&bytes).is_ok() {
        return Err("Address is on curve".to_string());
    }

    Ok(bytes)
}

fn find_program_address(
    program_id: &[u8; 32],
    hashed_name: &[u8],
    name_class: Option<&[u8; 32]>,
    parent: Option<&[u8; 32]>,
) -> Result<[u8; 32], String> {
    let mut seeds_vec = Vec::new();
    seeds_vec.extend_from_slice(hashed_name);

    if let Some(class) = name_class {
        seeds_vec.extend_from_slice(class);
    } else {
        seeds_vec.extend_from_slice(&[0u8; 32]);
    }

    if let Some(parent_addr) = parent {
        seeds_vec.extend_from_slice(parent_addr);
    } else {
        seeds_vec.extend_from_slice(&[0u8; 32]);
    }

    let seed_chunks: Vec<&[u8]> = seeds_vec.chunks(32).collect();

    for bump in (0..=255u8).rev() {
        if let Ok(pda) = create_program_address(&seed_chunks, bump, program_id) {
            return Ok(pda);
        }
    }

    Err("Could not find valid PDA".to_string())
}

fn get_domain_key(domain: &str) -> Result<[u8; 32], String> {
    let hashed_name = hash_domain_name(domain);

    let root_domain = bs58::decode(ROOT_DOMAIN_ACCOUNT)
        .into_vec()
        .map_err(|e| format!("Failed to decode root domain: {}", e))?;
    if root_domain.len() != 32 {
        return Err("Invalid root domain length".to_string());
    }
    let mut root_array = [0u8; 32];
    root_array.copy_from_slice(&root_domain);

    let program_id = bs58::decode(NAME_SERVICE_PROGRAM_ID)
        .into_vec()
        .map_err(|e| format!("Failed to decode program ID: {}", e))?;
    if program_id.len() != 32 {
        return Err("Invalid program ID length".to_string());
    }
    let mut program_array = [0u8; 32];
    program_array.copy_from_slice(&program_id);

    find_program_address(&program_array, &hashed_name, None, Some(&root_array))
}

#[test]
fn test_bonfida_derivation() {
    let derived = get_domain_key("bonfida").expect("derive bonfida");
    let expected_bytes = bs58::decode("Crf8hzfthWGbGbLTVCiqRqV5MVnbpHB1L9KQMd6gsinb")
        .into_vec()
        .expect("decode expected");
    let expected: [u8; 32] = expected_bytes.try_into().expect("convert to array");

    assert_eq!(
        derived, expected,
        "bonfida.sol should derive to Crf8hzfthWGbGbLTVCiqRqV5MVnbpHB1L9KQMd6gsinb"
    );

    // Also verify as base58
    let derived_base58 = bs58::encode(&derived).into_string();
    assert_eq!(
        derived_base58,
        "Crf8hzfthWGbGbLTVCiqRqV5MVnbpHB1L9KQMd6gsinb"
    );
}

#[test]
fn test_derivation_is_deterministic() {
    let derived1 = get_domain_key("bonfida").expect("derive 1");
    let derived2 = get_domain_key("bonfida").expect("derive 2");
    assert_eq!(derived1, derived2, "Derivation should be deterministic");
}

#[test]
fn test_different_domains_differ() {
    let bonfida = get_domain_key("bonfida").expect("derive bonfida");
    let solana = get_domain_key("solana").expect("derive solana");
    assert_ne!(
        bonfida, solana,
        "Different domains should have different addresses"
    );
}
