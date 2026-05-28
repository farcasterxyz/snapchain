//! Small utilities lifted from the snapchain crate so the CLI doesn't have to depend on it.
//!
//! Keep these copies in sync with the originals (one-liners, easy to audit):
//!   - [`calculate_message_hash`] mirrors `snapchain::core::util::calculate_message_hash`
//!   - [`MAX_KEY_TTL_SECONDS`] mirrors `snapchain::core::validations::key::MAX_KEY_TTL_SECONDS`
//!   - [`farcaster_time`] mirrors `snapchain::utils::factory::time::farcaster_time`

pub const FARCASTER_EPOCH: u64 = 1_609_459_200_000;

pub const MAX_KEY_TTL_SECONDS: u32 = 90 * 24 * 60 * 60;

pub fn calculate_message_hash(data_bytes: &[u8]) -> Vec<u8> {
    blake3::hash(data_bytes).as_bytes()[0..20].to_vec()
}

fn current_timestamp() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before unix epoch")
        .as_secs() as u32
}

pub fn farcaster_time() -> u32 {
    current_timestamp() - (FARCASTER_EPOCH / 1000) as u32
}
