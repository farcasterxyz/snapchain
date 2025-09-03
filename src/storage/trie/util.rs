use crate::storage::trie::errors::TrieError;
use base64::Engine;

#[inline(always)]
fn expand_nibbles(input: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(input.len() * 2);
    for &byte in input {
        result.push(byte >> 4);
        result.push(byte & 0x0F);
    }
    result
}

#[inline(always)]
fn combine_nibbles(input: &[u8]) -> Vec<u8> {
    assert!(input.len() % 2 == 0, "Input length must be even");
    let mut result = Vec::with_capacity(input.len() / 2);
    for chunk in input.chunks_exact(2) {
        result.push((chunk[0] << 4) | (chunk[1] & 0x0F));
    }
    result
}

#[inline(always)]
fn expand_quibbles(input: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(input.len() * 4);
    for &byte in input {
        result.push((byte >> 6) & 0x03);
        result.push((byte >> 4) & 0x03);
        result.push((byte >> 2) & 0x03);
        result.push(byte & 0x03);
    }
    result
}

#[inline(always)]
fn combine_quibbles(input: &[u8]) -> Vec<u8> {
    assert!(input.len() % 4 == 0, "Input length must be a multiple of 4");
    let mut result = Vec::with_capacity(input.len() / 4);
    for chunk in input.chunks_exact(4) {
        result.push(
            ((chunk[0] & 0x03) << 6)
                | ((chunk[1] & 0x03) << 4)
                | ((chunk[2] & 0x03) << 2)
                | (chunk[3] & 0x03),
        );
    }
    result
}

#[inline(always)]
fn expand_byte(input: &[u8]) -> Vec<u8> {
    input.to_vec()
}

#[inline(always)]
fn combine_byte(input: &[u8]) -> Vec<u8> {
    input.to_vec()
}

#[derive(Clone)]
pub struct BranchingFactorTransform {
    pub expand: fn(&[u8]) -> Vec<u8>,
    pub combine: fn(&[u8]) -> Vec<u8>,
}

pub fn get_transform_functions(branching_factor: u32) -> Option<BranchingFactorTransform> {
    match branching_factor {
        4 => Some(BranchingFactorTransform {
            expand: expand_quibbles,
            combine: combine_quibbles,
        }),
        16 => Some(BranchingFactorTransform {
            expand: expand_nibbles,
            combine: combine_nibbles,
        }),
        256 => Some(BranchingFactorTransform {
            expand: expand_byte,
            combine: combine_byte,
        }),
        _ => None,
    }
}

/// Encode a trie iteration page token (Vec<Vec<u8>>) into a single base64 string.
/// Format:
/// [u8;4] big endian number of segments (N) followed by N repetitions of:
///   [u8;4] big endian segment length (L) followed by L bytes of segment.
/// Returns empty string if token is None or empty.
pub fn encode_trie_page_token(token: &Option<Vec<Vec<u8>>>) -> String {
    use base64::Engine;
    if token.is_none() {
        return String::new();
    }
    let token_ref = token.as_ref().unwrap();
    if token_ref.is_empty() {
        return String::new();
    }
    let mut buf: Vec<u8> =
        Vec::with_capacity(4 + token_ref.iter().map(|v| 4 + v.len()).sum::<usize>());
    let count: u32 = token_ref.len() as u32;
    buf.extend_from_slice(&count.to_be_bytes());
    for part in token_ref.iter() {
        let len = part.len() as u32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(part);
    }
    base64::engine::general_purpose::STANDARD.encode(buf)
}

/// Decode a base64 string produced by `encode_trie_token` back into a token structure.
/// Returns Ok(None) if input is empty.
pub fn decode_trie_page_token(encoded: &str) -> Result<Option<Vec<Vec<u8>>>, TrieError> {
    if encoded.is_empty() {
        return Ok(None);
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded.as_bytes())
        .map_err(|e| TrieError::TokenDecodeError(e.to_string()))?;
    if bytes.len() < 4 {
        return Err(TrieError::TokenDecodeError("Too short".into()));
    }
    let mut offset = 0usize;
    let count = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > bytes.len() {
            return Err(TrieError::TokenDecodeError(
                "Unexpected EOF reading length".into(),
            ));
        }
        let len = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + len > bytes.len() {
            return Err(TrieError::TokenDecodeError(
                "Unexpected EOF reading segment".into(),
            ));
        }
        let segment = bytes[offset..offset + len].to_vec();
        offset += len;
        out.push(segment);
    }
    if offset != bytes.len() {
        return Err(TrieError::TokenDecodeError("Extra bytes at end".into()));
    }
    if out.is_empty() {
        Ok(None)
    } else {
        Ok(Some(out))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::trie::util::{decode_trie_page_token, encode_trie_page_token};

    #[test]
    fn test_encode_decode_trie_token() {
        let original: Option<Vec<Vec<u8>>> = Some(vec![vec![1, 2, 3], vec![], vec![255]]);
        let encoded = encode_trie_page_token(&original);
        assert!(!encoded.is_empty());
        let decoded = decode_trie_page_token(&encoded).unwrap();
        assert_eq!(decoded, Some(vec![vec![1, 2, 3], vec![], vec![255]]));

        // Empty / None handling
        assert_eq!(encode_trie_page_token(&None), "");
        assert_eq!(decode_trie_page_token("").unwrap(), None);
        let empty: Option<Vec<Vec<u8>>> = Some(vec![]);
        assert_eq!(encode_trie_page_token(&empty), "");
    }
}
