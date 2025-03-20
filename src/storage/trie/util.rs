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
