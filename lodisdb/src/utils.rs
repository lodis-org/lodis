/// Create an integer value from its representation as a byte array in big endian.
pub fn u8x8_to_u64(u8x8: &[u8; 8]) -> u64 {
    u64::from_be_bytes(*u8x8)
}

pub fn u8x8_to_i64(u8x8: &[u8; 8]) -> i64 {
    i64::from_be_bytes(*u8x8)
}

pub fn u8x4_to_u32(u8x4: &[u8; 4]) -> u32 {
    u32::from_be_bytes(*u8x4)
}

/// Return the memory representation of this integer as a byte array in big-endian (network) byte
/// order.
pub fn u64_to_u8x8(u: u64) -> [u8; 8] {
    u.to_be_bytes()
}

pub fn i64_to_u8x8(u: i64) -> [u8; 8] {
    u.to_be_bytes()
}

pub fn u32_to_u8x4(u: u32) -> [u8; 4] {
    u.to_be_bytes()
}

pub fn u8_to_u8x1(u: u8) -> [u8; 1] {
    u.to_be_bytes()
}

pub fn is_numeric(buf: &str) -> bool {
    for (i, c) in buf.chars().enumerate() {
        if i == 0 {
            if !(c == '-' || c.is_numeric()) {
                return false;
            }
            continue;
        }
        if !c.is_numeric() {
            return false;
        }
    }
    return true;
}
