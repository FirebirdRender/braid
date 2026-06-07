/// Compute fragment CRC32C over a single buffer.
pub fn compute_fragment_crc(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Compute chunk CRC32C over `seq` (big-endian u64) followed by `data`.
/// Uses `crc32fast::Hasher` to avoid allocating a temporary Vec.
pub fn compute_chunk_crc(seq: u64, data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&seq.to_be_bytes());
    hasher.update(data);
    hasher.finalize()
}

pub fn verify_fragment_crc(data: &[u8], expected: u32) -> bool {
    compute_fragment_crc(data) == expected
}

pub fn verify_chunk_crc(seq: u64, data: &[u8], expected: u32) -> bool {
    compute_chunk_crc(seq, data) == expected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragment_crc_round_trip_known_data() {
        let data = b"braid crc32c";
        let crc = compute_fragment_crc(data);
        assert!(verify_fragment_crc(data, crc));
    }

    #[test]
    fn fragment_crc_rejects_corruption() {
        let data = b"braid crc32c";
        let crc = compute_fragment_crc(data);
        let corrupted = b"braid crc32d";
        assert!(!verify_fragment_crc(corrupted, crc));
    }

    #[test]
    fn chunk_crc_includes_sequence_number() {
        let data = b"payload";
        let crc1 = compute_chunk_crc(1, data);
        let crc2 = compute_chunk_crc(2, data);
        assert_ne!(crc1, crc2);
        assert!(verify_chunk_crc(1, data, crc1));
        assert!(!verify_chunk_crc(2, data, crc1));
    }
}
