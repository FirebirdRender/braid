use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompressionError {
    CompressError(String),
    DecompressError(String),
}

impl fmt::Display for CompressionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionError::CompressError(msg) => write!(f, "compress error: {msg}"),
            CompressionError::DecompressError(msg) => write!(f, "decompress error: {msg}"),
        }
    }
}

impl std::error::Error for CompressionError {}

pub fn compress_lz4(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    Ok(lz4_flex::compress_prepend_size(data))
}

pub fn decompress_lz4(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| CompressionError::DecompressError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_lz4_roundtrip() {
        let data = b"Hello, BRAID! This is a test of LZ4 compression.";
        let compressed = compress_lz4(data).expect("compress should succeed");
        let decompressed = decompress_lz4(&compressed).expect("decompress should succeed");
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compress_lz4_reduces_size() {
        let data = vec![0xABu8; 4096];
        let compressed = compress_lz4(&data).expect("compress should succeed");
        assert!(
            compressed.len() < data.len(),
            "compressed size {} should be less than original {}",
            compressed.len(),
            data.len()
        );
    }

    #[test]
    fn decompress_lz4_corrupted_data_returns_error() {
        let corrupted = vec![0xFFu8; 64];
        let result = decompress_lz4(&corrupted);
        assert!(result.is_err(), "corrupted data should produce an error");
    }

    #[test]
    fn compress_lz4_empty_input() {
        let data = b"";
        let compressed = compress_lz4(data).expect("compress should succeed");
        let decompressed = decompress_lz4(&compressed).expect("decompress should succeed");
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compress_lz4_roundtrip_various_sizes() {
        let sizes = [0, 1, 1024, 65537, 1_048_576];
        for &size in &sizes {
            let data: Vec<u8> = if size == 0 {
                vec![]
            } else {
                (0..size).map(|i| (i % 251) as u8).collect()
            };
            let compressed = compress_lz4(&data).expect("compress should succeed");
            let decompressed = decompress_lz4(&compressed).expect("decompress should succeed");
            assert_eq!(decompressed, data, "roundtrip failed for size {size}");
        }
    }

    #[test]
    fn compression_error_display() {
        let err = CompressionError::CompressError("oops".into());
        assert_eq!(format!("{err}"), "compress error: oops");

        let err = CompressionError::DecompressError("bad data".into());
        assert_eq!(format!("{err}"), "decompress error: bad data");
    }
}
