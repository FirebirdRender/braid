//! File mode receiver — resolves output paths and validates file integrity.
//!
//! `FileModeReceiver` takes an optional output path override, resolves the
//! final output path from sender-supplied metadata, and computes CRC32C
//! hashes for post-transfer validation.

use std::path::{Path, PathBuf};

use crate::error::{BraidError, Result};
use crate::file_mode::{hash, output, sanitize, FileMetadata};

/// A file-mode receiver that resolves output paths from `FileMetadata`.
///
/// If an explicit `output_override` is provided, its path is used
/// (after conflict resolution via `output::resolve_output_path`).
/// Otherwise, the sender's filename is sanitized and used directly.
pub struct FileModeReceiver {
    output_override: Option<PathBuf>,
}

impl FileModeReceiver {
    /// Create a new `FileModeReceiver`.
    ///
    /// `output_override` — if `Some(path)`, that path is used as the
    /// output destination after conflict resolution (auto-rename on
    /// collision). If `None`, the sender's filename from
    /// `FileMetadata` is used.
    pub fn new(output_override: Option<PathBuf>) -> Self {
        Self { output_override }
    }

    /// Resolve the output path for a file transfer.
    ///
    /// 1. If `self.output_override` is `Some`, use that path after
    ///    `output::resolve_output_path` to handle file-exists conflicts.
    /// 2. Otherwise, sanitize `metadata.filename` via
    ///    `sanitize::sanitize_filename` and use it as the output path.
    ///
    /// Both paths go through `output::resolve_output_path` to prevent
    /// accidental overwrites.
    pub async fn resolve_output_path(&self, metadata: &FileMetadata) -> Result<PathBuf> {
        let desired = if let Some(ref override_path) = self.output_override {
            override_path.clone()
        } else {
            let sanitized =
                sanitize::sanitize_filename(&metadata.filename).map_err(BraidError::Protocol)?;
            PathBuf::from(sanitized)
        };

        output::resolve_output_path(&desired)
            .await
            .map_err(BraidError::Io)
    }

    /// Compute the CRC32C hash of a file on disk.
    ///
    /// This is used after the file is fully written to validate
    /// integrity against the expected hash from `FileStart`.
    pub async fn compute_hash(&self, path: &Path) -> Result<u32> {
        hash::compute_file_crc32c(path)
            .await
            .map_err(BraidError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::tempdir;

    fn temp_file_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("braid-recv-{name}-{nonce}"))
    }

    async fn write_temp_file(path: &Path, data: &[u8]) {
        tokio::fs::write(path, data).await.unwrap();
    }

    fn make_metadata(filename: &str, filesize: u64, hash: u32) -> FileMetadata {
        FileMetadata::from_basename(filename.to_string(), filesize, hash)
    }

    #[tokio::test]
    async fn resolve_uses_sender_filename() {
        let receiver = FileModeReceiver::new(None);
        let meta = make_metadata("myfile.bin", 100, 0x12345678);

        let resolved = receiver.resolve_output_path(&meta).await.unwrap();

        assert_eq!(resolved.file_name().unwrap(), "myfile.bin");
    }

    #[tokio::test]
    async fn resolve_uses_explicit_output() {
        let dir = tempdir().unwrap();
        let override_path = dir.path().join("explicit.bin");
        let receiver = FileModeReceiver::new(Some(override_path.clone()));
        let meta = make_metadata("sender_file.bin", 100, 0x12345678);

        let resolved = receiver.resolve_output_path(&meta).await.unwrap();

        assert_eq!(resolved, override_path);
    }

    #[tokio::test]
    async fn resolve_sanitizes_filename() {
        let receiver = FileModeReceiver::new(None);
        // Sender tries path traversal — sanitizer must reject.
        let meta = make_metadata("foo/../bar.bin", 100, 0x12345678);

        let result = receiver.resolve_output_path(&meta).await;
        assert!(result.is_err(), "should reject path traversal");
    }

    #[tokio::test]
    async fn compute_hash_matches_expected() {
        let path = temp_file_path("match");
        let data = b"braid integrity check";
        write_temp_file(&path, data).await;

        let receiver = FileModeReceiver::new(None);
        let computed = receiver.compute_hash(&path).await.unwrap();
        let expected = crc32fast::hash(data);

        assert_eq!(computed, expected);

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn compute_hash_mismatch() {
        let path = temp_file_path("badhash");
        let data = b"some data";
        write_temp_file(&path, data).await;

        let receiver = FileModeReceiver::new(None);
        // Compute hash of a different string
        let computed = receiver.compute_hash(&path).await.unwrap();
        let wrong_hash = crc32fast::hash(b"different data");

        assert_ne!(computed, wrong_hash);

        let _ = tokio::fs::remove_file(&path).await;
    }
}
