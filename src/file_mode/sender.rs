//! File sender — prepares a local file for transfer in file mode.
//!
//! `FileModeSender` wraps a local file path, validates it is a regular
//! file, and provides methods to extract metadata (size, hash, basename)
//! and open the file for streaming reads.

use std::path::PathBuf;

use crate::error::{BraidError, Result};
use crate::file_mode::{hash, FileMetadata};

/// A file-mode sender that wraps a local file path.
///
/// `FileModeSender` validates that the path points to a regular file
/// and provides methods to extract metadata and open the file for
/// streaming reads.
pub struct FileModeSender {
    input_path: PathBuf,
}

impl FileModeSender {
    /// Create a new `FileModeSender` for the given path.
    ///
    /// Validates that the path exists and is a regular file.
    /// Returns `BraidError::Io` if the path cannot be stat'd and
    /// `BraidError::Protocol` if the path is not a regular file.
    pub fn new(input_path: PathBuf) -> Result<Self> {
        let metadata = std::fs::metadata(&input_path)?;

        if !metadata.is_file() {
            return Err(BraidError::Protocol(
                "input path is not a regular file",
            ));
        }

        Ok(Self { input_path })
    }

    /// Compute file metadata: hash, size, and basename.
    ///
    /// Hashes the file content via streaming CRC32C (never loads the
    /// entire file into memory), reads the file size from the
    /// filesystem, and extracts the basename from the path.
    pub async fn prepare(&self) -> Result<FileMetadata> {
        let file_crc32c = hash::compute_file_crc32c(&self.input_path).await?;

        let meta = tokio::fs::metadata(&self.input_path).await?;
        let filesize = meta.len();

        let filename = self
            .input_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                BraidError::Protocol("invalid filename in input path")
            })?
            .to_string();

        Ok(FileMetadata::from_basename(filename, filesize, file_crc32c))
    }

    /// Open the input file for async reading.
    ///
    /// Returns a `tokio::fs::File` ready for streaming reads.
    /// The caller is responsible for not calling `seek(0)` — the
    /// splitter handles file positioning.
    pub async fn open_async(&self) -> std::io::Result<tokio::fs::File> {
        tokio::fs::File::open(&self.input_path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_file_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("braid-sender-{name}-{nonce}"))
    }

    async fn write_temp_file(path: &Path, data: &[u8]) {
        tokio::fs::write(path, data).await.unwrap();
    }

    fn known_crc32c(data: &[u8]) -> u32 {
        crc32fast::hash(data)
    }

    #[tokio::test]
    async fn prepare_returns_correct_metadata() {
        let path = temp_file_path("meta");
        let data = b"hello world";
        write_temp_file(&path, data).await;

        let sender = FileModeSender::new(path.clone()).unwrap();
        let meta = sender.prepare().await.unwrap();

        // Filename is the full basename of the temp path, not stripped.
        // Filename is the full basename of the temp path, not stripped.
        let expected_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap()
            .to_string();
        assert_eq!(meta.filename, expected_name);
        assert_eq!(meta.filesize, data.len() as u64);
        assert_eq!(meta.file_crc32c, known_crc32c(data));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn prepare_nonexistent_file_errors() {
        let path = PathBuf::from(
            "/nonexistent-file-for-braid-prepare-test-abc123",
        );
        let result = FileModeSender::new(path);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn prepare_directory_errors() {
        let path = PathBuf::from("/tmp");
        let result = FileModeSender::new(path);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn extract_filename_strips_path() {
        let dir = temp_file_path("subdir");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let file_path = dir.join("bar.bin");
        write_temp_file(&file_path, b"data").await;

        let sender = FileModeSender::new(file_path.clone()).unwrap();
        let meta = sender.prepare().await.unwrap();
        assert_eq!(meta.filename, "bar.bin");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
