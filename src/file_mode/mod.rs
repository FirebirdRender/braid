pub mod hash;
pub mod output;
pub mod receiver;
pub mod sanitize;
pub mod sender;
pub mod splitter;

use std::fmt;

/// Metadata for a file in file mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetadata {
    pub filename: String,
    pub filesize: u64,
    pub file_crc32c: u32,
}

impl FileMetadata {
    pub fn from_basename(basename: String, filesize: u64, hash: u32) -> Self {
        Self {
            filename: basename,
            filesize,
            file_crc32c: hash,
        }
    }
}

impl fmt::Display for FileMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({} bytes, crc32c={:08x})",
            self.filename, self.filesize, self.file_crc32c
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_basename_sets_fields() {
        let meta = FileMetadata::from_basename("sample.bin".to_string(), 42, 0xdead_beef);
        assert_eq!(meta.filename, "sample.bin");
        assert_eq!(meta.filesize, 42);
        assert_eq!(meta.file_crc32c, 0xdead_beef);
    }

    #[test]
    fn display_formats_metadata() {
        let meta = FileMetadata::from_basename("sample.bin".to_string(), 42, 0xdead_beef);
        assert_eq!(meta.to_string(), "sample.bin (42 bytes, crc32c=deadbeef)");
    }
}
