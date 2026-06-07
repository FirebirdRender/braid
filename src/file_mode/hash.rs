//! File mode utilities - stub module

use std::io;
use std::path::Path;

use tokio::io::{AsyncReadExt, BufReader};

pub async fn compute_file_crc32c(path: &Path) -> io::Result<u32> {
    let file = tokio::fs::File::open(path).await?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut hasher = crc32fast::Hasher::new();
    let mut buf = [0u8; 64 * 1024];

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hasher.finalize())
}

pub async fn verify_file_crc32c(path: &Path, expected: u32) -> io::Result<bool> {
    Ok(compute_file_crc32c(path).await? == expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_file_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("braid-{name}-{nonce}"))
    }

    async fn write_temp_file(path: &Path, data: &[u8]) {
        tokio::fs::write(path, data).await.unwrap();
    }

    #[tokio::test]
    async fn compute_file_crc32c_empty_file() {
        let path = temp_file_path("empty");
        write_temp_file(&path, b"").await;

        let crc = compute_file_crc32c(&path).await.unwrap();
        assert_eq!(crc, 0);

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn compute_file_crc32c_known_value() {
        let path = temp_file_path("hello");
        write_temp_file(&path, b"hello").await;

        let crc = compute_file_crc32c(&path).await.unwrap();
        assert_eq!(crc, 0x3610_a686);

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn compute_file_crc32c_large_file() {
        let path = temp_file_path("large");
        let data = vec![0xAB; 100 * 1024];
        write_temp_file(&path, &data).await;

        let crc = compute_file_crc32c(&path).await.unwrap();
        assert_eq!(crc, crc32fast::hash(&data));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn verify_file_crc32c_match() {
        let path = temp_file_path("match");
        let data = b"braid verification";
        write_temp_file(&path, data).await;

        let expected = crc32fast::hash(data);
        assert!(verify_file_crc32c(&path, expected).await.unwrap());

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn verify_file_crc32c_mismatch() {
        let path = temp_file_path("mismatch");
        write_temp_file(&path, b"braid verification").await;

        assert!(!verify_file_crc32c(&path, 0).await.unwrap());

        let _ = tokio::fs::remove_file(&path).await;
    }
}
