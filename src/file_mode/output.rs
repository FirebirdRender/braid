//! File mode utilities for output file handling.

use std::path::{Path, PathBuf};

/// Resolve an output path without overwriting an existing file.
///
/// If `desired` already exists, appends ` (N)` before the extension until an
/// unused name is found.
pub async fn resolve_output_path(desired: &Path) -> std::io::Result<PathBuf> {
    if !tokio::fs::try_exists(desired).await? {
        return Ok(desired.to_path_buf());
    }

    let parent = desired.parent().map(Path::to_path_buf).unwrap_or_default();
    let stem = desired
        .file_stem()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    let extension = desired.extension().map(|e| e.to_os_string());

    let mut n = 1;
    loop {
        let mut candidate_name = stem.clone();
        candidate_name.push(format!(" ({n})"));
        if let Some(ext) = &extension {
            candidate_name.push(".");
            candidate_name.push(ext);
        }

        let candidate = parent.join(candidate_name);
        if !tokio::fs::try_exists(&candidate).await? {
            return Ok(candidate);
        }
        n += 1;
    }
}

/// Best-effort delete of an output file.
pub async fn delete_output(path: &Path) -> std::io::Result<()> {
    let _ = tokio::fs::remove_file(path).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn resolve_no_conflict() {
        let dir = tempdir().unwrap();
        let desired = dir.path().join("file.bin");

        let resolved = resolve_output_path(&desired).await.unwrap();

        assert_eq!(resolved, desired);
    }

    #[tokio::test]
    async fn resolve_with_conflict() {
        let dir = tempdir().unwrap();
        let desired = dir.path().join("file.bin");
        tokio::fs::write(&desired, b"x").await.unwrap();

        let resolved = resolve_output_path(&desired).await.unwrap();

        assert_eq!(resolved, dir.path().join("file (1).bin"));
    }

    #[tokio::test]
    async fn resolve_with_multiple_conflicts() {
        let dir = tempdir().unwrap();
        let desired = dir.path().join("file.bin");
        tokio::fs::write(&desired, b"x").await.unwrap();
        tokio::fs::write(dir.path().join("file (1).bin"), b"x")
            .await
            .unwrap();

        let resolved = resolve_output_path(&desired).await.unwrap();

        assert_eq!(resolved, dir.path().join("file (2).bin"));
    }

    #[tokio::test]
    async fn resolve_no_extension() {
        let dir = tempdir().unwrap();
        let desired = dir.path().join("file");
        tokio::fs::write(&desired, b"x").await.unwrap();

        let resolved = resolve_output_path(&desired).await.unwrap();

        assert_eq!(resolved, dir.path().join("file (1)"));
    }
}
