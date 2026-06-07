use std::path::Path;

pub fn sanitize_filename(input: &str) -> Result<String, &'static str> {
    let trimmed = input.trim();

    if trimmed.is_empty() || trimmed.contains("..") {
        return Err("invalid filename");
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err("path separator not allowed");
    }
    if trimmed.as_bytes().contains(&0) {
        return Err("null byte not allowed");
    }
    if trimmed.len() > 255 {
        return Err("filename too long");
    }

    let basename = Path::new(trimmed)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("invalid filename")?;

    Ok(basename.to_string())
}

#[cfg(test)]
mod tests {
    use super::sanitize_filename;

    #[test]
    fn sanitize_accepts_simple_name() {
        assert_eq!(sanitize_filename("file.bin").unwrap(), "file.bin");
    }

    #[test]
    fn sanitize_rejects_path_traversal() {
        assert_eq!(sanitize_filename("../../../etc/passwd"), Err("invalid filename"));
    }

    #[test]
    fn sanitize_rejects_absolute_path() {
        assert_eq!(sanitize_filename("/etc/passwd"), Err("path separator not allowed"));
    }

    #[test]
    fn sanitize_rejects_null_byte() {
        assert_eq!(sanitize_filename("file\0.bin"), Err("null byte not allowed"));
    }

    #[test]
    fn sanitize_rejects_too_long() {
        let input = "a".repeat(256);
        assert_eq!(sanitize_filename(&input), Err("filename too long"));
    }

    #[test]
    fn sanitize_strips_whitespace() {
        assert_eq!(sanitize_filename("  file.bin  ").unwrap(), "file.bin");
    }
}
