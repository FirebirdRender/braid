use std::fmt;

/// Top-level error type for the Braid application.
///
/// Covers all error cases that can occur during data transfer:
/// I/O errors, protocol violations, CRC mismatches, negotiation
/// failures, channel exhaustion, timeouts, and graceful shutdown.
#[derive(Debug)]
pub enum BraidError {
    /// Standard I/O error (socket write/read, file I/O, etc.).
    Io(std::io::Error),
    /// Protocol-level violation (malformed header, unexpected message, etc.).
    Protocol(&'static str),
    /// CRC mismatch detected at the commit gate (fatal — data integrity risk).
    CrcMismatch {
        /// Sequence number of the chunk that failed CRC verification.
        sequence_number: u64,
    },
    /// File hash mismatch detected after receiving output data.
    FileHashMismatch {
        /// Expected CRC32C value for the file.
        expected: u32,
        /// Computed CRC32C value for the file.
        computed: u32,
    },
    /// Negotiation with the remote peer failed.
    NegotiationFailed(&'static str),
    /// All UDP channels failed — no path to the receiver remains.
    AllChannelsFailed,
    /// An operation timed out.
    Timeout,
    /// Graceful shutdown was requested (SIGINT/SIGTERM).
    Shutdown,
}

impl fmt::Display for BraidError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::CrcMismatch { sequence_number } => {
                write!(
                    f,
                    "CRC mismatch at commit gate: sequence_number={sequence_number}"
                )
            }
            Self::FileHashMismatch { expected, computed } => {
                write!(
                    f,
                    "file CRC32C mismatch: expected 0x{expected:08X}, got 0x{computed:08X}"
                )
            }
            Self::NegotiationFailed(msg) => write!(f, "negotiation failed: {msg}"),
            Self::AllChannelsFailed => write!(f, "all UDP channels failed"),
            Self::Timeout => write!(f, "operation timed out"),
            Self::Shutdown => write!(f, "graceful shutdown"),
        }
    }
}

impl std::error::Error for BraidError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for BraidError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Convenience alias for `Result<T, BraidError>`.
pub type Result<T> = std::result::Result<T, BraidError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn braid_error_io_display() {
        let err = BraidError::Io(std::io::Error::new(std::io::ErrorKind::Other, "disk full"));
        let msg = err.to_string();
        assert!(msg.contains("I/O error"));
        assert!(msg.contains("disk full"));
    }

    #[test]
    fn braid_error_protocol_display() {
        let err = BraidError::Protocol("bad header");
        assert_eq!(err.to_string(), "protocol error: bad header");
    }

    #[test]
    fn braid_error_crc_mismatch_display() {
        let err = BraidError::CrcMismatch {
            sequence_number: 42,
        };
        assert!(err.to_string().contains("CRC mismatch"));
        assert!(err.to_string().contains("42"));
    }

    #[test]
    fn braid_error_negotiation_failed_display() {
        let err = BraidError::NegotiationFailed("timeout");
        assert_eq!(err.to_string(), "negotiation failed: timeout");
    }

    #[test]
    fn braid_error_all_channels_failed_display() {
        let err = BraidError::AllChannelsFailed;
        assert_eq!(err.to_string(), "all UDP channels failed");
    }

    #[test]
    fn braid_error_timeout_display() {
        let err = BraidError::Timeout;
        assert_eq!(err.to_string(), "operation timed out");
    }

    #[test]
    fn braid_error_shutdown_display() {
        let err = BraidError::Shutdown;
        assert_eq!(err.to_string(), "graceful shutdown");
    }

    #[test]
    fn braid_error_io_source() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = BraidError::Io(io_err);
        assert!(err.source().is_some());
    }

    #[test]
    fn braid_error_non_io_source_is_none() {
        let err = BraidError::Protocol("bad");
        assert!(err.source().is_none());

        let err = BraidError::CrcMismatch { sequence_number: 0 };
        assert!(err.source().is_none());

        let err = BraidError::FileHashMismatch {
            expected: 0xDEADBEEF,
            computed: 0xCAFEBABE,
        };
        assert!(err.source().is_none());

        let err = BraidError::Shutdown;
        assert!(err.source().is_none());
    }

    #[test]
    fn braid_error_file_hash_mismatch_display() {
        let err = BraidError::FileHashMismatch {
            expected: 0xDEADBEEF,
            computed: 0xCAFEBABE,
        };
        assert_eq!(
            err.to_string(),
            "file CRC32C mismatch: expected 0xDEADBEEF, got 0xCAFEBABE"
        );
    }

    #[test]
    fn braid_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let err: BraidError = io_err.into();
        assert!(matches!(err, BraidError::Io(_)));
    }

    #[test]
    fn result_alias_works() {
        let ok: Result<i32> = Ok(42);
        assert_eq!(ok.unwrap(), 42);

        let err: Result<i32> = Err(BraidError::Shutdown);
        assert!(err.is_err());
    }

    #[test]
    fn braid_error_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<BraidError>();
    }

    #[test]
    fn braid_error_is_sync() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<BraidError>();
    }
}
