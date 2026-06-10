use std::convert::TryFrom;
use std::fmt;

use bytes::{Buf, BufMut, BytesMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ControlMessageKind {
    Hello = 0x01,
    Ack = 0x02,
    Nack = 0x03,
    DataReady = 0x04,
    Eos = 0x05,
    Reconnect = 0x06,
    Stats = 0x07,
    QueueStatus = 0x08,
    ChannelOpened = 0x09,
    ChannelClosed = 0x0A,
    FileStart = 0x0B,
    FileComplete = 0x0C,
    Error = 0xFF,
}

impl TryFrom<u8> for ControlMessageKind {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, <Self as TryFrom<u8>>::Error> {
        Ok(match value {
            0x01 => Self::Hello,
            0x02 => Self::Ack,
            0x03 => Self::Nack,
            0x04 => Self::DataReady,
            0x05 => Self::Eos,
            0x06 => Self::Reconnect,
            0x07 => Self::Stats,
            0x08 => Self::QueueStatus,
            0x09 => Self::ChannelOpened,
            0x0A => Self::ChannelClosed,
            0x0B => Self::FileStart,
            0x0C => Self::FileComplete,
            0xFF => Self::Error,
            _ => return Err("invalid control message kind"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlMessage {
    Hello {
        protocol_version: u16,
        features: u32,
    },
    Ack {
        sequence_number: u64,
    },
    Nack {
        sequence_number: u64,
        reason: u16,
    },
    DataReady {
        chunk_id: u32,
        payload_length: u16,
    },
    Eos {
        sequence_number: u64,
    },
    Reconnect {
        last_sequence_number: u64,
    },
    Stats {
        bytes_sent: u64,
        bytes_received: u64,
        active_channels: u16,
    },
    QueueStatus {
        queued_chunks: u32,
        queued_bytes: u32,
        total_capacity: u32,
    },
    ChannelOpened {
        channel_id: u16,
        port: u16,
    },
    ChannelClosed {
        channel_id: u16,
        code: u16,
    },
    FileStart {
        filename: String,
        filesize: u64,
        file_crc32c: u32,
    },
    FileComplete {
        success: bool,
        expected_hash: u32,
        computed_hash: u32,
    },
    Error {
        code: u16,
        detail: u32,
    },
}

impl ControlMessage {
    pub fn kind(&self) -> ControlMessageKind {
        match self {
            Self::Hello { .. } => ControlMessageKind::Hello,
            Self::Ack { .. } => ControlMessageKind::Ack,
            Self::Nack { .. } => ControlMessageKind::Nack,
            Self::DataReady { .. } => ControlMessageKind::DataReady,
            Self::Eos { .. } => ControlMessageKind::Eos,
            Self::Reconnect { .. } => ControlMessageKind::Reconnect,
            Self::Stats { .. } => ControlMessageKind::Stats,
            Self::QueueStatus { .. } => ControlMessageKind::QueueStatus,
            Self::ChannelOpened { .. } => ControlMessageKind::ChannelOpened,
            Self::ChannelClosed { .. } => ControlMessageKind::ChannelClosed,
            Self::FileStart { .. } => ControlMessageKind::FileStart,
            Self::FileComplete { .. } => ControlMessageKind::FileComplete,
            Self::Error { .. } => ControlMessageKind::Error,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(1 + 4 + 4 + 4);
        buf.put_u8(self.kind() as u8);
        match self {
            Self::Hello {
                protocol_version,
                features,
            } => {
                buf.put_u16(*protocol_version);
                buf.put_u32(*features);
            }
            Self::Ack { sequence_number } => buf.put_u64(*sequence_number),
            Self::Nack {
                sequence_number,
                reason,
            } => {
                buf.put_u64(*sequence_number);
                buf.put_u16(*reason);
            }
            Self::DataReady {
                chunk_id,
                payload_length,
            } => {
                buf.put_u32(*chunk_id);
                buf.put_u16(*payload_length);
            }
            Self::Eos { sequence_number } => buf.put_u64(*sequence_number),
            Self::Reconnect {
                last_sequence_number,
            } => buf.put_u64(*last_sequence_number),
            Self::Stats {
                bytes_sent,
                bytes_received,
                active_channels,
            } => {
                buf.put_u64(*bytes_sent);
                buf.put_u64(*bytes_received);
                buf.put_u16(*active_channels);
            }
            Self::QueueStatus {
                queued_chunks,
                queued_bytes,
                total_capacity,

            } => {
                buf.put_u32(*queued_chunks);
                buf.put_u32(*queued_bytes);
                buf.put_u32(*total_capacity);
            }
            Self::ChannelOpened { channel_id, port } => {
                buf.put_u16(*channel_id);
                buf.put_u16(*port);
            }
            Self::ChannelClosed { channel_id, code } => {
                buf.put_u16(*channel_id);
                buf.put_u16(*code);
            }
            Self::FileStart {
                filename,
                filesize,
                file_crc32c,
            } => {
                let filename = filename.as_bytes();
                buf.put_u16(filename.len() as u16);
                buf.put_slice(filename);
                buf.put_u64(*filesize);
                buf.put_u32(*file_crc32c);
            }
            Self::FileComplete {
                success,
                expected_hash,
                computed_hash,
            } => {
                buf.put_u8(u8::from(*success));
                buf.put_u32(*expected_hash);
                buf.put_u32(*computed_hash);
            }
            Self::Error { code, detail } => {
                buf.put_u16(*code);
                buf.put_u32(*detail);
            }
        }
        buf.to_vec()
    }
}

impl TryFrom<&[u8]> for ControlMessage {
    type Error = &'static str;

    fn try_from(value: &[u8]) -> Result<Self, <Self as TryFrom<&[u8]>>::Error> {
        if value.is_empty() {
            return Err("empty control message");
        }
        let mut buf = value;
        let kind = ControlMessageKind::try_from(buf.get_u8())?;
        Ok(match kind {
            ControlMessageKind::Hello => Self::Hello {
                protocol_version: buf.get_u16(),
                features: buf.get_u32(),
            },
            ControlMessageKind::Ack => Self::Ack {
                sequence_number: buf.get_u64(),
            },
            ControlMessageKind::Nack => Self::Nack {
                sequence_number: buf.get_u64(),
                reason: buf.get_u16(),
            },
            ControlMessageKind::DataReady => Self::DataReady {
                chunk_id: buf.get_u32(),
                payload_length: buf.get_u16(),
            },
            ControlMessageKind::Eos => Self::Eos {
                sequence_number: buf.get_u64(),
            },
            ControlMessageKind::Reconnect => Self::Reconnect {
                last_sequence_number: buf.get_u64(),
            },
            ControlMessageKind::Stats => Self::Stats {
                bytes_sent: buf.get_u64(),
                bytes_received: buf.get_u64(),
                active_channels: buf.get_u16(),
            },
            ControlMessageKind::QueueStatus => Self::QueueStatus {
                queued_chunks: buf.get_u32(),
                queued_bytes: buf.get_u32(),
                total_capacity: buf.get_u32(),
            },
            ControlMessageKind::ChannelOpened => Self::ChannelOpened {
                channel_id: buf.get_u16(),
                port: buf.get_u16(),
            },
            ControlMessageKind::ChannelClosed => Self::ChannelClosed {
                channel_id: buf.get_u16(),
                code: buf.get_u16(),
            },
            ControlMessageKind::FileStart => {
                let filename_len = buf.get_u16() as usize;
                let mut filename = vec![0; filename_len];
                buf.copy_to_slice(&mut filename);
                Self::FileStart {
                    filename: String::from_utf8(filename).map_err(|_| "invalid utf-8 filename")?,
                    filesize: buf.get_u64(),
                    file_crc32c: buf.get_u32(),
                }
            }
            ControlMessageKind::FileComplete => Self::FileComplete {
                success: buf.get_u8() != 0,
                expected_hash: buf.get_u32(),
                computed_hash: buf.get_u32(),
            },
            ControlMessageKind::Error => Self::Error {
                code: buf.get_u16(),
                detail: buf.get_u32(),
            },
        })
    }
}

impl From<ControlMessage> for Vec<u8> {
    fn from(value: ControlMessage) -> Self {
        value.to_bytes()
    }
}

impl fmt::Display for ControlMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hello {
                protocol_version,
                features,
            } => write!(
                f,
                "HELLO(protocol_version={}, features={})",
                protocol_version, features
            ),
            Self::Ack { sequence_number } => write!(f, "ACK(sequence_number={})", sequence_number),
            Self::Nack {
                sequence_number,
                reason,
            } => write!(
                f,
                "NACK(sequence_number={}, reason={})",
                sequence_number, reason
            ),
            Self::DataReady {
                chunk_id,
                payload_length,
            } => write!(
                f,
                "DATA_READY(chunk_id={}, payload_length={})",
                chunk_id, payload_length
            ),
            Self::Eos { sequence_number } => write!(f, "EOS(sequence_number={})", sequence_number),
            Self::Reconnect {
                last_sequence_number,
            } => write!(
                f,
                "RECONNECT(last_sequence_number={})",
                last_sequence_number
            ),
            Self::Stats {
                bytes_sent,
                bytes_received,
                active_channels,
            } => write!(
                f,
                "STATS(bytes_sent={}, bytes_received={}, active_channels={})",
                bytes_sent, bytes_received, active_channels
            ),
            Self::QueueStatus {
                queued_chunks,
                queued_bytes,
                total_capacity,

            } => write!(
                f,
                "QUEUE_STATUS(chunks={}, bytes={}, capacity={})",
                queued_chunks, queued_bytes, total_capacity
            ),
            Self::ChannelOpened { channel_id, port } => write!(
                f,
                "CHANNEL_OPENED(channel_id={}, port={})",
                channel_id, port
            ),
            Self::ChannelClosed { channel_id, code } => write!(
                f,
                "CHANNEL_CLOSED(channel_id={}, code={})",
                channel_id, code
            ),
            Self::FileStart {
                filename,
                filesize,
                file_crc32c,
            } => write!(
                f,
                "FILE_START(filename={}, filesize={}, file_crc32c={})",
                filename, filesize, file_crc32c
            ),
            Self::FileComplete {
                success,
                expected_hash,
                computed_hash,
            } => write!(
                f,
                "FILE_COMPLETE(success={}, expected_hash={}, computed_hash={})",
                success, expected_hash, computed_hash
            ),
            Self::Error { code, detail } => write!(f, "ERROR(code={}, detail={})", code, detail),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_start_round_trip() {
        let msg = ControlMessage::FileStart {
            filename: "example.bin".to_string(),
            filesize: 123456789,
            file_crc32c: 0xDEADBEEF,
        };
        let bytes = msg.to_bytes();
        let parsed = ControlMessage::try_from(bytes.as_slice()).unwrap();
        assert_eq!(parsed, msg);
        assert_eq!(msg.kind(), ControlMessageKind::FileStart);
        assert_eq!(
            msg.to_string(),
            "FILE_START(filename=example.bin, filesize=123456789, file_crc32c=3735928559)"
        );
    }

    #[test]
    fn test_file_complete_round_trip() {
        let msg = ControlMessage::FileComplete {
            success: true,
            expected_hash: 0x11112222,
            computed_hash: 0x33334444,
        };
        let bytes = msg.to_bytes();
        let parsed = ControlMessage::try_from(bytes.as_slice()).unwrap();
        assert_eq!(parsed, msg);
        assert_eq!(msg.kind(), ControlMessageKind::FileComplete);
        assert_eq!(
            msg.to_string(),
            "FILE_COMPLETE(success=true, expected_hash=286335522, computed_hash=858997828)"
        );
    }

    #[test]
    fn test_file_start_long_filename() {
        let filename = "a".repeat(65535);
        let msg = ControlMessage::FileStart {
            filename: filename.clone(),
            filesize: u64::MAX,
            file_crc32c: u32::MAX,
        };
        let bytes = msg.to_bytes();
        assert_eq!(bytes.len(), 1 + 2 + 65535 + 8 + 4);
        let parsed = ControlMessage::try_from(bytes.as_slice()).unwrap();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_file_start_empty_filename() {
        let msg = ControlMessage::FileStart {
            filename: String::new(),
            filesize: 0,
            file_crc32c: 0,
        };
        let bytes = msg.to_bytes();
        let parsed = ControlMessage::try_from(bytes.as_slice()).unwrap();
        assert_eq!(parsed, msg);
    }
}
