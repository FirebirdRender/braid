use std::convert::TryFrom;
use std::fmt;

use bytes::{Buf, BufMut, BytesMut};

pub const COMPRESSION_NONE: u8 = 0x00;
pub const COMPRESSED_LZ4: u8 = 0x01;
pub const COMPRESSED_ZSTD: u8 = 0x02;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentHeader {
    pub chunk_id: u32,
    pub fragment_index: u16,
    pub total_fragments: u16,
    pub fragment_length: u16,
    pub fragment_crc: u32,
}

impl FragmentHeader {
    pub const LEN: usize = 14;

    pub fn to_bytes(self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(Self::LEN);
        self.write_to(&mut buf);
        buf.to_vec()
    }

    /// Write header fields directly into any `BufMut` writer — zero-allocation alternative to `to_bytes()`.
    pub fn write_to(self, buf: &mut impl BufMut) {
        buf.put_u32(self.chunk_id);
        buf.put_u16(self.fragment_index);
        buf.put_u16(self.total_fragments);
        buf.put_u16(self.fragment_length);
        buf.put_u32(self.fragment_crc);
    }
}

impl TryFrom<&[u8]> for FragmentHeader {
    type Error = &'static str;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != Self::LEN {
            return Err("invalid fragment header length");
        }
        let mut buf = value;
        Ok(Self {
            chunk_id: buf.get_u32(),
            fragment_index: buf.get_u16(),
            total_fragments: buf.get_u16(),
            fragment_length: buf.get_u16(),
            fragment_crc: buf.get_u32(),
        })
    }
}

impl From<FragmentHeader> for Vec<u8> {
    fn from(value: FragmentHeader) -> Self {
        value.to_bytes()
    }
}

impl fmt::Display for FragmentHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FragmentHeader(chunk_id={}, fragment_index={}, total_fragments={}, fragment_length={}, fragment_crc={})", self.chunk_id, self.fragment_index, self.total_fragments, self.fragment_length, self.fragment_crc)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkHeader {
    pub magic: u8,
    pub flags: u8,
    pub payload_length: u16,
    pub sequence_number: u64,
    pub chunk_crc: u32,
}

impl ChunkHeader {
    pub const LEN: usize = 16;
    pub const MAGIC: u8 = 0x50;

    pub fn new(flags: u8, payload_length: u16, sequence_number: u64, chunk_crc: u32) -> Self {
        Self {
            magic: Self::MAGIC,
            flags,
            payload_length,
            sequence_number,
            chunk_crc,
        }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(Self::LEN);
        self.write_to(&mut buf);
        buf.to_vec()
    }

    /// Write header fields directly into a `BufMut` — zero-allocation alternative to `to_bytes()`.
    pub fn write_to(self, buf: &mut impl BufMut) {
        buf.put_u8(self.magic);
        buf.put_u8(self.flags);
        buf.put_u16(self.payload_length);
        buf.put_u64(self.sequence_number);
        buf.put_u32(self.chunk_crc);
    }
}

impl TryFrom<&[u8]> for ChunkHeader {
    type Error = &'static str;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != Self::LEN {
            return Err("invalid chunk header length");
        }
        let mut buf = value;
        let magic = buf.get_u8();
        if magic != Self::MAGIC {
            return Err("invalid chunk magic");
        }
        Ok(Self {
            magic,
            flags: buf.get_u8(),
            payload_length: buf.get_u16(),
            sequence_number: buf.get_u64(),
            chunk_crc: buf.get_u32(),
        })
    }
}

impl From<ChunkHeader> for Vec<u8> {
    fn from(value: ChunkHeader) -> Self {
        value.to_bytes()
    }
}

impl fmt::Display for ChunkHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkHeader(magic=0x{:02X}, flags={}, payload_length={}, sequence_number={}, chunk_crc={})", self.magic, self.flags, self.payload_length, self.sequence_number, self.chunk_crc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_constants_have_correct_values() {
        assert_eq!(COMPRESSION_NONE, 0x00);
        assert_eq!(COMPRESSED_LZ4, 0x01);
        assert_eq!(COMPRESSED_ZSTD, 0x02);
    }

    #[test]
    fn compression_constants_are_mutually_exclusive() {
        assert_eq!(COMPRESSION_NONE & COMPRESSED_LZ4, 0);
        assert_eq!(COMPRESSION_NONE & COMPRESSED_ZSTD, 0);
        assert_eq!(COMPRESSED_LZ4 & COMPRESSED_ZSTD, 0);
    }
}
