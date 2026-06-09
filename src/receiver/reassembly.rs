use std::collections::{HashMap, HashSet};
use std::time::Instant;

use bytes::Bytes;
use tracing::{debug, warn};

use crate::buffer::pool::BufferPool;
use crate::compress::decompress_lz4;
use crate::protocol::crc::verify_chunk_crc;
use crate::protocol::headers::{ChunkHeader, FragmentHeader, COMPRESSED_LZ4};

/// Tracks the reassembly state for a single chunk.
struct ChunkReassembly {
    /// Total number of fragments expected for this chunk.
    total_fragments: u16,
    /// Indices of fragments already received.
    received: HashSet<u16>,
    /// Fragment payloads stored by index (header stripped).
    /// Indexed by fragment_index.
    fragments: Vec<Option<Bytes>>,
    /// When the first fragment for this chunk was received (for timeout tracking).
    started_at: Instant,
    /// Total bytes of fragment payload data accumulated so far.
    accumulated_bytes: usize,
}

impl ChunkReassembly {
    fn new(total_fragments: u16) -> Self {
        Self {
            total_fragments,
            received: HashSet::with_capacity(total_fragments as usize),
            fragments: (0..total_fragments).map(|_| None).collect(),
            started_at: Instant::now(),
            accumulated_bytes: 0,
        }
    }

    fn is_complete(&self) -> bool {
        self.received.len() == self.total_fragments as usize
    }

    #[allow(dead_code)]
    fn missing_count(&self) -> usize {
        self.total_fragments as usize - self.received.len()
    }
}

/// Reassembles fragments into complete chunks with CRC verification.
pub struct FragmentReassembler {
    chunks: HashMap<u64, ChunkReassembly>,
    tx: tokio::sync::mpsc::Sender<Bytes>,
    max_inflight_bytes: usize,
    inflight_bytes: usize,
    chunk_timeout_ns: u128,
    pool: BufferPool,
}

impl FragmentReassembler {
    pub fn new(
        tx: tokio::sync::mpsc::Sender<Bytes>,
        max_inflight_bytes: usize,
        chunk_timeout_secs: u64,
        pool: BufferPool,
    ) -> Self {
        Self {
            chunks: HashMap::new(),
            tx,
            max_inflight_bytes,
            inflight_bytes: 0,
            chunk_timeout_ns: (chunk_timeout_secs as u128) * 1_000_000_000,
            pool,
        }
    }

    pub async fn add_fragment(&mut self, fragment: Bytes) -> Result<bool, &'static str> {
        if fragment.len() < FragmentHeader::LEN {
            return Err("fragment too short: missing header");
        }

        let header = FragmentHeader::try_from(&fragment[..FragmentHeader::LEN])?;
        let chunk_id = header.chunk_id as u64;
        let payload_offset = FragmentHeader::LEN;

        let is_duplicate = self
            .chunks
            .get(&chunk_id)
            .is_some_and(|s| s.received.contains(&header.fragment_index));

        if is_duplicate {
            debug!(
                "duplicate fragment ignored: chunk_id={}, fragment_index={}",
                header.chunk_id, header.fragment_index
            );
            return Ok(false);
        }

        let state = self
            .chunks
            .entry(chunk_id)
            .or_insert_with(|| ChunkReassembly::new(header.total_fragments));

        if header.total_fragments != state.total_fragments {
            return Err("inconsistent total_fragments across fragments");
        }

        if header.fragment_index >= state.total_fragments {
            return Err("fragment_index out of range");
        }

        let payload_len = fragment.len() - payload_offset;
        state.received.insert(header.fragment_index);
        state.fragments[header.fragment_index as usize] = Some(fragment);
        state.accumulated_bytes += payload_len;
        self.inflight_bytes += payload_len;

        let is_complete = state.is_complete();
        self.enforce_memory_bound();

        if !is_complete {
            return Ok(false);
        }

        self.assemble_chunk(chunk_id).await
    }

    async fn assemble_chunk(&mut self, chunk_id: u64) -> Result<bool, &'static str> {
        let state = self.chunks.get(&chunk_id).ok_or("chunk state not found")?;

        let mut pool_buf = self.pool.acquire().await;
        pool_buf.buffer.clear();
        for fi in 0..state.total_fragments as usize {
            if let Some(ref data) = state.fragments[fi] {
                pool_buf.buffer.extend_from_slice(&data[FragmentHeader::LEN..]);
            }
        }

        self.inflight_bytes = self.inflight_bytes.saturating_sub(state.accumulated_bytes);

        let assembled = &pool_buf.buffer;
        if assembled.len() < ChunkHeader::LEN {
            return Err("reassembled data too short for chunk header");
        }

        let chunk_header = ChunkHeader::try_from(&assembled[..ChunkHeader::LEN])?;
        let wire_data_len = chunk_header.payload_length as usize;
        let seq = chunk_header.sequence_number;
        let crc = chunk_header.chunk_crc;
        let flags = chunk_header.flags;

        if assembled.len() - ChunkHeader::LEN != wire_data_len {
            return Err("chunk payload length mismatch");
        }

        let wire_data = &assembled[ChunkHeader::LEN..];
        let decompressed = if flags == COMPRESSED_LZ4 {
            match decompress_lz4(wire_data) {
                Ok(data) => data,
                Err(_) => return Err("decompression failed"),
            }
        } else {
            wire_data.to_vec()
        };

        let crc_ok = verify_chunk_crc(seq, &decompressed, crc);
        if !crc_ok {
            warn!(
                "chunk CRC mismatch: chunk_id={}, sequence_number={}",
                chunk_id, seq
            );
            return Err("chunk CRC mismatch");
        }

        let mut payload = Vec::with_capacity(ChunkHeader::LEN + decompressed.len());
        payload.extend_from_slice(&assembled[..ChunkHeader::LEN]);
        payload.extend_from_slice(&decompressed);

        match self.tx.send(Bytes::from(payload)).await {
            Ok(()) => {
                debug!(
                    "chunk emitted: chunk_id={}, sequence_number={}, size={}",
                    chunk_id,
                    seq,
                    decompressed.len()
                );
                Ok(true)
            }
            Err(_) => {
                warn!("output channel closed, dropping chunk {}", chunk_id);
                Err("output channel closed")
            }
        }
    }

    fn enforce_memory_bound(&mut self) {
        if self.inflight_bytes <= self.max_inflight_bytes {
            return;
        }

        let oldest_id = {
            let mut oldest: Option<(u64, Instant)> = None;
            for (&id, state) in &self.chunks {
                if state.is_complete() {
                    continue;
                }
                match oldest {
                    None => oldest = Some((id, state.started_at)),
                    Some((_, ts)) if state.started_at < ts => oldest = Some((id, state.started_at)),
                    _ => {}
                }
            }
            oldest.map(|(id, _)| id)
        };

        if let Some(id) = oldest_id {
            if let Some(state) = self.chunks.remove(&id) {
                self.inflight_bytes = self.inflight_bytes.saturating_sub(state.accumulated_bytes);
                warn!(
                    "evicted incomplete chunk {} due to memory pressure (inflight={}, limit={})",
                    id, self.inflight_bytes, self.max_inflight_bytes
                );
            }
        }
    }

    pub fn check_timeouts(&mut self) {
        let now = Instant::now();
        let mut to_remove: Vec<u64> = Vec::new();

        for (&chunk_id, state) in &self.chunks {
            if state.is_complete() {
                continue;
            }
            let elapsed = now.duration_since(state.started_at).as_nanos();
            if elapsed > self.chunk_timeout_ns {
                warn!(
                    "chunk {} incomplete after timeout: {}/{} fragments received",
                    chunk_id,
                    state.received.len(),
                    state.total_fragments
                );
                to_remove.push(chunk_id);
            }
        }

        for id in to_remove {
            if let Some(state) = self.chunks.remove(&id) {
                self.inflight_bytes = self.inflight_bytes.saturating_sub(state.accumulated_bytes);
            }
        }
    }

    pub fn in_flight_count(&self) -> usize {
        self.chunks.len()
    }

    pub fn inflight_bytes(&self) -> usize {
        self.inflight_bytes
    }

    pub fn contains_chunk(&self, chunk_id: u64) -> bool {
        self.chunks.contains_key(&chunk_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    use crate::protocol::crc::{compute_chunk_crc, compute_fragment_crc};
    use crate::protocol::headers::{ChunkHeader, COMPRESSED_LZ4};

    fn build_fragment(
        chunk_id: u32,
        fragment_index: u16,
        total_fragments: u16,
        chunk_header: &ChunkHeader,
        chunk_data: &[u8],
        fragment_payload_size: usize,
    ) -> Bytes {
        let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + chunk_data.len());
        chunk_buf.extend_from_slice(&chunk_header.to_bytes());
        chunk_buf.extend_from_slice(chunk_data);

        let start = fragment_index as usize * fragment_payload_size;
        let end = std::cmp::min(start + fragment_payload_size, chunk_buf.len());
        let fragment_payload = &chunk_buf[start..end];

        let fragment_crc = compute_fragment_crc(fragment_payload);

        let frag_header = FragmentHeader {
            chunk_id,
            fragment_index,
            total_fragments,
            fragment_length: fragment_payload.len() as u16,
            fragment_crc,
        };

        let mut fragment = Vec::with_capacity(FragmentHeader::LEN + fragment_payload.len());
        fragment.extend_from_slice(&frag_header.to_bytes());
        fragment.extend_from_slice(fragment_payload);
        Bytes::from(fragment)
    }

    fn build_fragments(chunk_id: u32, chunk_data: &[u8], mtu: usize) -> Vec<Bytes> {
        let fragment_payload_size = mtu - FragmentHeader::LEN;
        let chunk_crc = compute_chunk_crc(chunk_id as u64, chunk_data);
        let chunk_header = ChunkHeader::new(0, chunk_data.len() as u16, chunk_id as u64, chunk_crc);

        let chunk_buf_len = ChunkHeader::LEN + chunk_data.len();
        let total_fragments = chunk_buf_len.div_ceil(fragment_payload_size) as u16;

        (0..total_fragments)
            .map(|fi| {
                build_fragment(
                    chunk_id,
                    fi,
                    total_fragments,
                    &chunk_header,
                    chunk_data,
                    fragment_payload_size,
                )
            })
            .collect()
    }

    fn make_pool() -> BufferPool {
        BufferPool::new(4, 65536)
    }

    fn strip_header(output: Bytes) -> Vec<u8> {
        if output.len() > ChunkHeader::LEN {
            output[ChunkHeader::LEN..].to_vec()
        } else {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn reassemble_in_order() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, make_pool());

        let chunk_data = b"hello braid reassembly test";
        let fragments = build_fragments(0, chunk_data, 1500);

        for fragment in fragments {
            let completed = reassembler.add_fragment(fragment).await.unwrap();
            if completed {
                break;
            }
        }

        let output = rx.recv().await;
        assert!(output.is_some());
        assert_eq!(strip_header(output.unwrap()), chunk_data);
    }

    #[tokio::test]
    async fn reassemble_out_of_order() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, make_pool());

        let mtu = 50;
        let chunk_data: Vec<u8> = (0..200u8).collect();
        let mut fragments = build_fragments(0, &chunk_data, mtu);

        fragments.reverse();

        for fragment in fragments {
            let completed = reassembler.add_fragment(fragment).await.unwrap();
            if completed {
                break;
            }
        }

        let output = rx.recv().await;
        assert!(output.is_some());
        assert_eq!(strip_header(output.unwrap()), chunk_data);
    }

    #[tokio::test]
    async fn duplicate_fragment_ignored() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, make_pool());

        let mtu = 50;
        let chunk_data: Vec<u8> = (0..100u8).collect();
        let fragments = build_fragments(0, &chunk_data, mtu);

        let result1 = reassembler
            .add_fragment(fragments[0].clone())
            .await
            .unwrap();
        assert!(!result1, "first fragment should not complete chunk");

        let result2 = reassembler
            .add_fragment(fragments[0].clone())
            .await
            .unwrap();
        assert!(!result2, "duplicate should not complete chunk");

        for fragment in &fragments[1..] {
            let completed = reassembler.add_fragment(fragment.clone()).await.unwrap();
            if completed {
                break;
            }
        }

        let output = rx.recv().await;
        assert!(output.is_some());
        assert_eq!(strip_header(output.unwrap()), chunk_data);
    }

    #[tokio::test]
    async fn crc_mismatch_detected() {
        let (tx, _rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, make_pool());

        let chunk_data = b"crc test data";
        let mut fragments = build_fragments(0, chunk_data, 1500);

        if fragments.len() > 1 {
            let frag = fragments[1].to_vec();
            let payload_start = FragmentHeader::LEN;
            let mut corrupted = frag;
            if corrupted.len() > payload_start {
                corrupted[payload_start] ^= 0xFF;
            }
            fragments[1] = Bytes::from(corrupted);
        } else {
            let frag = fragments[0].to_vec();
            let payload_start = FragmentHeader::LEN;
            let mut corrupted = frag;
            if corrupted.len() > payload_start {
                corrupted[payload_start] ^= 0xFF;
            }
            fragments[0] = Bytes::from(corrupted);
        }

        for frag in fragments {
            let _ = reassembler.add_fragment(frag).await;
        }
        let result = reassembler.assemble_chunk(0).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid chunk magic");
    }

    #[tokio::test]
    async fn chunk_crc_mismatch_detected() {
        let (tx, _rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, make_pool());

        let chunk_data = b"chunk crc test";
        let fragment_payload_size = 1500 - FragmentHeader::LEN;
        let wrong_crc = 0xDEADBEEF;
        let chunk_header = ChunkHeader::new(0, chunk_data.len() as u16, 0, wrong_crc);

        let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + chunk_data.len());
        chunk_buf.extend_from_slice(&chunk_header.to_bytes());
        chunk_buf.extend_from_slice(chunk_data);

        let total_fragments = chunk_buf.len().div_ceil(fragment_payload_size) as u16;

        let fragments: Vec<Bytes> = (0..total_fragments)
            .map(|fi| {
                let start = fi as usize * fragment_payload_size;
                let end = std::cmp::min(start + fragment_payload_size, chunk_buf.len());
                let fp = &chunk_buf[start..end];
                let fcrc = compute_fragment_crc(fp);
                let fh = FragmentHeader {
                    chunk_id: 0,
                    fragment_index: fi,
                    total_fragments,
                    fragment_length: fp.len() as u16,
                    fragment_crc: fcrc,
                };
                let mut frag = Vec::with_capacity(FragmentHeader::LEN + fp.len());
                frag.extend_from_slice(&fh.to_bytes());
                frag.extend_from_slice(fp);
                Bytes::from(frag)
            })
            .collect();

        for fragment in &fragments {
            let result = reassembler.add_fragment(fragment.clone()).await;
            if let Err(msg) = result {
                assert_eq!(msg, "chunk CRC mismatch");
                return;
            }
        }

        panic!("expected chunk CRC mismatch error");
    }

    #[tokio::test]
    async fn multiple_chunks_reassembled() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, make_pool());

        let data1 = b"chunk one data";
        let data2 = b"chunk two data";

        let frags1 = build_fragments(0, data1, 1500);
        let frags2 = build_fragments(1, data2, 1500);

        let mut all_frags: Vec<Bytes> = Vec::new();
        let max_len = frags1.len().max(frags2.len());
        for i in 0..max_len {
            if i < frags1.len() {
                all_frags.push(frags1[i].clone());
            }
            if i < frags2.len() {
                all_frags.push(frags2[i].clone());
            }
        }

        let _emitted_count = Arc::new(AtomicUsize::new(0));

        for fragment in all_frags {
            let _ = reassembler.add_fragment(fragment).await;
        }

        let mut outputs = Vec::new();
        while let Ok(Some(data)) =
            tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await
        {
            outputs.push(data);
            if outputs.len() >= 2 {
                break;
            }
        }

        assert_eq!(outputs.len(), 2);
        assert_eq!(strip_header(outputs[0].clone()), data1);
        assert_eq!(strip_header(outputs[1].clone()), data2);
    }

    #[tokio::test]
    async fn memory_bound_evicts_oldest() {
        let (tx, _rx) = mpsc::channel(16);
let mut reassembler = FragmentReassembler::new(tx, 50, 60, make_pool());



        let data0: Vec<u8> = (0..100u8).collect();
        let frags0 = build_fragments(0, &data0, 50);

        let data1: Vec<u8> = (100..200u8).collect();
        let frags1 = build_fragments(1, &data1, 50);

        let _ = reassembler.add_fragment(frags0[0].clone()).await;
        let _ = reassembler.add_fragment(frags1[0].clone()).await;

        assert!(!reassembler.contains_chunk(0));
        assert!(reassembler.contains_chunk(1));
    }

    #[tokio::test]
    async fn fragment_too_short_rejected() {
        let (tx, _rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, make_pool());

        let result = reassembler.add_fragment(Bytes::from(vec![0u8; 5])).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "fragment too short: missing header");
    }

    #[tokio::test]
    async fn timeout_check_logs_warning() {
        let (tx, _rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 0, make_pool());

        let data: Vec<u8> = (0..200u8).collect();
        let frags = build_fragments(0, &data, 50);

        let _ = reassembler.add_fragment(frags[0].clone()).await;

        assert_eq!(reassembler.in_flight_count(), 1);

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        reassembler.check_timeouts();

        assert_eq!(reassembler.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn test_blocking_send_propagates_backpressure() {
        let (tx, rx) = mpsc::channel::<Bytes>(1);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, make_pool());

        let chunk_data = b"backpressure test data";
        let fragments = build_fragments(0, chunk_data, 1500);
        assert_eq!(fragments.len(), 1, "test requires single-fragment chunks");

        let result = reassembler.add_fragment(fragments[0].clone()).await;
        assert!(result.is_ok(), "first chunk should assemble and send");
        assert!(result.unwrap(), "first chunk should be emitted");

        let fragments2 = build_fragments(1, chunk_data, 1500);

        let blocked = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            reassembler.add_fragment(fragments2[0].clone()),
        )
        .await;

        assert!(
            blocked.is_err(),
            "add_fragment should block when output channel is full"
        );

        drop(rx);
    }

    fn build_compressed_fragments(
        chunk_id: u32,
        data: &[u8],
        mtu: usize,
    ) -> (Vec<Bytes>, Vec<u8>) {
        let compressed = crate::compress::lz4::compress_lz4(data).expect("compress should succeed");
        let chunk_crc = compute_chunk_crc(chunk_id as u64, data);
        let chunk_header = ChunkHeader::new(
            COMPRESSED_LZ4,
            compressed.len() as u16,
            chunk_id as u64,
            chunk_crc,
        );

        let fragment_payload_size = mtu - FragmentHeader::LEN;
        let mut chunk_buf = Vec::with_capacity(ChunkHeader::LEN + compressed.len());
        chunk_buf.extend_from_slice(&chunk_header.to_bytes());
        chunk_buf.extend_from_slice(&compressed);

        let total_fragments = chunk_buf.len().div_ceil(fragment_payload_size) as u16;

        let fragments: Vec<Bytes> = (0..total_fragments)
            .map(|fi| {
                let start = fi as usize * fragment_payload_size;
                let end = std::cmp::min(start + fragment_payload_size, chunk_buf.len());
                let fp = &chunk_buf[start..end];
                let fcrc = compute_fragment_crc(fp);
                let fh = FragmentHeader {
                    chunk_id,
                    fragment_index: fi,
                    total_fragments,
                    fragment_length: fp.len() as u16,
                    fragment_crc: fcrc,
                };
                let mut frag = Vec::with_capacity(FragmentHeader::LEN + fp.len());
                frag.extend_from_slice(&fh.to_bytes());
                frag.extend_from_slice(fp);
                Bytes::from(frag)
            })
            .collect();

        (fragments, data.to_vec())
    }

    #[tokio::test]
    async fn compressed_chunk_decompresses_correctly() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, make_pool());

        let data = b"Hello BRAID! This data should be compressed and decompressed.";
        let (fragments, original) = build_compressed_fragments(0, data, 1500);

        for fragment in fragments {
            let completed = reassembler.add_fragment(Bytes::from(fragment)).await.unwrap();
            if completed {
                break;
            }
        }

        let output = rx.recv().await;
        assert!(output.is_some());
        assert_eq!(strip_header(output.unwrap()), original);
    }

    #[tokio::test]
    async fn uncompressed_chunk_passes_through() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, make_pool());

        let data = b"uncompressed data should pass through unchanged";
        let fragments = build_fragments(0, data, 1500);

        for fragment in fragments {
            let completed = reassembler.add_fragment(Bytes::from(fragment)).await.unwrap();
            if completed {
                break;
            }
        }

        let output = rx.recv().await;
        assert!(output.is_some());
        assert_eq!(strip_header(output.unwrap()), data);
    }

    #[tokio::test]
    async fn corrupted_compressed_data_returns_error() {
        let (tx, _rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, make_pool());

        // Build compressed fragments, then corrupt the compressed data directly
        // by overwriting the size-prepended header that lz4_flex uses.
        // lz4_flex::decompress_size_prepended reads the first 5 bytes as the
        // uncompressed size — corrupting those causes a decompression error.
        let data = b"this data will be corrupted after compression";
        let (mut fragments, _) = build_compressed_fragments(0, data, 1500);

        if !fragments.is_empty() {
            let mut frag = fragments[0].to_vec();
            // Compressed data starts after FragmentHeader + ChunkHeader
            let compressed_start = FragmentHeader::LEN + ChunkHeader::LEN;
            // Replace the compressed data with garbage to force decompression failure
            if frag.len() > compressed_start + 16 {
                let garbage = [0xFFu8; 16];
                frag[compressed_start..compressed_start + 16].copy_from_slice(&garbage);
            }
            fragments[0] = Bytes::from(frag);
        }

        for fragment in fragments {
            let result = reassembler.add_fragment(Bytes::from(fragment)).await;
            if let Err(msg) = result {
                assert_eq!(msg, "decompression failed");
                return;
            }
        }

        panic!("expected decompression failure error");
    }
}
