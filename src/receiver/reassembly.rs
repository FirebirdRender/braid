use std::collections::{HashMap, HashSet};
use std::time::Instant;

use tracing::{debug, warn};

use crate::protocol::crc::verify_chunk_crc;
use crate::protocol::headers::{ChunkHeader, FragmentHeader};

/// Tracks the reassembly state for a single chunk.
struct ChunkReassembly {
    /// Total number of fragments expected for this chunk.
    total_fragments: u16,
    /// Indices of fragments already received.
    received: HashSet<u16>,
    /// Fragment payloads stored by index (header stripped).
    /// Indexed by fragment_index.
    fragments: Vec<Option<Vec<u8>>>,
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

    /// Returns true if all fragments have been received.
    fn is_complete(&self) -> bool {
        self.received.len() == self.total_fragments as usize
    }

    /// Returns the number of fragments still missing.
    #[allow(dead_code)]
    fn missing_count(&self) -> usize {
        self.total_fragments as usize - self.received.len()
    }
}

/// Reassembles fragments into complete chunks with CRC verification.
///
/// # Architecture
///
/// `FragmentReassembler` receives raw fragment datagrams (FragmentHeader + payload),
/// tracks per-chunk state in a `HashMap`, and emits complete chunk payloads
/// through an mpsc channel when all fragments for a chunk arrive and CRCs verify.
///
/// ## Memory management
///
/// A configurable `max_inflight_bytes` limit bounds total fragment payload data
/// held across all in-flight chunks. When exceeded, the oldest incomplete chunk
/// is evicted (logged as an error).
pub struct FragmentReassembler {
    /// Per-chunk reassembly state.
    chunks: HashMap<u64, ChunkReassembly>,
    /// Channel to emit complete chunk payloads.
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    /// Maximum total fragment payload bytes held across all in-flight chunks.
    max_inflight_bytes: usize,
    /// Current total fragment payload bytes held.
    inflight_bytes: usize,
    /// Chunk timeout duration (nanos). Chunks older than this are warned.
    chunk_timeout_ns: u128,
}

impl FragmentReassembler {
    /// Create a new `FragmentReassembler`.
    ///
    /// * `tx` - Channel sender for emitting complete chunk payloads.
    /// * `max_inflight_bytes` - Maximum total fragment payload data to buffer.
    /// * `chunk_timeout_secs` - Seconds after which an incomplete chunk logs a warning.
    pub fn new(
        tx: tokio::sync::mpsc::Sender<Vec<u8>>,
        max_inflight_bytes: usize,
        chunk_timeout_secs: u64,
    ) -> Self {
        Self {
            chunks: HashMap::new(),
            tx,
            max_inflight_bytes,
            inflight_bytes: 0,
            chunk_timeout_ns: (chunk_timeout_secs as u128) * 1_000_000_000,
        }
    }

    /// Process an incoming fragment datagram.
    ///
    /// The fragment is a `Vec<u8>` containing:
    /// - `FragmentHeader` (14 bytes)
    /// - Fragment payload (ChunkHeader + chunk data bytes)
    ///
    /// Fragment CRC is expected to have been verified by the caller (the UDP
    /// receive worker) before this method is called, distributing CRC
    /// computation across worker cores instead of bottlenecking here.
    ///
    /// Returns `Ok(true)` if the fragment completed a chunk (emitted to channel),
    /// `Ok(false)` if the chunk is still incomplete, and `Err` on parse failure.
    pub async fn add_fragment(&mut self, fragment: Vec<u8>) -> Result<bool, &'static str> {
        if fragment.len() < FragmentHeader::LEN {
            return Err("fragment too short: missing header");
        }

        // Parse the fragment header
        let header = FragmentHeader::try_from(&fragment[..FragmentHeader::LEN])?;
        let chunk_id = header.chunk_id as u64;
        let payload_offset = FragmentHeader::LEN;

        // Check for duplicate fragment before any state mutation
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

        // Get or create reassembly state for this chunk
        let state = self
            .chunks
            .entry(chunk_id)
            .or_insert_with(|| ChunkReassembly::new(header.total_fragments));

        // Validate total_fragments consistency
        if header.total_fragments != state.total_fragments {
            return Err("inconsistent total_fragments across fragments");
        }

        // Validate fragment_index is in range
        if header.fragment_index >= state.total_fragments {
            return Err("fragment_index out of range");
        }

        // Store the full fragment Vec — we keep the header bytes but only
        // count the payload portion toward accumulated/inflight tracking.
        // At assembly time we slice from FragmentHeader::LEN to skip headers.
        let payload_len = fragment.len() - payload_offset;
        state.received.insert(header.fragment_index);
        state.fragments[header.fragment_index as usize] = Some(fragment);
        state.accumulated_bytes += payload_len;
        self.inflight_bytes += payload_len;

        // Check if chunk is complete — capture needed info before dropping borrow
        let is_complete = state.is_complete();

        // Enforce memory bound: evict oldest incomplete chunk if over limit
        // (done after the mutable borrow on state is released)
        self.enforce_memory_bound();

        if !is_complete {
            return Ok(false);
        }

        // All fragments received — assemble and verify
        self.assemble_chunk(chunk_id).await
    }

    /// Assemble a complete chunk from its fragments and verify chunk CRC.
    ///
    /// Returns `Ok(true)` if the chunk was emitted, `Err` if CRC verification failed.
    async fn assemble_chunk(&mut self, chunk_id: u64) -> Result<bool, &'static str> {
        let state = self.chunks.get(&chunk_id).ok_or("chunk state not found")?;

        // Build the reassembled fragment payload (ChunkHeader + chunk data).
        // Each stored fragment starts with FragmentHeader (14 bytes), which we skip.
        let mut reassembled = Vec::with_capacity(state.accumulated_bytes);
        for fi in 0..state.total_fragments as usize {
            if let Some(ref data) = state.fragments[fi] {
                reassembled.extend_from_slice(&data[FragmentHeader::LEN..]);
            }
        }

        // Subtract inflight bytes for this chunk
        self.inflight_bytes = self.inflight_bytes.saturating_sub(state.accumulated_bytes);

        // Parse the chunk header from the reassembled data
        if reassembled.len() < ChunkHeader::LEN {
            return Err("reassembled data too short for chunk header");
        }

        let chunk_header = ChunkHeader::try_from(&reassembled[..ChunkHeader::LEN])?;
        let chunk_data_len = chunk_header.payload_length as usize;
        let seq = chunk_header.sequence_number;
        let crc = chunk_header.chunk_crc;

        // Verify chunk data length matches header
        if reassembled.len() - ChunkHeader::LEN != chunk_data_len {
            return Err("chunk payload length mismatch");
        }

        // Verify chunk CRC
        let crc_ok = {
            let chunk_data = &reassembled[ChunkHeader::LEN..];
            verify_chunk_crc(seq, chunk_data, crc)
        };
        if !crc_ok {
            warn!(
                "chunk CRC mismatch: chunk_id={}, sequence_number={}",
                chunk_id, seq
            );
            return Err("chunk CRC mismatch");
        }

        // Emit the complete chunk payload (ChunkHeader + data) for the orderer
        // which needs the header to parse sequence number and CRC.
        let payload = reassembled;

        // Try to send — if channel is closed, log and continue
        match self.tx.send(payload).await {
            Ok(()) => {
                debug!(
                    "chunk emitted: chunk_id={}, sequence_number={}, size={}",
                    chunk_id, seq, chunk_data_len
                );
                Ok(true)
            }
            Err(_) => {
                warn!("output channel closed, dropping chunk {}", chunk_id);
                Err("output channel closed")
            }
        }
    }

    /// Enforce the inflight memory bound by evicting the oldest incomplete chunk.
    fn enforce_memory_bound(&mut self) {
        if self.inflight_bytes <= self.max_inflight_bytes {
            return;
        }

        // Find the oldest incomplete chunk
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

    /// Check for timed-out incomplete chunks and log warnings.
    ///
    /// Should be called periodically (e.g., from a timer task).
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

    /// Returns the number of chunks currently being reassembled.
    pub fn in_flight_count(&self) -> usize {
        self.chunks.len()
    }

    /// Returns the current total inflight fragment payload bytes.
    pub fn inflight_bytes(&self) -> usize {
        self.inflight_bytes
    }

    /// Returns whether a chunk with the given ID is currently being reassembled.
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
    use crate::protocol::headers::ChunkHeader;

    /// Build a single fragment for a given chunk.
    fn build_fragment(
        chunk_id: u32,
        fragment_index: u16,
        total_fragments: u16,
        chunk_header: &ChunkHeader,
        chunk_data: &[u8],
        fragment_payload_size: usize,
    ) -> Vec<u8> {
        // Build the full chunk buffer
        let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + chunk_data.len());
        chunk_buf.extend_from_slice(&chunk_header.to_bytes());
        chunk_buf.extend_from_slice(chunk_data);

        // Extract this fragment's portion
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
        fragment
    }

    /// Build all fragments for a chunk.
    fn build_fragments(chunk_id: u32, chunk_data: &[u8], mtu: usize) -> Vec<Vec<u8>> {
        let fragment_payload_size = mtu - FragmentHeader::LEN;
        let chunk_crc = compute_chunk_crc(chunk_id as u64, chunk_data);
        let chunk_header = ChunkHeader::new(0, chunk_data.len() as u16, chunk_id as u64, chunk_crc);

        let chunk_buf_len = ChunkHeader::LEN + chunk_data.len();
        let total_fragments =
            ((chunk_buf_len + fragment_payload_size - 1) / fragment_payload_size) as u16;

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

    /// Helper: strip the ChunkHeader from a reassembler output to get just the data.
    fn strip_header(output: Vec<u8>) -> Vec<u8> {
        if output.len() > ChunkHeader::LEN {
            output[ChunkHeader::LEN..].to_vec()
        } else {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn reassemble_in_order() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60);

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
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60);

        // Use small MTU to force multiple fragments
        let mtu = 50;
        let chunk_data: Vec<u8> = (0..200u8).collect();
        let mut fragments = build_fragments(0, &chunk_data, mtu);

        // Reverse the fragments (out of order)
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
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60);

        // Use small MTU to force multiple fragments
        let mtu = 50;
        let chunk_data: Vec<u8> = (0..100u8).collect();
        let fragments = build_fragments(0, &chunk_data, mtu);

        // Send first fragment twice
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

        // Send remaining fragments
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
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60);

        let chunk_data = b"crc test data";
        let mut fragments = build_fragments(0, chunk_data, 1500);

        if fragments.len() > 1 {
            let frag = &mut fragments[1];
            let payload_start = FragmentHeader::LEN;
            if frag.len() > payload_start {
                frag[payload_start] ^= 0xFF;
            }
        } else {
            let frag = &mut fragments[0];
            let payload_start = FragmentHeader::LEN;
            if frag.len() > payload_start {
                frag[payload_start] ^= 0xFF;
            }
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
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60);

        // Build fragments with a wrong chunk CRC
        let chunk_data = b"chunk crc test";
        let fragment_payload_size = 1500 - FragmentHeader::LEN;
        let wrong_crc = 0xDEADBEEF;
        let chunk_header = ChunkHeader::new(0, chunk_data.len() as u16, 0, wrong_crc);

        let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + chunk_data.len());
        chunk_buf.extend_from_slice(&chunk_header.to_bytes());
        chunk_buf.extend_from_slice(chunk_data);

        let total_fragments =
            ((chunk_buf.len() + fragment_payload_size - 1) / fragment_payload_size) as u16;

        let fragments: Vec<Vec<u8>> = (0..total_fragments)
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
                frag
            })
            .collect();

        // All fragments should be accepted (fragment CRCs are valid)
        for fragment in &fragments {
            let result = reassembler.add_fragment(fragment.clone()).await;
            // The last fragment should trigger assembly and fail on chunk CRC
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
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60);

        let data1 = b"chunk one data";
        let data2 = b"chunk two data";

        let frags1 = build_fragments(0, data1, 1500);
        let frags2 = build_fragments(1, data2, 1500);

        // Interleave fragments from both chunks
        let mut all_frags: Vec<Vec<u8>> = Vec::new();
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

        // Collect all emitted chunks
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
        let mut reassembler = FragmentReassembler::new(tx, 50, 60);

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
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60);

        let result = reassembler.add_fragment(vec![0u8; 5]).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "fragment too short: missing header");
    }

    #[tokio::test]
    async fn timeout_check_logs_warning() {
        let (tx, _rx) = mpsc::channel(16);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 0);

        // Use data large enough to require multiple fragments
        let data: Vec<u8> = (0..200u8).collect();
        let frags = build_fragments(0, &data, 50);

        let _ = reassembler.add_fragment(frags[0].clone()).await;

        assert_eq!(reassembler.in_flight_count(), 1);

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        reassembler.check_timeouts();

        assert_eq!(reassembler.in_flight_count(), 0);
    }

    /// Verifies that a reassembler with a small output channel properly
    /// blocks when the channel is full, without losing any data.
    ///
    /// This test creates a reassembler with a channel of capacity 1,
    /// assembles fragments into chunks until the channel fills, and
    /// verifies the reassembler blocks (backpressure) rather than
    /// dropping data.
    #[tokio::test]
    async fn test_blocking_send_propagates_backpressure() {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(1);
        let mut reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60);

        // Build single-fragment chunks
        let chunk_data = b"backpressure test data";
        let fragments = build_fragments(0, chunk_data, 1500);
        assert_eq!(fragments.len(), 1, "test requires single-fragment chunks");

        // First fragment completes the chunk and sends to channel (has capacity)
        let result = reassembler.add_fragment(fragments[0].clone()).await;
        assert!(result.is_ok(), "first chunk should assemble and send");
        assert!(result.unwrap(), "first chunk should be emitted");

        // Build second chunk that must block when channel is full
        let fragments2 = build_fragments(1, chunk_data, 1500);

        // Second fragment should block because output channel is full (rx not consumed)
        let blocked = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            reassembler.add_fragment(fragments2[0].clone()),
        )
        .await;

        assert!(
            blocked.is_err(),
            "add_fragment should block when output channel is full"
        );

        // Cleanup: drop rx to unblock any pending send, then drop reassembler
        drop(rx);
    }
}
