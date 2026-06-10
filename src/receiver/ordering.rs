use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, trace};

use crate::protocol::headers::ChunkHeader;
use crate::receiver::commit::CommitGateInput;

/// Maximum number of chunks allowed in the heap before force-emitting the oldest.
///
/// When a gap (missing sequence number) blocks emission and the output channel
/// has capacity, chunks accumulate in the heap. This threshold ensures bounded
/// heap growth by force-emitting the oldest chunk (breaking order) so the
/// pipeline can drain. 512 chunks at 262KB each ≈ 134MB worst case.
const HEAP_HIGH_WATERMARK: usize = 512;

/// A chunk queued in the min-heap, ordered by sequence number.
#[derive(Debug, Eq, PartialEq)]
struct OrderedChunk {
    /// Sequence number from the chunk header.
    sequence_number: u64,
    /// The chunk payload bytes (ChunkHeader + data) — header is stripped on emit.
    payload: Vec<u8>,
}

impl Ord for OrderedChunk {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sequence_number.cmp(&other.sequence_number)
    }
}

impl PartialOrd for OrderedChunk {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Statistics for the `ChunkOrderer`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ChunkOrdererStats {
    /// Total chunks received (pushed to heap).
    pub chunks_received: u64,
    /// Total chunks emitted in order (sent to output).
    pub chunks_emitted: u64,
    /// Number of chunks currently waiting in the heap.
    pub chunks_waiting: usize,
}

/// Orders complete chunks by sequence number using a min-heap.
///
/// # Architecture
///
/// `ChunkOrderer` receives complete chunk payloads (ChunkHeader + data) from
/// reassembly and emits them in sequence number order. It uses a `BinaryHeap`
/// wrapped with `Reverse` to implement a min-heap keyed by sequence number.
///
/// When a chunk arrives, it is pushed into the heap. The orderer then pops and
/// emits all chunks whose sequence numbers form a consecutive sequence starting
/// from `next_expected_seq`. Gaps are handled naturally: if chunk N arrives but
/// chunk N-1 is missing, chunk N stays in the heap until the gap is filled.
pub struct ChunkOrderer {
    /// Min-heap of chunks ordered by sequence number.
    heap: BinaryHeap<Reverse<OrderedChunk>>,
    /// The next sequence number we expect to emit.
    next_expected_seq: u64,
    /// Channel to emit ordered chunk payloads as CommitGateInput.
    tx: tokio::sync::mpsc::Sender<CommitGateInput>,
    /// Statistics.
    stats: ChunkOrdererStats,
}

impl ChunkOrderer {
    /// Create a new `ChunkOrderer`.
    ///
    /// * `tx` - Channel sender for emitting ordered chunk payloads as CommitGateInput.
    /// * `initial_seq` - The first expected sequence number (typically 0).
    pub fn new(tx: tokio::sync::mpsc::Sender<CommitGateInput>, initial_seq: u64) -> Self {
        Self {
            heap: BinaryHeap::new(),
            next_expected_seq: initial_seq,
            tx,
            stats: ChunkOrdererStats::default(),
        }
    }

    /// Run the ordering loop, consuming chunks from `rx` and emitting ordered
    /// chunks to `tx`.
    ///
    /// This method returns when the input channel is closed (all senders dropped).
    /// Any remaining chunks in the heap are drained and emitted in order.
    ///
    /// If `gap_timeout` is Some, the loop will also check for stale gaps:
    /// if the oldest chunk in the heap has been waiting longer than the timeout,
    /// it is emitted anyway (best-effort) to prevent memory buildup.
    pub async fn run(
        &mut self,
        mut rx: tokio::sync::mpsc::Receiver<Bytes>,
        gap_timeout: Option<Duration>,
    ) {
        let mut ticker = gap_timeout.map(|d| tokio::time::interval(d));
        let mut status_ticker = tokio::time::interval(Duration::from_secs(5));
        status_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let recv_fut = rx.recv();
            tokio::select! {
                _ = status_ticker.tick() => {
                    debug!("ORDERER_STATUS: heap={} next_seq={} emitted={}",
                        self.heap.len(), self.next_expected_seq, self.stats.chunks_emitted);
                }
                payload = recv_fut => {
                    match payload {
                        Some(payload) => {
                            self.push_chunk(payload);
                        }
                        None => {
                            debug!(
                                "input channel closed, draining {} remaining chunks",
                                self.heap.len()
                            );
                            self.drain_all().await;
                            break;
                        }
                    }
                }
                _ = async {
                    if let Some(ref mut ticker) = ticker {
                        ticker.tick().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    if !self.heap.is_empty() {
                        debug!(
                            "gap timeout: {} chunks waiting in heap, oldest seq={}",
                            self.heap.len(),
                            self.heap.peek().map(|r| r.0.sequence_number).unwrap_or(0)
                        );
                        self.force_emit_oldest();
                    }
                }
            }
        }
    }

    pub fn push_chunk(&mut self, payload: Bytes) -> ChunkOrdererStats {
        let seq = parse_sequence_number(&payload);

        self.stats.chunks_received += 1;

        self.heap.push(Reverse(OrderedChunk {
            sequence_number: seq,
            payload: payload.to_vec(),
        }));

        debug!("ORDERER_HEAP: seq={} heap_size={}", seq, self.heap.len());

        trace!(
            "chunk {} pushed to heap (waiting: {})",
            seq,
            self.heap.len()
        );

        self.try_emit();

        // If the heap exceeds the high watermark, force-emit the oldest chunk
        // to prevent unbounded growth from gaps (missing sequence numbers).
        if self.heap.len() > HEAP_HIGH_WATERMARK {
            debug!(
                "heap high watermark reached ({} chunks), force-emitting oldest",
                self.heap.len()
            );
            self.force_emit_oldest();
        }

        self.stats()
    }

    fn try_emit(&mut self) {
        while let Some(peeked) = self.heap.peek() {
            let chunk_seq = peeked.0.sequence_number;
            if chunk_seq != self.next_expected_seq {
                break;
            }

            let (chunk_crc, data) = {
                let payload = &peeked.0.payload;
                let chunk_crc = if payload.len() >= ChunkHeader::LEN {
                    u32::from_be_bytes([
                        payload[12], payload[13], payload[14], payload[15],
                    ])
                } else {
                    0
                };
                let data: Bytes = if payload.len() > ChunkHeader::LEN {
                    Bytes::copy_from_slice(&payload[ChunkHeader::LEN..])
                } else {
                    Bytes::new()
                };
                (chunk_crc, data)
            };

            let input = CommitGateInput {
                data,
                sequence_number: chunk_seq,
                chunk_crc,
            };

            match self.tx.try_send(input) {
                Ok(()) => {
                    self.heap.pop();
                    self.stats.chunks_emitted += 1;
                    self.next_expected_seq = chunk_seq.wrapping_add(1);
                    debug!(
                        "chunk {} emitted (next expected: {}, waiting: {})",
                        chunk_seq,
                        self.next_expected_seq,
                        self.heap.len()
                    );
                }
                Err(TrySendError::Full(_)) => break,
                Err(TrySendError::Closed(_)) => {
                    debug!("output channel closed, stopping emit");
                    break;
                }
            }
        }
    }

    fn force_emit_oldest(&mut self) {
        let (seq, chunk_crc, data) = match self.heap.peek() {
            Some(peeked) => {
                let seq = peeked.0.sequence_number;
                let payload = &peeked.0.payload;
                let chunk_crc = if payload.len() >= ChunkHeader::LEN {
                    u32::from_be_bytes([
                        payload[12], payload[13], payload[14], payload[15],
                    ])
                } else {
                    0
                };
                let data: Bytes = if payload.len() > ChunkHeader::LEN {
                    Bytes::copy_from_slice(&payload[ChunkHeader::LEN..])
                } else {
                    Bytes::new()
                };
                (seq, chunk_crc, data)
            }
            None => return,
        };

        let input = CommitGateInput {
            data,
            sequence_number: seq,
            chunk_crc,
        };

        match self.tx.try_send(input) {
            Ok(()) => {
                self.heap.pop();
                self.stats.chunks_emitted += 1;
                if seq >= self.next_expected_seq {
                    self.next_expected_seq = seq.wrapping_add(1);
                }
                trace!("force-emitted chunk {} (gap timeout)", seq);
                self.try_emit();
            }
            Err(TrySendError::Full(_)) => {
                debug!("force_emit_oldest: channel full, chunk {} stays in heap", seq);
            }
            Err(TrySendError::Closed(_)) => {}
        }
    }

    async fn drain_all(&mut self) {
        let mut chunks: Vec<OrderedChunk> = self.heap.drain().map(|Reverse(c)| c).collect();
        chunks.sort_by_key(|c| c.sequence_number);

        for chunk in chunks {
            let seq = chunk.sequence_number;
            let mut payload = chunk.payload;

            let chunk_crc = if payload.len() >= ChunkHeader::LEN {
                u32::from_be_bytes([payload[12], payload[13], payload[14], payload[15]])
            } else {
                0
            };

            let data: Bytes = if payload.len() > ChunkHeader::LEN {
                let data_len = payload.len() - ChunkHeader::LEN;
                payload.copy_within(ChunkHeader::LEN.., 0);
                unsafe {
                    payload.set_len(data_len);
                }
                Bytes::from(payload)
            } else {
                Bytes::new()
            };

            let input = CommitGateInput {
                data,
                sequence_number: seq,
                chunk_crc,
            };

            // drain_all is only called at shutdown — blocking send is acceptable
            if self.tx.send(input).await.is_err() {
                debug!("output channel closed during drain, stopping");
                break;
            }
            self.stats.chunks_emitted += 1;
            debug!("chunk {} emitted during drain", seq);
        }
    }

    /// Returns a snapshot of the current statistics.
    pub fn stats(&self) -> ChunkOrdererStats {
        ChunkOrdererStats {
            chunks_received: self.stats.chunks_received,
            chunks_emitted: self.stats.chunks_emitted,
            chunks_waiting: self.heap.len(),
        }
    }
}

/// Parse the sequence number from a complete chunk payload (ChunkHeader + data).
///
/// The sequence number is at byte offset 4..12 (u64 big-endian) in the chunk header.
fn parse_sequence_number(payload: &[u8]) -> u64 {
    if payload.len() < 12 {
        return 0;
    }
    u64::from_be_bytes([
        payload[4],
        payload[5],
        payload[6],
        payload[7],
        payload[8],
        payload[9],
        payload[10],
        payload[11],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use tokio::sync::mpsc;

    use crate::protocol::crc::compute_chunk_crc;
    use crate::protocol::headers::ChunkHeader;

    /// Build a complete chunk payload (ChunkHeader + data) with the given
    /// sequence number and data.
    fn build_chunk(sequence_number: u64, data: &[u8]) -> Bytes {
        let chunk_crc = compute_chunk_crc(sequence_number, data);
        let header = ChunkHeader::new(0, data.len() as u16, sequence_number, chunk_crc);
        let mut buf = BytesMut::with_capacity(ChunkHeader::LEN + data.len());
        buf.extend_from_slice(&header.to_bytes());
        buf.extend_from_slice(data);
        Bytes::from(buf.to_vec())
    }

    /// Build chunks with consecutive sequence numbers starting from `start_seq`.
    fn build_chunks(start_seq: u64, count: u64, data_prefix: u8) -> Vec<Bytes> {
        (0..count)
            .map(|i| {
                let seq = start_seq + i;
                let data = vec![data_prefix + i as u8; 16];
                build_chunk(seq, &data)
            })
            .collect()
    }

    #[tokio::test]
    async fn chunks_emitted_in_order() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut orderer = ChunkOrderer::new(tx, 0);

        let chunks = build_chunks(0, 5, 0xAA);
        for chunk in &chunks {
            orderer.push_chunk(chunk.clone());
        }

        let mut emitted = Vec::new();
        for _ in 0..5 {
            emitted.push(rx.recv().await.unwrap());
        }

        assert_eq!(emitted.len(), 5);
        for (i, input) in emitted.iter().enumerate() {
            assert_eq!(input.data, vec![0xAA + i as u8; 16]);
            assert_eq!(input.sequence_number, i as u64);
        }
    }

    #[tokio::test]
    async fn out_of_order_emitted_in_order() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut orderer = ChunkOrderer::new(tx, 0);

        let chunks = build_chunks(0, 5, 0xBB);
        let indices = vec![1usize, 2, 0, 4, 3];

        for &i in &indices {
            orderer.push_chunk(chunks[i].clone());
        }

        let mut emitted = Vec::new();
        for _ in 0..5 {
            emitted.push(rx.recv().await.unwrap());
        }

        assert_eq!(emitted.len(), 5);
        for (i, input) in emitted.iter().enumerate() {
            assert_eq!(input.data, vec![0xBB + i as u8; 16]);
            assert_eq!(input.sequence_number, i as u64);
        }
    }

    #[tokio::test]
    async fn gap_holds_chunks_until_filled() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut orderer = ChunkOrderer::new(tx, 0);

        let chunks = build_chunks(0, 4, 0xCC);

        orderer.push_chunk(chunks[1].clone());
        orderer.push_chunk(chunks[2].clone());
        orderer.push_chunk(chunks[3].clone());

        assert_eq!(orderer.stats().chunks_emitted, 0);
        assert_eq!(orderer.stats().chunks_waiting, 3);

        orderer.push_chunk(chunks[0].clone());

        let stats = orderer.stats();
        assert_eq!(stats.chunks_received, 4);
        assert_eq!(stats.chunks_emitted, 4);
        assert_eq!(stats.chunks_waiting, 0);

        let mut emitted = Vec::new();
        for _ in 0..4 {
            emitted.push(rx.recv().await.unwrap());
        }

        assert_eq!(emitted.len(), 4);
        for (i, input) in emitted.iter().enumerate() {
            assert_eq!(input.data, vec![0xCC + i as u8; 16]);
            assert_eq!(input.sequence_number, i as u64);
        }
    }

    #[tokio::test]
    async fn burst_emission_on_gap_fill() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut orderer = ChunkOrderer::new(tx, 0);

        let chunks = build_chunks(0, 6, 0xDD);

        orderer.push_chunk(chunks[2].clone());
        orderer.push_chunk(chunks[3].clone());
        orderer.push_chunk(chunks[4].clone());
        orderer.push_chunk(chunks[5].clone());

        assert_eq!(orderer.stats().chunks_emitted, 0);
        assert_eq!(orderer.stats().chunks_waiting, 4);

        orderer.push_chunk(chunks[0].clone());

        let stats = orderer.stats();
        assert_eq!(stats.chunks_emitted, 1);
        assert_eq!(stats.chunks_waiting, 4);

        let emitted0 = rx.recv().await.unwrap();
        assert_eq!(emitted0.data, vec![0xDD; 16]);
        assert_eq!(emitted0.sequence_number, 0);

        orderer.push_chunk(chunks[1].clone());

        let stats = orderer.stats();
        assert_eq!(stats.chunks_emitted, 6);
        assert_eq!(stats.chunks_waiting, 0);

        for i in 1..6 {
            let input = rx.recv().await.unwrap();
            assert_eq!(input.data, vec![0xDD + i as u8; 16]);
            assert_eq!(input.sequence_number, i as u64);
        }
    }

    #[tokio::test]
    async fn stats_track_correctly() {
        let (tx, _rx) = mpsc::channel(16);
        let mut orderer = ChunkOrderer::new(tx, 0);

        let chunks = build_chunks(0, 2, 0xEE);

        let stats = orderer.push_chunk(chunks[1].clone());
        assert_eq!(stats.chunks_received, 1);
        assert_eq!(stats.chunks_emitted, 0);
        assert_eq!(stats.chunks_waiting, 1);

        let stats = orderer.push_chunk(chunks[0].clone());
        assert_eq!(stats.chunks_received, 2);
        assert_eq!(stats.chunks_emitted, 2);
        assert_eq!(stats.chunks_waiting, 0);
    }

    #[tokio::test]
    async fn empty_payload_handled() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut orderer = ChunkOrderer::new(tx, 0);

        let chunk = build_chunk(0, b"");
        orderer.push_chunk(chunk);

        let emitted = rx.recv().await.unwrap();
        assert!(emitted.data.is_empty());
        assert_eq!(emitted.sequence_number, 0);
    }

    #[tokio::test]
    async fn non_zero_initial_seq() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut orderer = ChunkOrderer::new(tx, 100);

        let chunks = build_chunks(100, 3, 0x10);
        orderer.push_chunk(chunks[0].clone());
        orderer.push_chunk(chunks[1].clone());
        orderer.push_chunk(chunks[2].clone());

        let mut emitted = Vec::new();
        for _ in 0..3 {
            emitted.push(rx.recv().await.unwrap());
        }

        assert_eq!(emitted.len(), 3);
        for (i, input) in emitted.iter().enumerate() {
            assert_eq!(input.data, vec![0x10 + i as u8; 16]);
            assert_eq!(input.sequence_number, 100 + i as u64);
        }
    }

    #[tokio::test]
    async fn run_loop_processes_all_chunks() {
        let (tx, rx) = mpsc::channel(16);
        let (input_tx, input_rx) = mpsc::channel::<Bytes>(16);

        let mut orderer = ChunkOrderer::new(tx, 0);

        let chunks = build_chunks(0, 3, 0x11);
        for chunk in &chunks {
            input_tx.send(chunk.clone()).await.unwrap();
        }
        drop(input_tx);

        orderer.run(input_rx, None).await;
        drop(orderer);

        let mut emitted = Vec::new();
        let mut recv_rx = rx;
        while let Some(input) = recv_rx.recv().await {
            emitted.push(input);
        }

        assert_eq!(emitted.len(), 3);
        for (i, input) in emitted.iter().enumerate() {
            assert_eq!(input.data, vec![0x11 + i as u8; 16]);
            assert_eq!(input.sequence_number, i as u64);
        }
    }

    #[tokio::test]
    async fn run_loop_handles_out_of_order() {
        let (tx, rx) = mpsc::channel(16);
        let (input_tx, input_rx) = mpsc::channel::<Bytes>(16);

        let mut orderer = ChunkOrderer::new(tx, 0);

        let chunks = build_chunks(0, 4, 0x22);
        input_tx.send(chunks[2].clone()).await.unwrap();
        input_tx.send(chunks[0].clone()).await.unwrap();
        input_tx.send(chunks[1].clone()).await.unwrap();
        input_tx.send(chunks[3].clone()).await.unwrap();
        drop(input_tx);

        orderer.run(input_rx, None).await;
        drop(orderer);

        let mut emitted = Vec::new();
        let mut recv_rx = rx;
        while let Some(input) = recv_rx.recv().await {
            emitted.push(input);
        }

        assert_eq!(emitted.len(), 4);
        for (i, input) in emitted.iter().enumerate() {
            assert_eq!(input.data, vec![0x22 + i as u8; 16]);
            assert_eq!(input.sequence_number, i as u64);
        }
    }

    #[tokio::test]
    async fn run_loop_drains_heap_on_close() {
        let (tx, rx) = mpsc::channel(16);
        let (input_tx, input_rx) = mpsc::channel::<Bytes>(16);

        let mut orderer = ChunkOrderer::new(tx, 0);

        let chunks = build_chunks(0, 3, 0x33);
        input_tx.send(chunks[1].clone()).await.unwrap();
        input_tx.send(chunks[2].clone()).await.unwrap();
        drop(input_tx);

        orderer.run(input_rx, None).await;
        drop(orderer);

        let mut emitted = Vec::new();
        let mut recv_rx = rx;
        while let Some(input) = recv_rx.recv().await {
            emitted.push(input);
        }

        assert_eq!(emitted.len(), 2);
        assert_eq!(emitted[0].data, vec![0x33 + 1; 16]);
        assert_eq!(emitted[0].sequence_number, 1);
        assert_eq!(emitted[1].data, vec![0x33 + 2; 16]);
        assert_eq!(emitted[1].sequence_number, 2);
    }

    /// Verifies that push_chunk does not block when the output channel is full.
    /// With non-blocking try_send, chunks accumulate in the heap until
    /// channel capacity becomes available.
    #[tokio::test]
    async fn non_blocking_send_accumulates_in_heap() {
        let (tx, rx) = mpsc::channel::<CommitGateInput>(1);
        let mut orderer = ChunkOrderer::new(tx, 0);

        let chunks = build_chunks(0, 3, 0x7F);

        orderer.push_chunk(chunks[0].clone());
        let stats = orderer.stats();
        assert_eq!(stats.chunks_emitted, 1, "chunk 0 emitted");
        assert_eq!(stats.chunks_waiting, 0);

        orderer.push_chunk(chunks[1].clone());
        let stats = orderer.stats();
        assert_eq!(stats.chunks_emitted, 1, "chunk 1 blocked by full channel");
        assert_eq!(stats.chunks_waiting, 1, "chunk 1 stays in heap");

        orderer.push_chunk(chunks[2].clone());
        let stats = orderer.stats();
        assert_eq!(stats.chunks_emitted, 1, "chunk 2 also blocked");
        assert_eq!(stats.chunks_waiting, 2, "chunks 1 and 2 in heap");

        drop(rx);
    }
}
