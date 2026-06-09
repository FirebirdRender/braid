use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Duration;

use bytes::Bytes;
use tracing::{debug, trace};

use crate::protocol::headers::ChunkHeader;
use crate::receiver::commit::CommitGateInput;

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
        loop {
            let recv_fut = rx.recv();
            tokio::select! {
                payload = recv_fut => {
                    match payload {
                        Some(payload) => {
                            self.push_chunk(payload).await;
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
                    // Gap timeout tick — check if we should force-emit the oldest chunk
                    if !self.heap.is_empty() {
                        debug!(
                            "gap timeout: {} chunks waiting in heap, oldest seq={}",
                            self.heap.len(),
                            self.heap.peek().map(|r| r.0.sequence_number).unwrap_or(0)
                        );
                        // Force-emit the oldest chunk to prevent unbounded memory
                        self.force_emit_oldest().await;
                    }
                }
            }
        }
    }

    async fn force_emit_oldest(&mut self) {
        if let Some(Reverse(chunk)) = self.heap.pop() {
            let seq = chunk.sequence_number;
            let payload = chunk.payload;
            trace!("force-emitting chunk {} (gap timeout)", seq);
            Self::emit_chunk(&self.tx, seq, payload, &mut self.stats).await;
            if seq >= self.next_expected_seq {
                self.next_expected_seq = seq.wrapping_add(1);
            }
            self.try_emit().await;
        }
    }

    fn prepare_input(seq: u64, mut payload: Vec<u8>) -> CommitGateInput {
        let chunk_crc = if payload.len() >= ChunkHeader::LEN {
            u32::from_be_bytes([payload[12], payload[13], payload[14], payload[15]])
        } else {
            0
        };

        let data: Bytes = if payload.len() > ChunkHeader::LEN {
            let data_len = payload.len() - ChunkHeader::LEN;
            payload.copy_within(ChunkHeader::LEN.., 0);
            // SAFETY: data_len < original len, and we just copied those bytes to [0..data_len)
            unsafe {
                payload.set_len(data_len);
            }
            Bytes::from(payload)
        } else {
            Bytes::new()
        };

        CommitGateInput {
            data,
            sequence_number: seq,
            chunk_crc,
        }
    }

    async fn emit_chunk(
        tx: &tokio::sync::mpsc::Sender<CommitGateInput>,
        seq: u64,
        payload: Vec<u8>,
        stats: &mut ChunkOrdererStats,
    ) -> bool {
        let input = Self::prepare_input(seq, payload);

        match tx.send(input).await {
            Ok(()) => {
                stats.chunks_emitted += 1;
                true
            }
            Err(tokio::sync::mpsc::error::SendError(_)) => {
                debug!("output channel closed, dropping chunk {}", seq);
                false
            }
        }
    }

    pub async fn push_chunk(&mut self, payload: Bytes) -> ChunkOrdererStats {
        let seq = parse_sequence_number(&payload);

        self.stats.chunks_received += 1;

        self.heap.push(Reverse(OrderedChunk {
            sequence_number: seq,
            payload: payload.to_vec(),
        }));

        trace!(
            "chunk {} pushed to heap (waiting: {})",
            seq,
            self.heap.len()
        );

        self.try_emit().await;

        self.stats()
    }

    async fn try_emit(&mut self) {
        while let Some(Reverse(c)) = self.heap.peek() {
            let smallest_seq = c.sequence_number;

            if smallest_seq != self.next_expected_seq {
                trace!(
                    "gap at seq {} (heap top: {}), waiting",
                    self.next_expected_seq,
                    smallest_seq
                );
                break;
            }

            let Reverse(chunk) = self.heap.pop().unwrap();
            let seq = chunk.sequence_number;

            let data: Bytes = chunk.payload[ChunkHeader::LEN..].to_vec().into();
            let chunk_crc = if chunk.payload.len() >= ChunkHeader::LEN {
                u32::from_be_bytes([
                    chunk.payload[12],
                    chunk.payload[13],
                    chunk.payload[14],
                    chunk.payload[15],
                ])
            } else {
                0
            };
            let input = CommitGateInput {
                data,
                sequence_number: seq,
                chunk_crc,
            };

            match self.tx.send(input).await {
                Ok(()) => {
                    self.stats.chunks_emitted += 1;
                    self.next_expected_seq = seq.wrapping_add(1);
                    debug!(
                        "chunk {} emitted (next expected: {}, waiting: {})",
                        seq,
                        self.next_expected_seq,
                        self.heap.len()
                    );
                }
                Err(tokio::sync::mpsc::error::SendError(_)) => {
                    debug!("output channel closed, dropping chunk {}", seq);
                    break;
                }
            }
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
            orderer.push_chunk(chunk.clone()).await;
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
            orderer.push_chunk(chunks[i].clone()).await;
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

        orderer.push_chunk(chunks[1].clone()).await;
        orderer.push_chunk(chunks[2].clone()).await;
        orderer.push_chunk(chunks[3].clone()).await;

        assert_eq!(orderer.stats().chunks_emitted, 0);
        assert_eq!(orderer.stats().chunks_waiting, 3);

        orderer.push_chunk(chunks[0].clone()).await;

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

        orderer.push_chunk(chunks[2].clone()).await;
        orderer.push_chunk(chunks[3].clone()).await;
        orderer.push_chunk(chunks[4].clone()).await;
        orderer.push_chunk(chunks[5].clone()).await;

        assert_eq!(orderer.stats().chunks_emitted, 0);
        assert_eq!(orderer.stats().chunks_waiting, 4);

        orderer.push_chunk(chunks[0].clone()).await;

        let stats = orderer.stats();
        assert_eq!(stats.chunks_emitted, 1);
        assert_eq!(stats.chunks_waiting, 4);

        let emitted0 = rx.recv().await.unwrap();
        assert_eq!(emitted0.data, vec![0xDD; 16]);
        assert_eq!(emitted0.sequence_number, 0);

        orderer.push_chunk(chunks[1].clone()).await;

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

        let stats = orderer.push_chunk(chunks[1].clone()).await;
        assert_eq!(stats.chunks_received, 1);
        assert_eq!(stats.chunks_emitted, 0);
        assert_eq!(stats.chunks_waiting, 1);

        let stats = orderer.push_chunk(chunks[0].clone()).await;
        assert_eq!(stats.chunks_received, 2);
        assert_eq!(stats.chunks_emitted, 2);
        assert_eq!(stats.chunks_waiting, 0);
    }

    #[tokio::test]
    async fn empty_payload_handled() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut orderer = ChunkOrderer::new(tx, 0);

        let chunk = build_chunk(0, b"");
        orderer.push_chunk(chunk).await;

        let emitted = rx.recv().await.unwrap();
        assert!(emitted.data.is_empty());
        assert_eq!(emitted.sequence_number, 0);
    }

    #[tokio::test]
    async fn non_zero_initial_seq() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut orderer = ChunkOrderer::new(tx, 100);

        let chunks = build_chunks(100, 3, 0x10);
        orderer.push_chunk(chunks[0].clone()).await;
        orderer.push_chunk(chunks[1].clone()).await;
        orderer.push_chunk(chunks[2].clone()).await;

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

    /// Verifies that a chunk orderer with a small output channel properly
    /// blocks when the channel is full, without losing any data.
    ///
    /// This test creates an orderer with a small output channel,
    /// pushes chunks until the channel fills, and verifies the
    /// orderer blocks (backpressure) rather than dropping data.
    #[tokio::test]
    async fn test_blocking_send_propagates_backpressure() {
        let (tx, rx) = mpsc::channel::<CommitGateInput>(1);
        let mut orderer = ChunkOrderer::new(tx, 0);

        // Push chunk 0 — should emit to channel (has capacity)
        let chunks = build_chunks(0, 2, 0x7F);
        orderer.push_chunk(chunks[0].clone()).await;

        let stats = orderer.stats();
        assert_eq!(stats.chunks_emitted, 1, "chunk 0 should be emitted");

        // Push chunk 1 — try_emit will block because channel is full (capacity 1, rx not consumed)
        let blocked = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            orderer.push_chunk(chunks[1].clone()),
        )
        .await;

        assert!(
            blocked.is_err(),
            "push_chunk should block when output channel is full"
        );

        // Cleanup: drop rx to unblock any pending send
        drop(rx);
    }
}
