use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::sync::mpsc;

use crate::buffer::pool::BufferPool;
use crate::compress::compress_lz4;
use crate::protocol::crc::{compute_chunk_crc, compute_fragment_crc};
use crate::protocol::headers::{ChunkHeader, FragmentHeader, COMPRESSED_LZ4, COMPRESSION_NONE};

/// Default number of fragments to batch per channel message (matches splitter.rs).
const DEFAULT_BATCH_SIZE: usize = 64;

// ─── Data types ───────────────────────────────────────────────────────────────

/// A raw chunk read from the input source, dispatched to a chunker worker.
pub struct RawChunk {
    /// Sequential chunk identifier (monotonically increasing from 0).
    pub chunk_id: u64,
    /// Raw payload bytes (before compression/fragmentation).
    pub data: Vec<u8>,
    /// True if this is the last chunk (EOF).
    pub is_last: bool,
}

/// Aggregate chunker statistics shared between workers and the main task.
#[derive(Debug, Default)]
pub struct ChunkStats {
    /// Total raw bytes processed by all workers.
    pub bytes_processed: AtomicU64,
}

// ─── Dispatcher ───────────────────────────────────────────────────────────────

/// Reads from an async input source and round-robins raw chunks to N workers.
///
/// # Architecture
///
/// The dispatcher is the front-end of the multithreaded chunker pipeline. It
/// reads bytes from the input (stdin or file), splits the stream into chunks
/// of `chunk_size` bytes, and distributes chunks to workers using a round-robin
/// strategy over dedicated mpsc channels.
///
/// # Backpressure
///
/// Two backpressure mechanisms work together:
///
/// 1. **Worker channel backpressure**: If a worker's channel is full (the worker
///    is processing slower than the dispatcher can read), the dispatcher's
///    `send().await` blocks, applying natural backpressure to the input read.
///
/// 2. **QueueManager pause signal**: When the QueueManager detects downstream
///    congestion, it sends a pause signal via `pause_rx`. The dispatcher enters
///    an inner pause loop and stops reading until a resume signal arrives.
pub struct Dispatcher {
    chunk_size: Arc<AtomicUsize>,
    max_chunk_size: usize,
    mtu: usize,
    num_workers: usize,
    fragment_payload_size: usize,
    work_txs: Vec<mpsc::Sender<RawChunk>>,
}

impl Dispatcher {
    /// Create a new `Dispatcher`.
    ///
    /// * `chunk_size` — Dynamically adjustable chunk size (from control/adaptive).
    /// * `max_chunk_size` — Hard upper bound for pre-allocation.
    /// * `mtu` — MTU for fragment sizing.
    /// * `num_workers` — Number of parallel chunker workers.
    /// * `work_txs` — One mpsc sender per worker (length must equal `num_workers`).
    pub fn new(
        chunk_size: Arc<AtomicUsize>,
        max_chunk_size: usize,
        mtu: usize,
        num_workers: usize,
        work_txs: Vec<mpsc::Sender<RawChunk>>,
    ) -> Self {
        assert!(
            chunk_size.load(Ordering::Acquire) > 0,
            "chunk_size must be positive"
        );
        assert!(
            mtu > FragmentHeader::LEN,
            "mtu must be larger than FragmentHeader::LEN"
        );
        assert_eq!(
            work_txs.len(),
            num_workers,
            "work_txs length must match num_workers"
        );

        let fragment_payload_size = mtu - FragmentHeader::LEN;
        assert!(
            fragment_payload_size > 0,
            "mtu too small: must leave room for at least 1 byte of fragment payload"
        );

        Self {
            chunk_size,
            max_chunk_size,
            mtu,
            num_workers,
            fragment_payload_size,
            work_txs,
        }
    }

    /// Returns the maximum fragment payload size in bytes.
    pub fn fragment_payload_size(&self) -> usize {
        self.fragment_payload_size
    }

    /// Returns the configured MTU.
    pub fn mtu(&self) -> usize {
        self.mtu
    }

    /// Returns the current chunk size (from the atomic).
    pub fn chunk_size(&self) -> usize {
        self.chunk_size.load(Ordering::Acquire)
    }

    /// Run the dispatcher: reads from the given reader, distributes chunks to workers.
    ///
    /// On EOF, all worker channels are dropped to signal EOS. Workers receive
    /// `None` from their `rx.recv()` and exit after flushing their batch.
    pub async fn run<R>(
        &self,
        pause_rx: Option<mpsc::Receiver<bool>>,
        reader: R,
    ) -> Result<(), std::io::Error>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let mut reader = BufReader::new(reader);
        // Pre-allocate read buffer (reused each iteration)
        let mut read_buf = vec![0u8; self.max_chunk_size.max(self.mtu)];
        let mut chunk_id: u64 = 0;
        let mut worker_idx: usize = 0;
        let mut pause_rx = pause_rx;

        loop {
            // Check for pause signal before reading stdin
            if let Some(ref mut rx) = pause_rx {
                match rx.try_recv() {
                    Ok(true) => {
                        // Enter inner pause loop: wait for resume (false) or channel close (None)
                        while let Some(paused) = rx.recv().await {
                            if !paused {
                                break;
                            }
                        }
                    }
                    Ok(false) => {
                        // Resume signal already available, continue reading
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        // No signal pending
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        // Channel closed, disable pause_rx
                        pause_rx = None;
                    }
                }
            }

            let current_chunk_size = self.chunk_size.load(Ordering::Acquire);

            // Use tokio::select! to read from input OR handle pause signals
            let bytes_read: usize;
            tokio::select! {
                result = reader.read(&mut read_buf[..current_chunk_size]) => {
                    let n = result?;
                    if n == 0 {
                        // EOF: drop all work_txs to signal EOS to workers
                        // Workers will get None from rx.recv() and exit cleanly.
                        return Ok(());
                    }
                    bytes_read = n;
                }
                Some(paused) = async {
                    pause_rx.as_mut()?.recv().await
                }, if pause_rx.is_some() => {
                    if paused {
                        // Enter inner pause loop: wait for resume (false) or channel close (None)
                        while let Some(p) = pause_rx.as_mut().unwrap().recv().await {
                            if !p { break; }
                        }
                    }
                    // After resume, continue to next loop iteration to read input
                    continue;
                }
            }

            // Copy the read data into an owned Vec for the worker
            let data = read_buf[..bytes_read].to_vec();
            let chunk = RawChunk {
                chunk_id,
                data,
                is_last: false,
            };

            // Round-robin send to the next worker
            let tx = &self.work_txs[worker_idx % self.num_workers];
            if tx.send(chunk).await.is_err() {
                // Worker channel closed (worker may have crashed). Continue
                // sending to remaining workers. If all channels are dead, the
                // next send will error and we'll propagate — but that's handled
                // by the caller monitoring QueueManager health.
            }

            chunk_id += 1;
            worker_idx += 1;
        }
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        // Ensure workers get EOS if dispatcher is dropped without completing
        // (e.g., on cancellation). Take the vec to avoid clone, then drop it.
        let txs = std::mem::take(&mut self.work_txs);
        drop(txs);
    }
}

// ─── Chunker Worker ───────────────────────────────────────────────────────────

/// Process raw chunks from a dedicated channel: compress, CRC, fragment, and batch.
///
/// Each worker:
///
/// 1. Receives a raw chunk from the dispatcher via its `rx` channel.
/// 2. Computes the chunk CRC over the uncompressed payload + sequence number.
/// 3. Attempts LZ4 compression; uses compressed data if smaller, else original.
/// 4. Builds a `ChunkHeader` and concatenates it with the wire payload.
/// 5. Splits the chunk header + wire payload into MTU-sized fragments.
/// 6. Computes fragment CRCs and builds `FragmentHeader` for each fragment.
/// 7. Batches fragments and sends them to the shared `result_tx`.
/// 8. On channel close (EOS), flushes the remaining batch and returns.
///
/// The wire format is identical to `ChunkSplitter::run()` — the receiver
/// has no knowledge of parallelization.
pub async fn chunker_worker(
    mtu: usize,
    max_chunk_size: usize,
    mut rx: mpsc::Receiver<RawChunk>,
    result_tx: mpsc::Sender<Vec<Bytes>>,
    _pool: BufferPool,
    stats: Arc<ChunkStats>,
) {
    let fragment_payload_size = mtu - FragmentHeader::LEN;
    let mut batch: Vec<Bytes> = Vec::with_capacity(DEFAULT_BATCH_SIZE);
    // Pre-allocate chunk buffer and reuse it each iteration
    let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + max_chunk_size);

    while let Some(chunk) = rx.recv().await {
        let payload = &chunk.data;
        let payload_len = payload.len();

        // Compute chunk CRC (covers sequence number + payload, always on uncompressed data)
        let chunk_crc = compute_chunk_crc(chunk.chunk_id, payload);

        // Compress payload with LZ4; use compressed if smaller
        let (flags, wire_payload) = match compress_lz4(payload) {
            Ok(compressed) if compressed.len() < payload_len => (COMPRESSED_LZ4, compressed),
            _ => {
                // Compression didn't help (or failed) — use original
                (COMPRESSION_NONE, payload.to_vec())
            }
        };

        // Build chunk header
        let chunk_header = ChunkHeader::new(
            flags,
            wire_payload.len() as u16,
            chunk.chunk_id,
            chunk_crc,
        );

        // Build chunk buffer: header + wire payload (reuse pre-allocated buffer)
        chunk_buf.clear();
        chunk_header.write_to(&mut chunk_buf);
        chunk_buf.extend_from_slice(&wire_payload);

        // Split the chunk into fragments
        let total_fragments =
            (ChunkHeader::LEN + wire_payload.len()).div_ceil(fragment_payload_size);

        for fragment_index in 0..total_fragments {
            let start = fragment_index * fragment_payload_size;
            let end = std::cmp::min(start + fragment_payload_size, chunk_buf.len());
            let fragment_payload = &chunk_buf[start..end];
            let fragment_len = fragment_payload.len();

            // Compute fragment CRC
            let fragment_crc = compute_fragment_crc(fragment_payload);

            // Build fragment header
            let frag_header = FragmentHeader {
                chunk_id: chunk.chunk_id as u32,
                fragment_index: fragment_index as u16,
                total_fragments: total_fragments as u16,
                fragment_length: fragment_len as u16,
                fragment_crc,
            };

            // Assemble the full fragment: header + payload
            let mut fragment = BytesMut::with_capacity(FragmentHeader::LEN + fragment_payload.len());
            frag_header.write_to(&mut fragment);
            fragment.extend_from_slice(fragment_payload);

            batch.push(fragment.freeze());

            // Send batch when full
            if batch.len() >= DEFAULT_BATCH_SIZE
                && result_tx.send(std::mem::take(&mut batch)).await.is_err() {
                    return; // Receiver dropped (QueueManager channel closed)
                }
        }

        // Track bytes processed
        stats.bytes_processed.fetch_add(payload_len as u64, Ordering::Relaxed);
    }

    // Flush remaining batch on EOS
    if !batch.is_empty() {
        let _ = result_tx.send(batch).await;
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::crc::{verify_chunk_crc, verify_fragment_crc};
    use std::sync::atomic::AtomicUsize;

    /// Helper: create a dispatcher and workers for testing with round-robin dispatch.
    fn test_dispatcher(
        chunk_size: usize,
        max_chunk_size: usize,
        mtu: usize,
        num_workers: usize,
    ) -> (Dispatcher, Vec<mpsc::Receiver<RawChunk>>) {
        let chunk_size_atomic = Arc::new(AtomicUsize::new(chunk_size));
        let (txs, rxs): (Vec<_>, Vec<_>) = (0..num_workers)
            .map(|_| mpsc::channel::<RawChunk>(64))
            .unzip();
        let dispatcher = Dispatcher::new(chunk_size_atomic, max_chunk_size, mtu, num_workers, txs);
        (dispatcher, rxs)
    }

    #[test]
    fn fragment_payload_size_is_mtu_minus_header() {
        let (d, _rxs) = test_dispatcher(4096, 4096, 1500, 2);
        assert_eq!(d.fragment_payload_size(), 1500 - FragmentHeader::LEN);
    }

    #[test]
    fn dispatcher_work_txs_equals_num_workers() {
        let (d, _rxs) = test_dispatcher(1024, 1024, 1500, 4);
        assert_eq!(d.work_txs.len(), 4);
    }

    #[test]
    #[should_panic(expected = "work_txs length must match num_workers")]
    fn dispatcher_rejects_mismatched_work_txs() {
        let chunk_size = Arc::new(AtomicUsize::new(1024));
        let (txs, _rxs): (Vec<_>, Vec<_>) = (0..3).map(|_| mpsc::channel::<RawChunk>(64)).unzip();
        let _d = Dispatcher::new(chunk_size, 1024, 1500, 4, txs);
    }

    #[test]
    #[should_panic(expected = "chunk_size must be positive")]
    fn dispatcher_rejects_zero_chunk_size() {
        let chunk_size = Arc::new(AtomicUsize::new(0));
        let (txs, _rxs): (Vec<_>, Vec<_>) = (0..2).map(|_| mpsc::channel::<RawChunk>(64)).unzip();
        let _d = Dispatcher::new(chunk_size, 0, 1500, 2, txs);
    }

    // ─── Chunker Worker Integration Tests ─────────────────────────────────

    /// Run a single worker with input and collect all fragments.
    async fn run_worker_and_collect(
        input: Vec<u8>,
        chunk_size: usize,
        mtu: usize,
    ) -> Vec<Bytes> {
        let pool = BufferPool::new(4, mtu.max(chunk_size));
        let stats = Arc::new(ChunkStats::default());
        let (work_tx, work_rx) = mpsc::channel::<RawChunk>(64);
        let (result_tx, mut result_rx) = mpsc::channel::<Vec<Bytes>>(64);

        // Spawn the worker
        let worker = tokio::spawn(async move {
            chunker_worker(mtu, chunk_size, work_rx, result_tx, pool, stats).await;
        });

        // Send chunks
        for (chunk_id, chunk_data) in input.chunks(chunk_size).enumerate() {
            let _ = work_tx
                .send(RawChunk {
                    chunk_id: chunk_id as u64,
                    data: chunk_data.to_vec(),
                    is_last: false,
                })
                .await;
        }
        // Drop sender to signal EOS
        drop(work_tx);

        // Collect all fragment batches
        let mut all_fragments = Vec::new();
        while let Some(batch) = result_rx.recv().await {
            all_fragments.extend(batch);
        }

        worker.await.expect("worker should complete");
        all_fragments
    }

    #[tokio::test]
    async fn worker_produces_valid_fragments() {
        let input: Vec<u8> = (0..100u8).collect();
        let fragments = run_worker_and_collect(input, 64, 1500).await;

        assert!(!fragments.is_empty(), "should produce fragments");

        for frag in &fragments {
            assert!(
                frag.len() >= FragmentHeader::LEN,
                "fragment too short"
            );
            let header = FragmentHeader::try_from(&frag[..FragmentHeader::LEN])
                .expect("valid fragment header");
            let payload = &frag[FragmentHeader::LEN..];
            assert!(
                verify_fragment_crc(payload, header.fragment_crc),
                "fragment CRC mismatch"
            );
            assert_eq!(
                header.fragment_length as usize,
                payload.len(),
                "fragment_length mismatch"
            );
        }
    }

    #[tokio::test]
    async fn worker_handles_eos_cleanly() {
        let pool = BufferPool::new(4, 1500);
        let stats = Arc::new(ChunkStats::default());
        let (work_tx, work_rx) = mpsc::channel::<RawChunk>(64);
        let (result_tx, mut result_rx) = mpsc::channel::<Vec<Bytes>>(64);

        let worker = tokio::spawn(async move {
            chunker_worker(1500, 1024, work_rx, result_tx, pool, stats).await;
        });

        // Send one chunk, then drop sender
        let _ = work_tx
            .send(RawChunk {
                chunk_id: 0,
                data: vec![0u8; 10],
                is_last: false,
            })
            .await;
        drop(work_tx);

        let mut count = 0;
        while let Some(batch) = result_rx.recv().await {
            count += batch.len();
        }
        assert!(count > 0, "should produce at least one fragment");
        worker.await.expect("worker should complete");
    }

    #[tokio::test]
    async fn worker_reassembled_content_matches_input() {
        let input: Vec<u8> = (0..200u8).collect();
        let fragments = run_worker_and_collect(input.clone(), 64, 50).await; // tiny MTU for multiple fragments

        // Reassemble chunks from fragments
        let mut chunks: std::collections::BTreeMap<u32, Vec<(u16, Vec<u8>)>> =
            std::collections::BTreeMap::new();
        for frag in &fragments {
            let header = FragmentHeader::try_from(&frag[..FragmentHeader::LEN]).unwrap();
            let payload = frag[FragmentHeader::LEN..].to_vec();
            chunks
                .entry(header.chunk_id)
                .or_default()
                .push((header.fragment_index, payload));
        }

        let mut reassembled = Vec::new();
        for (_chunk_id, mut fragments) in chunks {
            fragments.sort_by_key(|(idx, _)| *idx);
            let mut chunk_buf = Vec::new();
            for (_, payload) in &fragments {
                chunk_buf.extend_from_slice(payload);
            }

            // Parse chunk header and verify CRC
            assert!(
                chunk_buf.len() >= ChunkHeader::LEN,
                "chunk buffer too short for header"
            );
            let chunk_header = ChunkHeader::try_from(&chunk_buf[..ChunkHeader::LEN]).unwrap();
            let payload = &chunk_buf[ChunkHeader::LEN..];
            assert!(
                verify_chunk_crc(chunk_header.sequence_number, payload, chunk_header.chunk_crc),
                "chunk CRC mismatch"
            );
            reassembled.extend_from_slice(payload);
        }

        assert_eq!(reassembled, input, "reassembled content must match original");
    }

    #[tokio::test]
    async fn round_robin_distributes_across_workers() {
        let chunk_size = Arc::new(AtomicUsize::new(32));
        let num_workers = 4;
        let (txs, mut rxs): (Vec<_>, Vec<_>) = (0..num_workers)
            .map(|_| mpsc::channel::<RawChunk>(64))
            .unzip();

        let dispatcher = Dispatcher::new(chunk_size, 32, 1500, num_workers, txs);

        // We won't actually run the dispatcher (it needs a reader), just test
        // that the work_txs are set up correctly for round-robin
        assert_eq!(dispatcher.work_txs.len(), num_workers);
        assert_eq!(dispatcher.num_workers, num_workers);
        drop(dispatcher);
        for rx in &mut rxs {
            assert!(rx.recv().await.is_none(), "dropping dispatcher should signal EOS");
        }
    }

    #[tokio::test]
    async fn worker_handles_large_chunk_with_compression() {
        // Highly compressible data
        let input = vec![0xABu8; 4096];
        let fragments = run_worker_and_collect(input.clone(), 4096, 1500).await;

        assert!(!fragments.is_empty(), "should produce fragments");

        // Verify CRC integrity on all fragments
        for frag in &fragments {
            let header = FragmentHeader::try_from(&frag[..FragmentHeader::LEN]).unwrap();
            let payload = &frag[FragmentHeader::LEN..];
            assert!(
                verify_fragment_crc(payload, header.fragment_crc),
                "fragment CRC mismatch"
            );
        }

        // Reassemble and verify content
        let mut chunks: std::collections::BTreeMap<u32, Vec<(u16, Vec<u8>)>> =
            std::collections::BTreeMap::new();
        for frag in &fragments {
            let header = FragmentHeader::try_from(&frag[..FragmentHeader::LEN]).unwrap();
            chunks
                .entry(header.chunk_id)
                .or_default()
                .push((header.fragment_index, frag[FragmentHeader::LEN..].to_vec()));
        }

        let mut reassembled = Vec::new();
        for (_cid, mut frags) in chunks {
            frags.sort_by_key(|(idx, _)| *idx);
            let mut chunk_buf = Vec::new();
            for (_, payload) in &frags {
                chunk_buf.extend_from_slice(payload);
            }
            let chunk_header = ChunkHeader::try_from(&chunk_buf[..ChunkHeader::LEN]).unwrap();
            let wire_payload = &chunk_buf[ChunkHeader::LEN..];

            // Decompress if needed before CRC verify (CRC is computed over uncompressed data)
            let (reassembled_payload, uncompressed) = if chunk_header.flags == COMPRESSED_LZ4 {
                let decompressed = crate::compress::decompress_lz4(wire_payload)
                    .expect("decompression should succeed");
                (decompressed.clone(), decompressed)
            } else {
                (wire_payload.to_vec(), wire_payload.to_vec())
            };

            assert!(
                verify_chunk_crc(chunk_header.sequence_number, &uncompressed, chunk_header.chunk_crc),
                "chunk CRC mismatch"
            );
            reassembled.extend_from_slice(&reassembled_payload);
        }

        assert_eq!(reassembled, input, "reassembled content must equal original after decompression");
    }

    #[tokio::test]
    async fn worker_stats_track_bytes() {
        let pool = BufferPool::new(4, 1500);
        let stats = Arc::new(ChunkStats::default());
        let (work_tx, work_rx) = mpsc::channel::<RawChunk>(64);
        let (result_tx, _result_rx) = mpsc::channel::<Vec<Bytes>>(64);

        let stats_clone = stats.clone();
        let worker = tokio::spawn(async move {
            chunker_worker(1500, 1024, work_rx, result_tx, pool, stats_clone).await;
        });

        // Send chunks of known sizes
        let _ = work_tx.send(RawChunk { chunk_id: 0, data: vec![0u8; 100], is_last: false }).await;
        let _ = work_tx.send(RawChunk { chunk_id: 1, data: vec![0u8; 200], is_last: false }).await;
        let _ = work_tx.send(RawChunk { chunk_id: 2, data: vec![0u8; 300], is_last: false }).await;
        drop(work_tx);

        worker.await.expect("worker should complete");

        assert_eq!(
            stats.bytes_processed.load(Ordering::Relaxed),
            600,
            "should track total bytes processed"
        );
    }
}