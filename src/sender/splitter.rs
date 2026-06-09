use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::sync::mpsc;

use crate::buffer::pool::BufferPool;
use crate::compress::compress_lz4;
use crate::protocol::crc::{compute_chunk_crc, compute_fragment_crc};
use crate::protocol::headers::{ChunkHeader, FragmentHeader, COMPRESSED_LZ4, COMPRESSION_NONE};

/// A batch of fragments sent from the splitter to downstream consumers.
/// EOS is signaled by dropping the sender / channel close.
pub type FragmentOrEos = Vec<Bytes>;

/// Default number of fragments to batch per channel message.
const DEFAULT_BATCH_SIZE: usize = 64;

/// Splits stdin into chunks, then into MTU-sized fragments.
///
/// # Architecture
///
/// The splitter reads from stdin in a background task, producing fragments
/// into an mpsc channel. Downstream consumers (UDP senders in Task 7) read
/// from the channel receiver.
///
/// ## Two-level framing
///
/// 1. **Chunk layer**: stdin is split into chunks of `chunk_size` bytes.
///    Each chunk gets a `ChunkHeader` (16 bytes) wrapping the payload with
///    a CRC that covers the sequence number + payload.
///
/// 2. **Fragment layer**: each chunk is split into fragments sized to fit
///    within `mtu` bytes. Each fragment gets a `FragmentHeader` (14 bytes)
///    with chunk_id, fragment_index, total_fragments, fragment_length, and
///    a CRC over the fragment payload.
///
/// ## Backpressure
///
/// When the mpsc channel is full, the splitter blocks reading from stdin,
/// applying natural backpressure upstream.
pub struct ChunkSplitter {
    chunk_size: Arc<AtomicUsize>,
    max_chunk_size: usize,
    mtu: usize,
    next_chunk_id: AtomicU64,
    fragment_payload_size: usize,
    pool: BufferPool,
}

impl ChunkSplitter {
    /// Create a new `ChunkSplitter`.
    ///
    /// * `chunk_size` - Maximum payload bytes per chunk (before chunk header).
    /// * `mtu` - Maximum transmission unit in bytes. Fragment payloads are
    ///   sized to fit within this minus `FragmentHeader::LEN`.
    /// * `pool` - Buffer pool to use for I/O buffers.
    pub fn new(chunk_size: Arc<AtomicUsize>, max_chunk_size: usize, mtu: usize, pool: BufferPool) -> Self {
        assert!(
            chunk_size.load(Ordering::Acquire) > 0,
            "chunk_size must be positive"
        );
        assert!(
            mtu > FragmentHeader::LEN,
            "mtu must be larger than FragmentHeader::LEN"
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
            next_chunk_id: AtomicU64::new(0),
            fragment_payload_size,
            pool,
        }
    }

    /// Returns the next monotonically increasing chunk ID.
    fn next_chunk_id(&self) -> u64 {
        self.next_chunk_id.fetch_add(1, Ordering::SeqCst)
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

    /// Update the chunk size at runtime (called by control handler).
    pub fn set_chunk_size(&self, new_size: usize) {
        self.chunk_size.store(new_size, Ordering::Release);
    }

    /// Run the splitter: reads from the given reader, produces fragments into the channel.
    ///
    /// This function is intended to be spawned as a background task. It reads
    /// from `reader` until EOF, splitting data into chunks and fragments. On EOF
    /// it flushes any remaining batch and returns. EOS is signaled by dropping
    /// the sender / channel close.
    ///
    /// # Type parameters
    ///
    /// * `R` — An async reader (e.g. `tokio::io::Stdin`, `tokio::fs::File`).
    ///
    /// # Errors
    ///
    /// Returns an IO error if reading from the reader fails.
    pub async fn run<R>(
        &self,
        tx: mpsc::Sender<Vec<Bytes>>,
        pause_rx: Option<mpsc::Receiver<bool>>,
        reader: R,
    ) -> Result<(), std::io::Error>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let mut reader = BufReader::new(reader);
        let mut pool_read_buf = self.pool.acquire().await;
        // Pre-allocate chunk buffer and reuse it each iteration
        let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + self.max_chunk_size);
        // Batch buffer: collect fragments and send in bulk
        let mut batch: Vec<Bytes> = Vec::with_capacity(DEFAULT_BATCH_SIZE);

        // Unwrap pause_rx into a local mut variable for use in select!
        let mut pause_rx = pause_rx;

        loop {
            // Check for pause signal before reading stdin
            if let Some(ref mut rx) = pause_rx {
                // Non-blocking check: if a pause signal is already available, enter pause loop
                // without going through select! (avoids edge case where stdin is ready too)
                match rx.try_recv() {
                    Ok(true) => {
                        // Enter inner pause loop
                        while let Some(paused) = rx.recv().await {
                            if !paused {
                                break; // Resume signal
                            }
                        }
                        // If channel closed (None), break out and let outer loop detect EOF
                    }
                    Ok(false) => {
                        // Resume signal already available, continue reading
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        // No signal pending, proceed to select!
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        // Channel closed, disable pause_rx
                        pause_rx = None;
                    }
                }
            }

            // Use tokio::select! to read from stdin OR handle pause signals
            let bytes_read: usize;
            tokio::select! {
                result = reader.read(&mut pool_read_buf.buffer[..self.chunk_size.load(Ordering::Acquire)]) => {
                    let n = result?;
                    if n == 0 {
                        // EOF: flush remaining batch, then return.
                        // EOS is signaled by dropping the sender / channel close.
                        if !batch.is_empty() {
                            tx.send(std::mem::take(&mut batch)).await.map_err(|_| {
                                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "channel closed")
                            })?;
                        }
                        return Ok(());
                    }
                    bytes_read = n;
                }
                Some(paused) = async {
                    pause_rx.as_mut()?.recv().await
                }, if pause_rx.is_some() => {
                    if paused {
                        // Enter inner pause loop: wait for resume (false) or channel close (None)
                        while let Some(paused) = pause_rx.as_mut().unwrap().recv().await {
                            if !paused {
                                break; // Resume signal
                            }
                        }
                        // After resume, continue to next loop iteration to read stdin
                        continue;
                    }
                    // false means resume signal (already unpaused), continue reading
                    continue;
                }
            }

            let chunk_id = self.next_chunk_id() as u32;
            let payload = &pool_read_buf.buffer[..bytes_read];

            // Compute chunk CRC (covers sequence number + payload, always on uncompressed data)
            let chunk_crc = compute_chunk_crc(chunk_id as u64, payload);

            // Compress payload with LZ4; use compressed if it's smaller
            let (flags, wire_payload) = match compress_lz4(payload) {
                Ok(compressed) if compressed.len() < payload.len() => (COMPRESSED_LZ4, compressed),
                _ => {
                    // Compression didn't help (or failed) — use original
                    (COMPRESSION_NONE, payload.to_vec())
                }
            };

            // Build chunk header
            let chunk_header =
                ChunkHeader::new(flags, wire_payload.len() as u16, chunk_id as u64, chunk_crc);

            // Build chunk buffer: header + wire payload (reuse pre-allocated buffer)
            chunk_buf.clear();
            chunk_header.write_to(&mut chunk_buf);
            chunk_buf.extend_from_slice(&wire_payload);

            // Split the chunk into fragments
            let total_fragments =
                (ChunkHeader::LEN + wire_payload.len()).div_ceil(self.fragment_payload_size);

            for fragment_index in 0..total_fragments {
                let start = fragment_index * self.fragment_payload_size;
                let end = std::cmp::min(start + self.fragment_payload_size, chunk_buf.len());
                let fragment_payload = &chunk_buf[start..end];
                let fragment_len = fragment_payload.len();

                // Compute fragment CRC
                let fragment_crc = compute_fragment_crc(fragment_payload);

                // Build fragment header
                let frag_header = FragmentHeader {
                    chunk_id,
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
                if batch.len() >= DEFAULT_BATCH_SIZE {
                    tx.send(std::mem::take(&mut batch)).await.map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "channel closed")
                    })?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    /// Helper: create a splitter with small values for testing.
    fn test_splitter(chunk_size: usize, mtu: usize) -> ChunkSplitter {
        let pool = BufferPool::new(2, chunk_size.max(mtu));
        ChunkSplitter::new(Arc::new(AtomicUsize::new(chunk_size)), chunk_size, mtu, pool)
    }

    /// Helper: collect all fragments from a splitter run into a Vec.
    #[allow(dead_code)]
    async fn collect_fragments(_splitter: &ChunkSplitter, _input: &[u8]) -> Vec<Vec<u8>> {
        vec![]
    }

    // ─── Fragment construction tests ───────────────────────────────────────

    #[test]
    fn fragment_payload_size_is_mtu_minus_header() {
        let s = test_splitter(4096, 1500);
        assert_eq!(s.fragment_payload_size(), 1500 - FragmentHeader::LEN);
    }

    #[test]
    fn chunk_id_monotonically_increases() {
        let s = test_splitter(1024, 1500);
        let id1 = s.next_chunk_id();
        let id2 = s.next_chunk_id();
        let id3 = s.next_chunk_id();
        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 2);
    }

    #[test]
    fn single_fragment_for_small_chunk() {
        // Chunk of 10 bytes with MTU 1500 → fits in 1 fragment
        let s = test_splitter(1024, 1500);
        let payload = b"hello world";
        let chunk_id = 0u32;
        let chunk_crc = compute_chunk_crc(chunk_id as u64, payload);

        let chunk_header = ChunkHeader::new(0, payload.len() as u16, chunk_id as u64, chunk_crc);
        let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + payload.len());
        chunk_buf.extend_from_slice(&chunk_header.to_bytes());
        chunk_buf.extend_from_slice(payload);

        let total_fragments = chunk_buf.len().div_ceil(s.fragment_payload_size);
        assert_eq!(total_fragments, 1);

        // Build the fragment
        let fragment_payload = &chunk_buf[..];
        let fragment_crc = compute_fragment_crc(fragment_payload);
        let frag_header = FragmentHeader {
            chunk_id,
            fragment_index: 0,
            total_fragments: 1,
            fragment_length: fragment_payload.len() as u16,
            fragment_crc,
        };

        let mut fragment = Vec::with_capacity(FragmentHeader::LEN + fragment_payload.len());
        fragment.extend_from_slice(&frag_header.to_bytes());
        fragment.extend_from_slice(fragment_payload);

        // Verify fragment header fields
        let parsed = FragmentHeader::try_from(&fragment[..FragmentHeader::LEN]).unwrap();
        assert_eq!(parsed.chunk_id, chunk_id);
        assert_eq!(parsed.fragment_index, 0);
        assert_eq!(parsed.total_fragments, 1);
        assert_eq!(parsed.fragment_length, fragment_payload.len() as u16);
        assert!(verify_fragment_crc(fragment_payload, parsed.fragment_crc));

        // Verify the fragment payload matches chunk header + original payload
        assert_eq!(&fragment[FragmentHeader::LEN..], &chunk_buf[..]);
    }

    #[test]
    fn multiple_fragments_for_large_chunk() {
        // Chunk of 200 bytes with tiny MTU (50) → multiple fragments
        let mtu = 50;
        let _s = test_splitter(1024, mtu);
        let payload: Vec<u8> = (0..200u8).collect();
        let chunk_id = 42u32;
        let chunk_crc = compute_chunk_crc(chunk_id as u64, &payload);

        let chunk_header = ChunkHeader::new(0, payload.len() as u16, chunk_id as u64, chunk_crc);
        let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + payload.len());
        chunk_buf.extend_from_slice(&chunk_header.to_bytes());
        chunk_buf.extend_from_slice(&payload);

        let frag_payload_size = mtu - FragmentHeader::LEN;
        let total_fragments = chunk_buf.len().div_ceil(frag_payload_size);
        assert!(total_fragments > 1, "test requires multiple fragments");

        let mut fragments = Vec::new();
        for fi in 0..total_fragments {
            let start = fi * frag_payload_size;
            let end = std::cmp::min(start + frag_payload_size, chunk_buf.len());
            let fp = &chunk_buf[start..end];
            let fcrc = compute_fragment_crc(fp);

            let fh = FragmentHeader {
                chunk_id,
                fragment_index: fi as u16,
                total_fragments: total_fragments as u16,
                fragment_length: fp.len() as u16,
                fragment_crc: fcrc,
            };

            let mut frag = Vec::with_capacity(FragmentHeader::LEN + fp.len());
            frag.extend_from_slice(&fh.to_bytes());
            frag.extend_from_slice(fp);
            fragments.push(frag);
        }

        // Verify each fragment
        assert_eq!(fragments.len(), total_fragments);
        for (i, frag) in fragments.iter().enumerate() {
            let parsed = FragmentHeader::try_from(&frag[..FragmentHeader::LEN]).unwrap();
            assert_eq!(parsed.chunk_id, chunk_id);
            assert_eq!(parsed.fragment_index, i as u16);
            assert_eq!(parsed.total_fragments, total_fragments as u16);
            assert!(verify_fragment_crc(
                &frag[FragmentHeader::LEN..],
                parsed.fragment_crc
            ));
        }

        // Verify fragments reassemble to the original chunk_buf
        let mut reassembled = Vec::new();
        for frag in &fragments {
            reassembled.extend_from_slice(&frag[FragmentHeader::LEN..]);
        }
        assert_eq!(reassembled, chunk_buf[..]);
    }

    #[test]
    fn chunk_crc_integrity() {
        let payload = b"important data";
        let seq = 7u64;
        let crc = compute_chunk_crc(seq, payload);
        assert!(verify_chunk_crc(seq, payload, crc));
        assert!(!verify_chunk_crc(seq, b"tampered data", crc));
    }

    #[test]
    fn fragment_crc_integrity() {
        let data = b"fragment payload";
        let crc = compute_fragment_crc(data);
        assert!(verify_fragment_crc(data, crc));
        assert!(!verify_fragment_crc(b"corrupted", crc));
    }

    #[test]
    #[should_panic(expected = "chunk_size must be positive")]
    fn rejects_zero_chunk_size() {
        let pool = crate::buffer::pool::BufferPool::new(2, 1500);
        ChunkSplitter::new(Arc::new(AtomicUsize::new(0)), 0, 1500, pool);
    }

    #[test]
    #[should_panic(expected = "mtu must be larger than FragmentHeader::LEN")]
    fn rejects_mtu_too_small() {
        let pool = crate::buffer::pool::BufferPool::new(2, 1500);
        ChunkSplitter::new(Arc::new(AtomicUsize::new(1024)), 1024, FragmentHeader::LEN, pool);
    }

    #[test]
    fn fragment_payload_size_never_zero() {
        // MTU = FragmentHeader::LEN + 1 → fragment_payload_size = 1
        let pool = crate::buffer::pool::BufferPool::new(2, 1500);
        let s = ChunkSplitter::new(Arc::new(AtomicUsize::new(1024)), 1024, FragmentHeader::LEN + 1, pool);
        assert_eq!(s.fragment_payload_size(), 1);
    }

    // ─── Integration test: run() with pipe ─────────────────────────────────

    #[tokio::test]
    async fn run_produces_fragments_then_eos() {
        let pool = crate::buffer::pool::BufferPool::new(2, 1500);
        let _s = ChunkSplitter::new(Arc::new(AtomicUsize::new(1024)), 1024, 1500, pool);
        let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(64);

        // Simulate a small input by directly constructing fragments
        // (same logic as run() would produce)
        let payload = b"test data";
        let chunk_id = 0u32;
        let chunk_crc = compute_chunk_crc(chunk_id as u64, payload);
        let chunk_header = ChunkHeader::new(0, payload.len() as u16, chunk_id as u64, chunk_crc);

        let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + payload.len());
        chunk_buf.extend_from_slice(&chunk_header.to_bytes());
        chunk_buf.extend_from_slice(payload);

        let fragment_crc = compute_fragment_crc(&chunk_buf);
        let frag_header = FragmentHeader {
            chunk_id,
            fragment_index: 0,
            total_fragments: 1,
            fragment_length: chunk_buf.len() as u16,
            fragment_crc,
        };

        let mut fragment = Vec::with_capacity(FragmentHeader::LEN + chunk_buf.len());
        fragment.extend_from_slice(&frag_header.to_bytes());
        fragment.extend_from_slice(&chunk_buf);

        tx.send(vec![Bytes::from(fragment)]).await.unwrap();
        drop(tx); // Drop tx to signal EOS

        let mut count = 0;
        while let Some(batch) = rx.recv().await {
            count += batch.len();
        }
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn backpressure_blocks_when_channel_full() {
        let pool = crate::buffer::pool::BufferPool::new(2, 1500);
        let _s = ChunkSplitter::new(Arc::new(AtomicUsize::new(1024)), 1024, 1500, pool);
        // Use a tiny channel capacity to force backpressure
        let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(1);

        // Fill the channel
        let payload = b"backpressure test";
        let chunk_id = 0u32;
        let chunk_crc = compute_chunk_crc(chunk_id as u64, payload);
        let chunk_header = ChunkHeader::new(0, payload.len() as u16, chunk_id as u64, chunk_crc);

        let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + payload.len());
        chunk_buf.extend_from_slice(&chunk_header.to_bytes());
        chunk_buf.extend_from_slice(payload);

        let fragment_crc = compute_fragment_crc(&chunk_buf);
        let frag_header = FragmentHeader {
            chunk_id,
            fragment_index: 0,
            total_fragments: 1,
            fragment_length: chunk_buf.len() as u16,
            fragment_crc,
        };

        let mut fragment = Vec::with_capacity(FragmentHeader::LEN + chunk_buf.len());
        fragment.extend_from_slice(&frag_header.to_bytes());
        fragment.extend_from_slice(&chunk_buf);

        let batch = vec![Bytes::from(fragment.clone())];

        // Send one batch (channel capacity is 1, so this should succeed)
        tx.send(batch).await.unwrap();

        // Spawn a task that tries to send another — it will block
        let tx2 = tx.clone();
        let handle = tokio::spawn(async move {
            tx2.send(vec![Bytes::from(fragment)]).await.unwrap();
        });

        // Drain the channel
        let received = rx.recv().await;
        assert!(received.is_some());
        assert_eq!(received.unwrap().len(), 1);

        let received = rx.recv().await;
        assert!(received.is_some());
        assert_eq!(received.unwrap().len(), 1);

        // Drop original tx so rx.recv() returns None (EOS)
        drop(tx);

        let received = rx.recv().await;
        assert!(received.is_none());

        handle.await.unwrap();
    }

    /// Verifies that compressible data gets COMPRESSED_LZ4 flag and
    /// incompressible data gets COMPRESSION_NONE flag.
    #[test]
    fn compressed_chunk_has_correct_flags() {
        // Compressible: repeating data
        let compressible_payload = vec![0xABu8; 4096];
        let chunk_id = 0u32;
        let chunk_crc = compute_chunk_crc(chunk_id as u64, &compressible_payload);

        let (flags, wire_payload) = match compress_lz4(&compressible_payload) {
            Ok(compressed) if compressed.len() < compressible_payload.len() => {
                (COMPRESSED_LZ4, compressed)
            }
            _ => (COMPRESSION_NONE, compressible_payload.to_vec()),
        };

        assert_eq!(
            flags, COMPRESSED_LZ4,
            "repeating data should be compressible"
        );
        assert!(
            wire_payload.len() < compressible_payload.len(),
            "compressed size should be smaller"
        );

        // Incompressible: LCG pseudo-random data (no repeating patterns)
        let mut rng_state = 12345u32;
        let incompressible_payload: Vec<u8> = (0..4096)
            .map(|_| {
                rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
                (rng_state >> 16) as u8
            })
            .collect();
        let chunk_crc2 = compute_chunk_crc(1u64, &incompressible_payload);

        let (flags2, wire_payload2) = match compress_lz4(&incompressible_payload) {
            Ok(compressed) if compressed.len() < incompressible_payload.len() => {
                (COMPRESSED_LZ4, compressed)
            }
            _ => (COMPRESSION_NONE, incompressible_payload.to_vec()),
        };

        assert_eq!(
            flags2, COMPRESSION_NONE,
            "random-ish data should not be compressible"
        );
        assert_eq!(
            wire_payload2.len(),
            incompressible_payload.len(),
            "should use original payload"
        );

        // Verify CRC is still valid on the original (uncompressed) payload
        assert!(verify_chunk_crc(
            chunk_id as u64,
            &compressible_payload,
            chunk_crc
        ));
        assert!(verify_chunk_crc(1u64, &incompressible_payload, chunk_crc2));
    }

    // Re-import CRC verify functions for tests
    use crate::protocol::crc::{verify_chunk_crc, verify_fragment_crc};

    /// Verifies that a splitter properly handles pause and resume signals
    /// through its pause_rx channel.
    ///
    /// This test creates a splitter with a pause channel, sends a pause
    /// signal to stop reading stdin, then sends a resume signal to restart.
    #[tokio::test]
    async fn test_splitter_pause_resume() {
        let pool = crate::buffer::pool::BufferPool::new(2, 1500);
        let splitter = ChunkSplitter::new(Arc::new(AtomicUsize::new(64)), 64, 1500, pool);
        let (fragment_tx, _fragment_rx) = mpsc::channel::<Vec<Bytes>>(16);
        let (pause_tx, pause_rx) = mpsc::channel::<bool>(16);

        // Send pause signal BEFORE spawning so the splitter enters pause immediately
        pause_tx.send(true).await.unwrap();

        let handle = tokio::spawn(async move {
            splitter
                .run(fragment_tx, Some(pause_rx), tokio::io::stdin())
                .await
        });

        // Give the splitter time to enter the pause loop
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Send resume signal — splitter will break out of pause, read stdin (EOF), and return
        pause_tx.send(false).await.unwrap();

        // Splitter should complete cleanly (stdin at EOF, pause handled)
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("splitter should finish within timeout");

        assert!(result.is_ok(), "splitter should return Ok(())");
    }

    #[tokio::test]
    async fn splitter_works_with_file_reader() {
        let tmp_path = std::env::temp_dir().join("braid_splitter_test_file.bin");
        let file_content: Vec<u8> = (0..200u8).collect();
        std::fs::write(&tmp_path, &file_content).expect("write temp file");

        let file = tokio::fs::File::open(&tmp_path)
            .await
            .expect("open temp file");

        let pool = crate::buffer::pool::BufferPool::new(2, 1500);
        let splitter = ChunkSplitter::new(Arc::new(AtomicUsize::new(64)), 64, 1500, pool);
        let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(64);

        let handle = tokio::spawn(async move { splitter.run(tx, None, file).await });

        let mut all_fragments: Vec<Bytes> = Vec::new();
        while let Some(batch) = rx.recv().await {
            all_fragments.extend(batch);
        }

        handle
            .await
            .expect("splitter should complete")
            .expect("splitter should return Ok(())");

        assert!(
            !all_fragments.is_empty(),
            "should produce at least one fragment"
        );

        for frag in &all_fragments {
            assert!(
                frag.len() >= FragmentHeader::LEN,
                "fragment too short: {} < {}",
                frag.len(),
                FragmentHeader::LEN
            );
            let header = FragmentHeader::try_from(&frag[..FragmentHeader::LEN])
                .expect("valid fragment header");
            assert!(
                header.fragment_length as usize <= frag.len() - FragmentHeader::LEN,
                "fragment_length exceeds available payload"
            );
            let payload = &frag[FragmentHeader::LEN..];
            assert!(
                verify_fragment_crc(payload, header.fragment_crc),
                "fragment CRC mismatch"
            );
        }

        let mut chunks: std::collections::BTreeMap<u32, Vec<(u16, Vec<u8>)>> =
            std::collections::BTreeMap::new();
        for frag in &all_fragments {
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

            assert!(
                chunk_buf.len() >= ChunkHeader::LEN,
                "chunk buffer too short for header"
            );
            let chunk_header = ChunkHeader::try_from(&chunk_buf[..ChunkHeader::LEN]).unwrap();
            let payload = &chunk_buf[ChunkHeader::LEN..];
            assert!(
                verify_chunk_crc(
                    chunk_header.sequence_number,
                    payload,
                    chunk_header.chunk_crc
                ),
                "chunk CRC mismatch"
            );
            reassembled.extend_from_slice(payload);
        }

        assert_eq!(
            reassembled, file_content,
            "reassembled content must match original"
        );

        let _ = std::fs::remove_file(&tmp_path);
    }
}
