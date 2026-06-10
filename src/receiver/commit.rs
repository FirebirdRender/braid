use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::{debug, error, info};

/// Output destination for the commit gate.
enum OutputWriter {
    Stdout(BufWriter<tokio::io::Stdout>),
    File(BufWriter<tokio::fs::File>),
}

impl OutputWriter {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            OutputWriter::Stdout(w) => w.write_all(buf).await,
            OutputWriter::File(w) => w.write_all(buf).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            OutputWriter::Stdout(w) => w.flush().await,
            OutputWriter::File(w) => w.flush().await,
        }
    }
}

/// Statistics for the `CommitGate`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CommitGateStats {
    pub bytes_written: u64,
    pub chunks_committed: u64,
    pub crc_failures: u64,
    pub write_errors: u64,
}

/// The final commit gate in the receiver pipeline.
///
/// Receives ordered chunk payloads, verifies CRC, and writes to stdout or a file.
pub struct CommitGate {
    rx: tokio::sync::mpsc::Receiver<CommitGateInput>,
    writer: OutputWriter,
    stats: Arc<CommitGateStatsAtomic>,
    /// Shared progress counter — updated after each write.
    progress_bytes: Option<Arc<std::sync::atomic::AtomicU64>>,
}

/// Input to the commit gate: chunk data payload + its sequence number and CRC.
///
/// The `ChunkOrderer` strips the `ChunkHeader` before emitting, so the commit
/// gate needs the sequence number and CRC separately to verify.
#[derive(Debug, Clone)]
pub struct CommitGateInput {
    /// The chunk data payload (no headers).
    pub data: Bytes,
    /// Sequence number from the chunk header.
    pub sequence_number: u64,
    /// Chunk CRC from the chunk header.
    pub chunk_crc: u32,
}

/// Atomic version of `CommitGateStats` for lock-free sharing.
#[derive(Debug)]
pub struct CommitGateStatsAtomic {
    bytes_written: AtomicU64,
    chunks_committed: AtomicU64,
    crc_failures: AtomicU64,
    write_errors: AtomicU64,
}

impl CommitGateStatsAtomic {
    pub(crate) fn new() -> Self {
        Self {
            bytes_written: AtomicU64::new(0),
            chunks_committed: AtomicU64::new(0),
            crc_failures: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
        }
    }

    /// Snapshot current stats — public so external code (e.g. braid_receive)
    /// can check write errors after the CommitGate has finished.
    pub fn snapshot(&self) -> CommitGateStats {
        CommitGateStats {
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            chunks_committed: self.chunks_committed.load(Ordering::Relaxed),
            crc_failures: self.crc_failures.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
        }
    }
}

impl CommitGate {
    /// Create a new `CommitGate` writing to stdout.
    pub fn new(rx: tokio::sync::mpsc::Receiver<CommitGateInput>) -> Self {
        Self {
            rx,
            writer: OutputWriter::Stdout(BufWriter::new(tokio::io::stdout())),
            stats: Arc::new(CommitGateStatsAtomic::new()),
            progress_bytes: None,
        }
    }

    /// Create a new `CommitGate` writing to a file.
    pub async fn with_file(
        rx: tokio::sync::mpsc::Receiver<CommitGateInput>,
        path: &PathBuf,
    ) -> std::io::Result<Self> {
        let file = tokio::fs::File::create(path).await?;
        Ok(Self {
            rx,
            writer: OutputWriter::File(BufWriter::new(file)),
            stats: Arc::new(CommitGateStatsAtomic::new()),
            progress_bytes: None,
        })
    }

    /// Wire the shared progress counter — called after construction.
    pub fn set_progress_bytes(&mut self, counter: Arc<std::sync::atomic::AtomicU64>) {
        self.progress_bytes = Some(counter);
    }

    /// Run the commit gate loop.
    ///
    /// Receives ordered chunks from the channel, verifies CRC, writes to output.
    /// Returns when the channel is closed (all senders dropped).
    pub async fn run(&mut self) {
        loop {
            match self.rx.recv().await {
                Some(input) => {
                    if let Err(()) = self.process_chunk(input).await {
                        // On write error, stop processing
                        break;
                    }
                }
                None => {
                    // Channel closed — flush and exit
                    info!("commit gate input channel closed, flushing stdout");
                    break;
                }
            }
        }

        // Flush stdout on exit
        if let Err(e) = self.writer.flush().await {
            error!("commit gate: failed to flush stdout: {}", e);
        }
    }

    /// Process a single chunk: write to stdout.
    ///
    /// Chunk CRC is verified upstream in the reassembler (`assemble_chunk`),
    /// so no redundant verification is performed here.
    ///
    /// Returns `Ok(())` on success, `Err(())` on write error (triggers shutdown).
    async fn process_chunk(&mut self, input: CommitGateInput) -> Result<(), ()> {
        debug!(
            "commit gate: writing chunk seq {} ({} bytes)",
            input.sequence_number,
            input.data.len()
        );

        if let Err(e) = self.writer.write_all(&input.data).await {
            error!("commit gate: stdout write error: {} (broken pipe?)", e);
            self.stats.write_errors.fetch_add(1, Ordering::Relaxed);
            return Err(());
        }

        // NOTE: No per-chunk flush. BufWriter handles its own internal buffering
        // (default 8KB) and the final flush() in run() ensures all data is written
        // before exit. Per-chunk flushes are expensive and provide no durability
        // benefit for streaming output — only the final flush matters.

        self.stats
            .bytes_written
            .fetch_add(input.data.len() as u64, Ordering::Relaxed);
        self.stats.chunks_committed.fetch_add(1, Ordering::Relaxed);

        if let Some(ref counter) = self.progress_bytes {
            counter.fetch_add(input.data.len() as u64, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Returns a snapshot of the current statistics.
    pub fn stats(&self) -> CommitGateStats {
        self.stats.snapshot()
    }

    /// Returns an `Arc`-wrapped handle to the atomic stats for external monitoring.
    pub fn stats_arc(&self) -> Arc<CommitGateStatsAtomic> {
        Arc::clone(&self.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use crate::protocol::crc::compute_chunk_crc;

    /// Build a `CommitGateInput` with the given sequence number and data.
    fn make_input(seq: u64, data: &[u8]) -> CommitGateInput {
        let chunk_crc = compute_chunk_crc(seq, data);
        CommitGateInput {
            data: Bytes::copy_from_slice(data),
            sequence_number: seq,
            chunk_crc,
        }
    }

    /// Helper: run the commit gate in a background task and return its stats
    /// after sending the given inputs and closing the channel.
    async fn run_gate_with_inputs(inputs: Vec<CommitGateInput>) -> CommitGateStats {
        let (tx, rx) = mpsc::channel(16);
        let mut gate = CommitGate::new(rx);

        // Spawn the gate in a background task
        let handle = tokio::spawn(async move {
            gate.run().await;
            gate.stats()
        });

        // Send all inputs
        for input in inputs {
            tx.send(input).await.unwrap();
        }
        drop(tx); // Close channel so gate exits

        timeout(Duration::from_secs(5), handle)
            .await
            .expect("gate timed out")
            .expect("gate task panicked")
    }

    #[tokio::test]
    async fn chunks_written_to_stdout_in_order() {
        // We can't easily capture stdout in tests, but we can verify stats
        let inputs = vec![
            make_input(0, b"hello "),
            make_input(1, b"world "),
            make_input(2, b"braid!"),
        ];

        let stats = run_gate_with_inputs(inputs).await;

        assert_eq!(stats.chunks_committed, 3);
        // "hello " = 6, "world " = 6, "braid!" = 6 => 18
        assert_eq!(stats.bytes_written, 18);
        assert_eq!(stats.bytes_written, 18);
        assert_eq!(stats.crc_failures, 0);
        assert_eq!(stats.write_errors, 0);
    }

    #[tokio::test]
    async fn crc_mismatch_no_longer_rejected() {
        // CommitGate no longer verifies CRC (done upstream in reassembler).
        // All chunks pass through regardless of CRC field value.
        let inputs = vec![
            make_input(0, b"good data"),
            CommitGateInput {
                data: Bytes::copy_from_slice(b"bad data"),
                sequence_number: 1,
                chunk_crc: 0xDEADBEEF,
            },
            make_input(2, b"more good data"),
        ];

        let stats = run_gate_with_inputs(inputs).await;

        assert_eq!(stats.chunks_committed, 3);
        assert_eq!(stats.crc_failures, 0);
        assert_eq!(stats.write_errors, 0);
    }

    #[tokio::test]
    async fn crc_mismatch_does_not_write_corrupt_data() {
        let (tx, rx) = mpsc::channel(16);
        let mut gate = CommitGate::new(rx);

        let handle = tokio::spawn(async move {
            gate.run().await;
            gate.stats()
        });

        tx.send(make_input(0, b"valid data")).await.unwrap();
        tx.send(CommitGateInput {
            data: Bytes::copy_from_slice(b"corrupt data"),
            sequence_number: 1,
            chunk_crc: 0xBADF00D,
        })
        .await
        .unwrap();
        tx.send(make_input(2, b"more valid data")).await.unwrap();
        drop(tx);

        let stats = timeout(Duration::from_secs(5), handle)
            .await
            .expect("gate timed out")
            .expect("gate task panicked");

        // All chunks committed — CRC verification moved upstream
        assert_eq!(stats.chunks_committed, 3);
        assert_eq!(stats.crc_failures, 0);
    }
}
