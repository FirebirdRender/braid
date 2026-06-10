use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

use braid::buffer::pool::BufferPool;
use braid::control::client::{ControlClient, ControlError};
use braid::control::negotiation::{negotiate, NegotiationConfig};
use braid::file_mode::sender::FileModeSender;
use braid::flow::SenderReactor;
use braid::progress::reporter::{ProgressReporter, ProgressVerbosity};
use braid::protocol::ControlMessage;
use braid::sender::parallel_splitter::{chunker_worker, Dispatcher, RawChunk, ChunkStats};
use braid::sender::queue::QueueManagerBuilder;
use braid::sender::splitter::ChunkSplitter;
use braid::sender::worker::{BatchSendWorker, UdpSendWorker, UdpSendWorkerStats, WorkerResult};
use braid::shutdown::manager::ShutdownManager;

use super::Mode;

/// Default high-watermark for per-worker pending bytes (1 MB).
const DEFAULT_HIGH_WATERMARK: u64 = 1024 * 1024;

/// Default mpsc channel capacity per worker.
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Default progress reporting interval.
const DEFAULT_PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

/// Default kernel send buffer size (64 MB).
const DEFAULT_SO_SNDBUF: usize = 64 * 1024 * 1024;

/// Default per-datagram send timeout.
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(2);

/// Default sendmmsg batch size (datagrams per syscall).
const DEFAULT_BATCH_SIZE: usize = 16;

/// Default batch flush timeout in microseconds.
const DEFAULT_BATCH_USEC: u64 = 100;

/// Default initial channel count for negotiation.
const DEFAULT_INITIAL_CHANNELS: usize = 4;

/// Default initial chunk size for negotiation (log2: 2^10 = 1024).
const DEFAULT_MIN_CHUNK_LOG2: u32 = 10;

/// Default max chunk size for negotiation (log2: 2^20 = 1048576).
const DEFAULT_MAX_CHUNK_LOG2: u32 = 20;

/// High-level orchestrator for the `braid send` command.
pub struct BraidSend {
    destination: SocketAddr,
    chunk_size: usize,
    channels: usize,
    mtu: usize,
    max_rate: u64,
    verbosity: ProgressVerbosity,
    mode: Mode,
    input: Option<PathBuf>,
    compress_lz4: bool,
    compress_zstd: bool,
    #[allow(dead_code)]
    retry: bool,
    #[allow(dead_code)]
    max_retries: u32,
    #[allow(dead_code)]
    retry_delay: u64,
    #[allow(dead_code)]
    channel_failure_threshold: u32,
    batch_size: usize,
    batch_usec: u64,
    no_batch: bool,
    chunker_threads: usize,
}

/// Dispatches between BatchSendWorker and UdpSendWorker at creation and spawn time.
enum WorkerKind {
    Batch(BatchSendWorker),
    Single(UdpSendWorker),
}

impl WorkerKind {
    async fn bind(&self) -> std::io::Result<UdpSocket> {
        match self {
            WorkerKind::Batch(w) => w.bind().await,
            WorkerKind::Single(w) => w.bind().await,
        }
    }

    async fn run(self, socket: UdpSocket, rx: mpsc::Receiver<Bytes>) {
        match self {
            WorkerKind::Batch(w) => w.run(socket, rx).await,
            WorkerKind::Single(w) => w.run(socket, rx).await,
        }
    }
}

impl BraidSend {
    /// Construct a `BraidSend` for the default pipe mode (reads from stdin).
    #[allow(dead_code)]
    pub fn new(
        destination: SocketAddr,
        chunk_size: usize,
        channels: usize,
        mtu: usize,
        max_rate: u64,
        verbosity: ProgressVerbosity,
    ) -> Self {
        Self::new_with_mode(
            destination,
            chunk_size,
            channels,
            mtu,
            max_rate,
            verbosity,
            Mode::Pipe,
            None,
            true,  // compress_lz4
            false, // compress_zstd
            DEFAULT_BATCH_SIZE,
            DEFAULT_BATCH_USEC,
            false, // no_batch
            0,     // chunker_threads: 0 = auto
        )
    }

    /// Construct a `BraidSend` with explicit mode and input selection.
    ///
    /// In `Mode::Pipe`, `input` must be `None`; the sender reads from stdin.
    /// In `Mode::File`, `input` must be `Some(path)`; the sender reads from
    /// the file and exchanges file-mode control messages with the receiver.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_mode(
        destination: SocketAddr,
        chunk_size: usize,
        channels: usize,
        mtu: usize,
        max_rate: u64,
        verbosity: ProgressVerbosity,
        mode: Mode,
        input: Option<PathBuf>,
        compress_lz4: bool,
        compress_zstd: bool,
        batch_size: usize,
        batch_usec: u64,
        no_batch: bool,
        chunker_threads: usize,
    ) -> Self {
        Self {
            destination,
            chunk_size,
            channels,
            mtu,
            max_rate,
            verbosity,
            mode,
            input,
            compress_lz4,
            compress_zstd,
            retry: false,
            max_retries: 3,
            retry_delay: 1000,
            channel_failure_threshold: 3,
            batch_size,
            batch_usec,
            no_batch,
            chunker_threads,
        }
    }

    /// Extended constructor with retry and channel failure parameters.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_retry(
        destination: SocketAddr,
        chunk_size: usize,
        channels: usize,
        mtu: usize,
        max_rate: u64,
        verbosity: ProgressVerbosity,
        mode: Mode,
        input: Option<PathBuf>,
        compress_lz4: bool,
        compress_zstd: bool,
        retry: bool,
        max_retries: u32,
        retry_delay: u64,
        channel_failure_threshold: u32,
        batch_size: usize,
        batch_usec: u64,
        no_batch: bool,
        chunker_threads: usize,
    ) -> Self {
        Self {
            destination,
            chunk_size,
            channels,
            mtu,
            max_rate,
            verbosity,
            mode,
            input,
            compress_lz4,
            compress_zstd,
            retry,
            max_retries,
            retry_delay,
            channel_failure_threshold,
            batch_size,
            batch_usec,
            no_batch,
            chunker_threads,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        // ─── Step 1: Shutdown manager ────────────────────────────────────
        let shutdown = ShutdownManager::new();
        let signal_received = Arc::new(AtomicBool::new(false));
        let shutdown_signal = shutdown.clone();
        let sr = signal_received.clone();
        tokio::spawn(async move {
            shutdown_signal.await_signals().await;
            sr.store(true, Ordering::SeqCst);
        });

        // ─── Step 2: Connect to receiver ─────────────────────────────────
        info!("connecting to receiver at {}", self.destination);
        let mut client = if self.retry {
            ControlClient::connect_with_retry(
                self.destination,
                self.max_retries,
                Duration::from_millis(self.retry_delay),
                Duration::from_secs(10),
            )
            .await
            .map_err(|e| format!("control connection failed after retries: {e}"))?
        } else {
            ControlClient::connect(self.destination)
                .await
                .map_err(|e| format!("control connection failed: {e}"))?
        };

        // ─── Step 3: Negotiate channels ──────────────────────────────────
        let channel_count = if self.channels > 0 {
            self.channels as u8
        } else {
            DEFAULT_INITIAL_CHANNELS as u8
        };
        let mtu_log2 = (self.mtu as f64).log2().ceil() as u32;
        let mtu_log2 = mtu_log2.min(255);
        let config = NegotiationConfig {
            channel_count,
            min_chunk: DEFAULT_MIN_CHUNK_LOG2,
            max_chunk: DEFAULT_MAX_CHUNK_LOG2,
            mtu: mtu_log2,
            compression_lz4: self.compress_lz4,
            compression_zstd: self.compress_zstd,
        };
        info!("negotiating {} channels with receiver", channel_count);
        let result = negotiate(&mut client, config)
            .await
            .map_err(|e| format!("negotiation failed: {e}"))?;
        let channels = result.channels;
        info!("negotiated {} channels", channels.len());

        // ─── Step 3.5: File mode setup (after negotiation, before pipeline) ─
        // In file mode, exchange file metadata with the receiver BEFORE the
        // splitter starts streaming, so the receiver can prepare its output
        // path and verify integrity after transfer.
        let (file_input, file_label, file_total) = match self.mode {
            Mode::File => {
                let input_path = self.input.clone().ok_or_else(|| {
                    Box::<dyn std::error::Error>::from("file mode requires --input <PATH>")
                })?;
                let file_sender = FileModeSender::new(input_path)
                    .map_err(|e| format!("file mode setup failed: {e}"))?;
                let meta = file_sender
                    .prepare()
                    .await
                    .map_err(|e| format!("file metadata prepare failed: {e}"))?;
                info!("file mode: sending {}", meta);
                client
                    .send_message(&ControlMessage::FileStart {
                        filename: meta.filename.clone(),
                        filesize: meta.filesize,
                        file_crc32c: meta.file_crc32c,
                    })
                    .await
                    .map_err(|e| format!("failed to send FileStart: {e}"))?;
                let file = file_sender
                    .open_async()
                    .await
                    .map_err(|e| format!("failed to open input file: {e}"))?;
                let total = meta.filesize;
                let label = meta.filename.clone();
                (Some(file), Some(label), Some(total))
            }
            Mode::Pipe => (None, None, None),
        };

        // ─── Step 4: Create UDP send workers ─────────────────────────────
        let mut worker_sockets = Vec::with_capacity(channels.len());
        let mut health_rxs = Vec::with_capacity(channels.len());
        for ch in channels.iter() {
            let (health_tx, health_rx) = mpsc::channel::<WorkerResult>(16);
            let dest = SocketAddr::new(self.destination.ip(), ch.port);
            let worker: WorkerKind = if self.no_batch {
                WorkerKind::Single(UdpSendWorker::new(
                    0,
                    dest,
                    DEFAULT_SO_SNDBUF,
                    DEFAULT_SEND_TIMEOUT,
                    Arc::new(UdpSendWorkerStats::default()),
                    Some(health_tx),
                ))
            } else {
                WorkerKind::Batch(BatchSendWorker::new(
                    0,
                    dest,
                    DEFAULT_SO_SNDBUF,
                    DEFAULT_SEND_TIMEOUT,
                    self.batch_size,
                    self.batch_usec,
                    Arc::new(UdpSendWorkerStats::default()),
                    Some(health_tx),
                ))
            };
            let socket = worker.bind().await?;
            worker_sockets.push((worker, socket));
            health_rxs.push(health_rx);
        }

        // ─── Step 5: Build QueueManager ──────────────────────────────────
        let (bp_tx, mut bp_rx) = mpsc::channel::<bool>(16);
        let mut qm_builder = QueueManagerBuilder::new(channels.len())
            .high_watermark(DEFAULT_HIGH_WATERMARK)
            .channel_capacity(DEFAULT_CHANNEL_CAPACITY)
            .backpressure_tx(bp_tx);
        if self.max_rate > 0 {
            qm_builder = qm_builder.max_rate(self.max_rate);
        }
        let (mut queue_manager, worker_receivers) = qm_builder.build();

        // ─── Step 6: Spawn UDP send workers ──────────────────────────────
        let mut worker_handles = Vec::with_capacity(channels.len());
        for (i, ((worker, socket), (rx, _stats))) in
            worker_sockets.into_iter().zip(worker_receivers).enumerate()
        {
            let local_port = socket.local_addr().map(|a| a.port()).unwrap_or(0);
            let handle = tokio::spawn(async move {
                info!("UDP send worker {} started on port {}", i, local_port);
                worker.run(socket, rx).await;
                info!("UDP send worker {} finished", i);
            });
            worker_handles.push(handle);
        }

        // ─── Step 7: Create pipeline channels ────────────────────────────
        let (fragment_tx, fragment_rx) = mpsc::channel::<Vec<Bytes>>(DEFAULT_CHANNEL_CAPACITY);

        // ─── Step 8: Create progress reporter ────────────────────────────
        let mut progress = if self.mode == Mode::File {
            ProgressReporter::new_with_total(
                DEFAULT_PROGRESS_INTERVAL,
                self.verbosity,
                file_label.clone(),
                file_total,
            )
        } else {
            ProgressReporter::new(DEFAULT_PROGRESS_INTERVAL, self.verbosity)
        };
        queue_manager.set_progress_bytes(progress.bytes_tx());
        let progress_bytes_tx = progress.bytes_tx();
        let input_path = self.input.clone();

// ─── Step 9: Spawn pipeline tasks ────────────────────────────────
        // Determine number of chunker workers.
        let num_workers: usize = if self.chunker_threads > 0 {
            self.chunker_threads
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get() / 2)
                .unwrap_or(1)
                .max(1)
        };

        let effective_chunk_size = if self.chunk_size > 0 {
            self.chunk_size
        } else {
            braid::adaptive::chunk_size::DEFAULT_INITIAL_CHUNK
        };
        let max_chunk_size = effective_chunk_size.max(braid::adaptive::chunk_size::MAX_CHUNK);
        let pool_buf_size = max_chunk_size.max(self.mtu);
        let chunk_size_atomic = Arc::new(std::sync::atomic::AtomicUsize::new(effective_chunk_size));

        let (splitter_pause_tx, splitter_pause_rx) = mpsc::channel::<bool>(16);

        // Extract values needed by spawned coroutines so they are 'static.
        let mtu = self.mtu;
        let fragment_tx_for_workers = fragment_tx.clone();

        let splitter_handle: tokio::task::JoinHandle<()> = if num_workers <= 1 {
            let splitter_pool = BufferPool::new(4, pool_buf_size);
            let splitter = ChunkSplitter::new(
                chunk_size_atomic.clone(),
                max_chunk_size,
                mtu,
                splitter_pool,
            );
            if let Some(file) = file_input {
                tokio::spawn(async move {
                    info!("chunk splitter started (file input)");
                    if let Err(e) = splitter.run(fragment_tx, Some(splitter_pause_rx), file).await {
                        error!("chunk splitter error: {}", e);
                    }
                    info!("chunk splitter finished");
                })
            } else {
                tokio::spawn(async move {
                    info!("chunk splitter started (stdin)");
                    if let Err(e) = splitter.run(fragment_tx, Some(splitter_pause_rx), tokio::io::stdin()).await {
                        error!("chunk splitter error: {}", e);
                    }
                    info!("chunk splitter finished");
                })
            }
        } else {
            let pool = BufferPool::new(num_workers * 2, pool_buf_size);
            let stats = Arc::new(ChunkStats::default());

            let (work_txs, work_rxs): (Vec<_>, Vec<_>) = (0..num_workers)
                .map(|_| mpsc::channel::<RawChunk>(64))
                .unzip();

            // Spawn N chunker workers
            for rx in work_rxs {
                let worker_pool = pool.clone();
                let worker_stats = stats.clone();
                let worker_fragment_tx = fragment_tx_for_workers.clone();
                tokio::spawn(async move {
                    trace!("chunker worker started");
                    chunker_worker(
                        mtu,
                        max_chunk_size,
                        rx,
                        worker_fragment_tx,
                        worker_pool,
                        worker_stats,
                    )
                    .await;
                    trace!("chunker worker finished");
                });
            }

            // Spawn the dispatcher
            let dispatcher = Dispatcher::new(
                chunk_size_atomic.clone(),
                max_chunk_size,
                mtu,
                num_workers,
                work_txs,
            );
            if let Some(file) = file_input {
                tokio::spawn(async move {
                    info!("dispatcher started (file input)");
                    if let Err(e) = dispatcher.run(Some(splitter_pause_rx), file).await {
                        error!("dispatcher error: {}", e);
                    }
                    info!("dispatcher finished");
                })
            } else {
                tokio::spawn(async move {
                    info!("dispatcher started (stdin)");
                    if let Err(e) = dispatcher.run(Some(splitter_pause_rx), tokio::io::stdin()).await {
                        error!("dispatcher error: {}", e);
                    }
                    info!("dispatcher finished");
                })
            }
        };

        // Spawn QueueManager dispatch loop.
        // Returns QmExitReason via oneshot so main task knows whether to reconnect.
        #[derive(Debug)]
        enum QmExitReason {
            Normal,
            AllWorkersDown { last_chunk_id: u32 },
        }
        let (qm_exit_tx, mut qm_exit_rx) = tokio::sync::oneshot::channel::<QmExitReason>();
        let qm_shutdown = shutdown.clone();
        let qm_handle = tokio::spawn(async move {
            info!("queue manager dispatch loop started");
            let mut rx = fragment_rx;
            let mut exit_reason = QmExitReason::Normal;

            // Merge all health_rx channels into a single receiver.
            let (health_merge_tx, mut health_merge_rx) = mpsc::channel::<(usize, braid::sender::worker::WorkerResult)>(128);
            for (i, mut hrx) in health_rxs.into_iter().enumerate() {
                let htx = health_merge_tx.clone();
                tokio::spawn(async move {
                    while let Some(result) = hrx.recv().await {
                        if htx.send((i, result)).await.is_err() {
                            break;
                        }
                    }
                });
            }
            // Drop the clone held by this scope so the merge rx closes when all forwarding tasks finish.
            drop(health_merge_tx);

            loop {
                tokio::select! {
                    batch = rx.recv() => {
                        match batch {
                            Some(batch) => {
                                if qm_shutdown.is_shutting_down() {
                                    break;
                                }
                                if let Err(e) = queue_manager.dispatch_batch(batch) {
                                    error!("queue manager dispatch_batch error: {}", e);
                                    if queue_manager.all_workers_down() {
                                        exit_reason = QmExitReason::AllWorkersDown {
                                            last_chunk_id: queue_manager.last_chunk_id(),
                                        };
                                    }
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    health = health_merge_rx.recv() => {
                        match health {
                            Some((worker_idx, braid::sender::worker::WorkerResult::Failed)) => {
                                warn!("health: worker {} reported failure, marking as failed", worker_idx);
                                if queue_manager.active_worker_count() > 0 {
                                    queue_manager.mark_worker_failed(worker_idx);
                                }
                            }
                            Some((_worker_idx, braid::sender::worker::WorkerResult::Done)) => {
                                trace!("health: worker {} finished normally", _worker_idx);
                            }
                            None => {
                                trace!("health merge channel closed");
                            }
                        }
                    }
                }
            }
            let _ = qm_exit_tx.send(exit_reason);
            info!("queue manager dispatch loop finished");
        });

        // Spawn FlowController (SenderReactor)
        let (queue_status_tx, queue_status_rx) =
            mpsc::channel::<braid::protocol::ControlMessage>(16);
        let (control_out_tx, mut control_out_rx) =
            mpsc::channel::<braid::protocol::ControlMessage>(16);
        let (reconnect_resp_tx, mut reconnect_resp_rx) =
            mpsc::channel::<braid::protocol::ControlMessage>(16);
        let (flow_pause_tx, _flow_pause_rx) = mpsc::channel::<bool>(16);
        let mut sender_reactor = SenderReactor::new(1024, queue_status_rx, Some(flow_pause_tx));
        let flow_handle = tokio::spawn(async move {
            info!("flow sender reactor started");
            sender_reactor.run().await;
            info!("flow sender reactor finished");
        });

        // Spawn backpressure handler
        let bp_shutdown = shutdown.clone();
        let bp_handle = tokio::spawn(async move {
            info!("backpressure handler started");
            while let Some(paused) = bp_rx.recv().await {
                if bp_shutdown.is_shutting_down() {
                    break;
                }
                if paused {
                    info!("backpressure: pausing splitter");
                    let _ = splitter_pause_tx.send(true).await;
                } else {
                    info!("backpressure: resuming splitter");
                    let _ = splitter_pause_tx.send(false).await;
                }
            }
            info!("backpressure handler finished");
        });

        // Spawn ProgressReporter tick loop
        let progress_shutdown = shutdown.clone();
        let progress_handle = tokio::spawn(async move {
            info!("progress reporter started");
            let mut stdout = std::io::stdout();
            loop {
                tokio::time::sleep(DEFAULT_PROGRESS_INTERVAL).await;
                if progress_shutdown.is_shutting_down() {
                    let _ = progress.tick(&mut stdout, true, true);
                    break;
                }
                let _ = progress.tick(&mut stdout, false, false);
            }
            let summary = progress.finalize_summary();
            info!("{}", summary);
            eprintln!("{}", summary);
            info!("progress reporter finished");
        });

        // TCP control forwarding loop.
        // Forwards Ack/ChannelOpened to reconnect_resp_tx so main task can
        // implement the reconnect protocol.
        let (file_complete_tx, file_complete_rx) =
            tokio::sync::oneshot::channel::<braid::protocol::ControlMessage>();
        let is_file_mode = self.mode == Mode::File;
        let mut client = client;
        let control_recv_handle = tokio::spawn(async move {
            let mut file_complete_tx = Some(file_complete_tx);
            let mut send_closed = false;
            loop {
                if !send_closed {
                    tokio::select! {
                        result = client.recv_message() => {
                            match result {
                                Ok(msg) => {
                                    match &msg {
                                        braid::protocol::ControlMessage::QueueStatus { .. } => {
                                            let _ = queue_status_tx.send(msg).await;
                                        }
                                        braid::protocol::ControlMessage::FileComplete { .. } => {
                                            if let Some(tx) = file_complete_tx.take() {
                                                let _ = tx.send(msg);
                                            }
                                            break;
                                        }
                                        braid::protocol::ControlMessage::Ack { .. }
                                        | braid::protocol::ControlMessage::ChannelOpened { .. } => {
                                            let _ = reconnect_resp_tx.send(msg).await;
                                        }
                                        _ => {
                                            trace!("ignored non-QueueStatus control message: {msg:?}");
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("control recv error: {e}");
                                    break;
                                }
                            }
                        }
                        msg = control_out_rx.recv() => {
                            match msg {
                                Some(msg) => {
                                    if let Err(e) = client.send_message(&msg).await {
                                        warn!("failed to send control message: {e}");
                                        break;
                                    }
                                }
                                None => {
                                    if is_file_mode {
                                        send_closed = true;
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    match client.recv_message().await {
                        Ok(msg) => match &msg {
                            braid::protocol::ControlMessage::FileComplete { .. } => {
                                if let Some(tx) = file_complete_tx.take() {
                                    let _ = tx.send(msg);
                                }
                                break;
                            }
                            braid::protocol::ControlMessage::QueueStatus { .. } => {
                                let _ = queue_status_tx.send(msg).await;
                            }
                            braid::protocol::ControlMessage::Ack { .. }
                            | braid::protocol::ControlMessage::ChannelOpened { .. } => {
                                let _ = reconnect_resp_tx.send(msg).await;
                            }
                            _ => {
                                trace!("ignored control message in file-mode wait: {msg:?}");
                            }
                        },
                        Err(ControlError::Timeout) => {
                            trace!("control recv timeout while waiting for FileComplete, retrying");
                        }
                        Err(e) => {
                            warn!("control recv error while waiting for FileComplete: {e}");
                            break;
                        }
                    }
                }
            }
            info!("control forwarding task finished");
        });

        // ─── Step 10: Wait for completion or reconnect ───────────────────
        let splitter_timeout = tokio::time::sleep(Duration::from_secs(5));
        tokio::select! {
            _ = splitter_handle => {}
            _ = splitter_timeout => {
                info!("splitter await timed out (expected during shutdown)");
            }
        }
        let _ = qm_handle.await;

        // Check if QM exited due to all workers being down.
        let reconnect_last_chunk = match qm_exit_rx.try_recv() {
            Ok(QmExitReason::AllWorkersDown { last_chunk_id }) => Some(last_chunk_id),
            _ => None,
        };

        if let Some(last_chunk_id) = reconnect_last_chunk {
            info!("all workers down, initiating reconnect from chunk_id={}", last_chunk_id);

            // Send Reconnect via control channel.
            let reconnect_msg = ControlMessage::Reconnect {
                last_sequence_number: last_chunk_id as u64,
            };
            control_out_tx
                .send(reconnect_msg)
                .await
                .map_err(|e| format!("failed to send Reconnect: {e}"))?;

            // Wait for Ack with 10s timeout.
            let ack = tokio::time::timeout(
                Duration::from_secs(10),
                reconnect_resp_rx.recv(),
            )
            .await
            .map_err(|_| "timeout waiting for Ack after Reconnect")?
            .ok_or("reconnect_resp channel closed while waiting for Ack")?;

            match &ack {
                ControlMessage::Ack { sequence_number } => {
                    info!("received Ack for sequence_number={}", sequence_number);
                }
                ControlMessage::Nack { sequence_number, reason } => {
                    return Err(format!(
                        "receiver rejected reconnect: Nack(sequence_number={}, reason={})",
                        sequence_number, reason
                    ).into());
                }
                other => {
                    return Err(format!(
                        "unexpected control message after Reconnect: {:?}",
                        other
                    ).into());
                }
            }

            // Receive ChannelOpened messages (one per new channel).
            let mut new_channels: Vec<(u16, u16)> = Vec::new();
            loop {
                match tokio::time::timeout(
                    Duration::from_secs(5),
                    reconnect_resp_rx.recv(),
                )
                .await
                {
                    Ok(Some(ControlMessage::ChannelOpened { channel_id, port })) => {
                        info!("received ChannelOpened: channel_id={}, port={}", channel_id, port);
                        new_channels.push((channel_id, port));
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            if new_channels.is_empty() {
                return Err("reconnect failed: no new channels received from receiver".into());
            }

            info!(
                "reconnect complete: {} new channels, resuming from chunk_id={}",
                new_channels.len(),
                last_chunk_id
            );

            // Create new UDP send workers.
            let mut new_worker_sockets = Vec::with_capacity(new_channels.len());
            for (_channel_id, port) in &new_channels {
                let dest = SocketAddr::new(self.destination.ip(), *port as u16);
                let worker: WorkerKind = if self.no_batch {
                    WorkerKind::Single(UdpSendWorker::new(
                        0,
                        dest,
                        DEFAULT_SO_SNDBUF,
                        DEFAULT_SEND_TIMEOUT,
                        Arc::new(UdpSendWorkerStats::default()),
                        None, // health reporting not wired on reconnect path
                    ))
                } else {
                    WorkerKind::Batch(BatchSendWorker::new(
                        0,
                        dest,
                        DEFAULT_SO_SNDBUF,
                        DEFAULT_SEND_TIMEOUT,
                        self.batch_size,
                        self.batch_usec,
                        Arc::new(UdpSendWorkerStats::default()),
                        None,
                    ))
                };
                let socket = worker.bind().await?;
                new_worker_sockets.push((worker, socket));
            }

            // Build new QueueManager.
            let (bp_tx_new, mut bp_rx_new) = mpsc::channel::<bool>(16);
            let mut qm_builder = QueueManagerBuilder::new(new_channels.len())
                .high_watermark(DEFAULT_HIGH_WATERMARK)
                .channel_capacity(DEFAULT_CHANNEL_CAPACITY)
                .backpressure_tx(bp_tx_new);
            if self.max_rate > 0 {
                qm_builder = qm_builder.max_rate(self.max_rate);
            }
            let (mut new_queue_manager, worker_receivers) = qm_builder.build();
            new_queue_manager.set_progress_bytes(progress_bytes_tx);

            let mut new_worker_handles = Vec::with_capacity(new_channels.len());
            for (i, ((worker, socket), (rx, _stats))) in
                new_worker_sockets.into_iter().zip(worker_receivers).enumerate()
            {
                let local_port = socket.local_addr().map(|a| a.port()).unwrap_or(0);
                let handle = tokio::spawn(async move {
                    info!("reconnect UDP send worker {} started on port {}", i, local_port);
                    worker.run(socket, rx).await;
                    info!("reconnect UDP send worker {} finished", i);
                });
                new_worker_handles.push(handle);
            }

            // Restart the splitter (stdin or file).
            let (new_fragment_tx, new_fragment_rx) =
                mpsc::channel::<Vec<Bytes>>(DEFAULT_CHANNEL_CAPACITY);
            let reconnect_effective_chunk_size = if self.chunk_size > 0 {
                self.chunk_size
            } else {
                braid::adaptive::chunk_size::DEFAULT_INITIAL_CHUNK
            };
            let reconnect_max_chunk_size = reconnect_effective_chunk_size.max(braid::adaptive::chunk_size::MAX_CHUNK);
            let reconnect_pool_buf_size = reconnect_max_chunk_size.max(self.mtu);
            let reconnect_pool = BufferPool::new(4, reconnect_pool_buf_size);
            let reconnect_chunk_size = Arc::new(std::sync::atomic::AtomicUsize::new(reconnect_effective_chunk_size));
            let splitter = ChunkSplitter::new(
                reconnect_chunk_size,
                reconnect_max_chunk_size,
                self.mtu,
                reconnect_pool,
            );
            let (splitter_pause_tx_new, splitter_pause_rx_new) = mpsc::channel::<bool>(16);
            let new_splitter_handle = if let Some(path) = &input_path {
                let file = tokio::fs::File::open(path).await
                    .map_err(|e| format!("failed to reopen input file for reconnect: {e}"))?;
                tokio::spawn(async move {
                    info!("reconnect chunk splitter started (file input)");
                    if let Err(e) = splitter
                        .run(new_fragment_tx, Some(splitter_pause_rx_new), file)
                        .await
                    {
                        error!("reconnect chunk splitter error: {}", e);
                    }
                    info!("reconnect chunk splitter finished");
                })
            } else {
                tokio::spawn(async move {
                    info!("reconnect chunk splitter started (stdin)");
                    if let Err(e) = splitter
                        .run(new_fragment_tx, Some(splitter_pause_rx_new), tokio::io::stdin())
                        .await
                    {
                        error!("reconnect chunk splitter error: {}", e);
                    }
                    info!("reconnect chunk splitter finished");
                })
            };

            let new_qm_shutdown = shutdown.clone();
            let new_qm_handle = tokio::spawn(async move {
                info!("reconnect queue manager dispatch loop started");
                let mut rx = new_fragment_rx;
                while let Some(batch) = rx.recv().await {
                    if new_qm_shutdown.is_shutting_down() {
                        break;
                    }
                    if let Err(e) = new_queue_manager.dispatch_batch(batch) {
                        error!("reconnect queue manager dispatch_batch error: {}", e);
                        break;
                    }
                }
                info!("reconnect queue manager dispatch loop finished");
            });

            let new_bp_shutdown = shutdown.clone();
            let new_bp_handle = tokio::spawn(async move {
                info!("reconnect backpressure handler started");
                while let Some(paused) = bp_rx_new.recv().await {
                    if new_bp_shutdown.is_shutting_down() {
                        break;
                    }
                    if paused {
                        info!("reconnect backpressure: pausing splitter");
                        let _ = splitter_pause_tx_new.send(true).await;
                    } else {
                        info!("reconnect backpressure: resuming splitter");
                        let _ = splitter_pause_tx_new.send(false).await;
                    }
                }
                info!("reconnect backpressure handler finished");
            });

            let new_splitter_timeout = tokio::time::sleep(Duration::from_secs(5));
            tokio::select! {
                _ = new_splitter_handle => {}
                _ = new_splitter_timeout => {
                    info!("reconnect splitter await timed out");
                }
            }
            shutdown.initiate();
            let _ = new_qm_handle.await;
            let _ = new_bp_handle.await;
            for handle in new_worker_handles {
                let _ = handle.await;
            }
        } else {
            shutdown.initiate();
        }

        // Flow handle may block on queue_status_rx if no status messages arrive.
        let flow_timeout = tokio::time::sleep(Duration::from_secs(2));
        tokio::select! {
            _ = flow_handle => {}
            _ = flow_timeout => {
                info!("flow handle await timed out (expected during shutdown)");
            }
        }

        // Control forwarding task will exit when TCP breaks or channel closes.
        let _ = control_recv_handle.await;

        let _ = progress_handle.await;
        let _ = bp_handle.await;
        for handle in worker_handles {
            let _ = handle.await;
        }

        // In file mode, wait for the receiver to acknowledge file completion
        // and verify the transfer's integrity.
        let file_mode_result: Result<(), String> = if self.mode == Mode::File {
            match tokio::time::timeout(Duration::from_secs(30), file_complete_rx).await {
                Err(_) => {
                    error!("receiver did not acknowledge file completion within 30s");
                    Err("receiver did not acknowledge file completion".to_string())
                }
                Ok(Err(_)) => {
                    error!("file complete channel closed before receiver acknowledged");
                    Err("file complete channel closed unexpectedly".to_string())
                }
                Ok(Ok(braid::protocol::ControlMessage::FileComplete {
                    success,
                    expected_hash,
                    computed_hash,
                })) => {
                    if success {
                        info!(
                            "file transfer verified: expected crc32c={:08x}, computed crc32c={:08x}",
                            expected_hash, computed_hash
                        );
                        eprintln!("file transfer verified (crc32c={:08x})", computed_hash);
                        Ok(())
                    } else {
                        let msg = format!(
                            "receiver reported file integrity failure: expected crc32c={:08x}, computed crc32c={:08x}",
                            expected_hash, computed_hash
                        );
                        error!("{msg}");
                        Err(msg)
                    }
                }
                Ok(Ok(other)) => {
                    let msg = format!(
                        "unexpected control message while waiting for FileComplete: {other:?}"
                    );
                    error!("{msg}");
                    Err(msg)
                }
            }
        } else {
            Ok(())
        };

        if signal_received.load(Ordering::SeqCst) {
            return Err("shutdown initiated by signal".into());
        }

        file_mode_result.map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use braid::adaptive::chunk_size::ChunkSizeAdaptor;
    use std::sync::atomic::Ordering;

    #[test]
    fn creates_braid_send_with_defaults() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let send = BraidSend::new(addr, 0, 0, 1500, 0, ProgressVerbosity::Normal);
        assert_eq!(send.destination.to_string(), "127.0.0.1:9000");
        assert_eq!(send.chunk_size, 0);
        assert_eq!(send.channels, 0);
        assert_eq!(send.mtu, 1500);
    }

    #[test]
    fn creates_braid_send_with_explicit_values() {
        let addr: SocketAddr = "[::1]:8080".parse().unwrap();
        let send = BraidSend::new(addr, 4096, 8, 9000, 125000000, ProgressVerbosity::Quiet);
        assert_eq!(send.destination.to_string(), "[::1]:8080");
        assert_eq!(send.chunk_size, 4096);
        assert_eq!(send.channels, 8);
        assert_eq!(send.mtu, 9000);
        assert_eq!(send.max_rate, 125000000);
    }

    #[test]
    fn creates_braid_send_verbose() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let send = BraidSend::new(addr, 0, 0, 1500, 0, ProgressVerbosity::Verbose);
        assert_eq!(send.verbosity, ProgressVerbosity::Verbose);
    }

    #[test]
    fn shutdown_manager_initial_state() {
        let mgr = ShutdownManager::new();
        assert!(!mgr.is_shutting_down());
        assert!(mgr.check().is_ok());
    }

    #[test]
    fn shutdown_manager_initiate_sets_flag() {
        let mgr = ShutdownManager::new();
        mgr.initiate();
        assert!(mgr.is_shutting_down());
        assert!(mgr.check().is_err());
    }

    #[test]
    fn shutdown_manager_is_idempotent() {
        let mgr = ShutdownManager::new();
        mgr.initiate();
        mgr.initiate();
        assert!(mgr.is_shutting_down());
    }

    #[test]
    fn shutdown_manager_clone_reflects_state() {
        let mgr1 = ShutdownManager::new();
        let mgr2 = mgr1.clone();
        mgr1.initiate();
        assert!(mgr2.is_shutting_down());
    }

    #[test]
    fn chunk_size_adaptor_defaults() {
        let adaptor = ChunkSizeAdaptor::new_default();
        assert_eq!(
            adaptor.current_chunk_size(),
            braid::adaptive::chunk_size::DEFAULT_INITIAL_CHUNK
        );
    }

    #[test]
    fn chunk_size_adaptor_fixed_mode() {
        let mut adaptor = ChunkSizeAdaptor::new_default();
        adaptor.set_fixed(8192);
        assert!(adaptor.is_disabled());
        assert_eq!(adaptor.current_chunk_size(), 8192);
    }

    #[test]
    fn chunk_size_adaptor_adaptive_mode() {
        let adaptor = ChunkSizeAdaptor::new(4096, 1024, 1048576, 10);
        assert!(!adaptor.is_disabled());
        assert_eq!(adaptor.current_chunk_size(), 4096);
    }

    #[test]
    fn channel_count_adaptor_defaults() {
        let adaptor = braid::adaptive::channels::ChannelCountAdaptor::new_default();
        assert_eq!(
            adaptor.current_channel_count(),
            braid::adaptive::channels::DEFAULT_INITIAL_CHANNELS
        );
    }

    #[test]
    fn channel_count_adaptor_fixed_mode() {
        let mut adaptor = braid::adaptive::channels::ChannelCountAdaptor::new_default();
        adaptor.set_fixed(8);
        assert!(adaptor.is_disabled());
        assert_eq!(adaptor.current_channel_count(), 8);
    }

    #[test]
    fn queue_manager_creates_workers() {
        let (mgr, receivers) = QueueManagerBuilder::new(4).channel_capacity(64).build();
        assert_eq!(mgr.worker_count(), 4);
        assert_eq!(receivers.len(), 4);
        assert_eq!(mgr.active_worker_count(), 4);
    }

    #[test]
    fn queue_manager_dispatch_to_worker() {
        let (mut mgr, _receivers) = QueueManagerBuilder::new(2).channel_capacity(64).build();
        let result = mgr.dispatch(Bytes::from(vec![0u8; 100]));
        assert!(result.is_ok());
        assert_eq!(mgr.stats().fragments_dispatched.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn queue_manager_all_workers_down() {
        let (mut mgr, _receivers) = QueueManagerBuilder::new(2).channel_capacity(64).build();
        mgr.mark_worker_failed(0);
        mgr.mark_worker_failed(1);
        assert!(mgr.all_workers_down());
        let result = mgr.dispatch(Bytes::from(vec![0u8; 100]));
        assert!(result.is_err());
    }
}