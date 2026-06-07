use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

use braid::control::client::ControlClient;
use braid::control::negotiation::{negotiate, NegotiationConfig};
use braid::flow::SenderReactor;
use braid::progress::reporter::{ProgressReporter, ProgressVerbosity};
use braid::sender::queue::QueueManagerBuilder;
use braid::sender::splitter::ChunkSplitter;
use braid::sender::worker::{UdpSendWorker, UdpSendWorkerStats};
use braid::shutdown::manager::ShutdownManager;

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
}

impl BraidSend {
    pub fn new(
        destination: SocketAddr,
        chunk_size: usize,
        channels: usize,
        mtu: usize,
        max_rate: u64,
        verbosity: ProgressVerbosity,
    ) -> Self {
        Self {
            destination,
            chunk_size,
            channels,
            mtu,
            max_rate,
            verbosity,
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
        let mut client = ControlClient::connect(self.destination)
            .await
            .map_err(|e| format!("control connection failed: {e}"))?;

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
        };
        info!("negotiating {} channels with receiver", channel_count);
        let result = negotiate(&mut client, config)
            .await
            .map_err(|e| format!("negotiation failed: {e}"))?;
        let channels = result.channels;
        info!("negotiated {} channels", channels.len());

        // ─── Step 4: Create UDP send workers ─────────────────────────────
        let mut worker_sockets = Vec::with_capacity(channels.len());
        for ch in &channels {
            let worker = UdpSendWorker::new(
                0,
                SocketAddr::new(self.destination.ip(), ch.port),
                DEFAULT_SO_SNDBUF,
                DEFAULT_SEND_TIMEOUT,
                Arc::new(UdpSendWorkerStats::default()),
            );
            let socket = worker.bind().await?;
            worker_sockets.push((worker, socket));
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
        let (fragment_tx, fragment_rx) = mpsc::channel::<Vec<Vec<u8>>>(DEFAULT_CHANNEL_CAPACITY);

        // ─── Step 8: Create progress reporter ────────────────────────────
        let mut progress = ProgressReporter::new(DEFAULT_PROGRESS_INTERVAL, self.verbosity);
        queue_manager.set_progress_bytes(progress.bytes_tx());

        // ─── Step 9: Spawn pipeline tasks ────────────────────────────────
        // Spawn ChunkSplitter
        let splitter = ChunkSplitter::new(
            if self.chunk_size > 0 {
                self.chunk_size
            } else {
                braid::adaptive::chunk_size::DEFAULT_INITIAL_CHUNK
            },
            self.mtu,
        );
        let (splitter_pause_tx, splitter_pause_rx) = mpsc::channel::<bool>(16);
        let splitter_handle = tokio::spawn(async move {
            info!("chunk splitter started");
            if let Err(e) = splitter.run(fragment_tx, Some(splitter_pause_rx), tokio::io::stdin()).await {
                error!("chunk splitter error: {}", e);
            }
            info!("chunk splitter finished");
        });

        // Spawn QueueManager dispatch loop
        let qm_shutdown = shutdown.clone();
        let qm_handle = tokio::spawn(async move {
            info!("queue manager dispatch loop started");
            let mut rx = fragment_rx;
            while let Some(batch) = rx.recv().await {
                if qm_shutdown.is_shutting_down() {
                    break;
                }
                // Dispatch the entire batch to a single worker
                if let Err(e) = queue_manager.dispatch_batch(batch) {
                    error!("queue manager dispatch_batch error: {}", e);
                    break;
                }
            }
            info!("queue manager dispatch loop finished");
        });

        // Spawn FlowController (SenderReactor)
        let (queue_status_tx, queue_status_rx) =
            mpsc::channel::<braid::protocol::ControlMessage>(16);
        let (_control_out_tx, mut control_out_rx) =
            mpsc::channel::<braid::protocol::ControlMessage>(16);
        let mut sender_reactor = SenderReactor::new(1024, queue_status_rx);
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

        // Spawn control message forwarding task (bidirectional: TCP ↔ SenderReactor)
        let mut client = client;
        let control_recv_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = client.recv_message() => {
                        match result {
                            Ok(msg) => {
                                if matches!(msg, braid::protocol::ControlMessage::QueueStatus { .. }) {
                                    let _ = queue_status_tx.send(msg).await;
                                } else {
                                    trace!("ignored non-QueueStatus control message: {msg:?}");
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
                            None => break,
                        }
                    }
                }
            }
            info!("control forwarding task finished");
        });

        // ─── Step 10: Wait for completion or shutdown ────────────────────
        // Wait for the splitter to finish (stdin EOF or error). The signal
        // handler calls shutdown.initiate() on SIGINT, which causes UDP workers
        // to exit, which closes the fragment channel, which causes the splitter
        // to error with "channel closed" and exit.
        // Use a timeout to prevent hanging if stdin doesn't close.
        let splitter_timeout = tokio::time::sleep(Duration::from_secs(5));
        tokio::select! {
            _ = splitter_handle => {}
            _ = splitter_timeout => {
                info!("splitter await timed out (expected during shutdown)");
            }
        }
        let _ = qm_handle.await;

        // Signal shutdown so all shutdown-aware tasks (progress reporter,
        // workers, backpressure handler) can exit cleanly on normal completion.
        shutdown.initiate();

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

        if signal_received.load(Ordering::SeqCst) {
            Err("shutdown initiated by signal".into())
        } else {
            Ok(())
        }
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
        let result = mgr.dispatch(vec![0u8; 100]);
        assert!(result.is_ok());
        assert_eq!(mgr.stats().fragments_dispatched.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn queue_manager_all_workers_down() {
        let (mut mgr, _receivers) = QueueManagerBuilder::new(2).channel_capacity(64).build();
        mgr.mark_worker_failed(0);
        mgr.mark_worker_failed(1);
        assert!(mgr.all_workers_down());
        let result = mgr.dispatch(vec![0u8; 100]);
        assert!(result.is_err());
    }
}
