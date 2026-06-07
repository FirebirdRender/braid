use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

use braid::buffer::pool::BufferPool;
use braid::control::negotiation::accept_negotiation;
use braid::control::server::ControlServer;
use braid::flow::ReceiverMonitor;
use braid::progress::reporter::{ProgressReporter, ProgressVerbosity};
use braid::protocol::crc::verify_fragment_crc;
use braid::protocol::headers::FragmentHeader;
use braid::receiver::commit::{CommitGate, CommitGateInput};
use braid::receiver::ordering::ChunkOrderer;
use braid::receiver::reassembly::FragmentReassembler;
use braid::shutdown::manager::ShutdownManager;

const DEFAULT_CHUNK_TIMEOUT_SECS: u64 = 60;
/// Large channel capacity to absorb burst rates between sender and receiver.
const DEFAULT_CHANNEL_CAPACITY: usize = 32_768;
const DEFAULT_PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_millis(1000);

pub struct BraidReceive {
    bind: SocketAddr,
    output: Option<PathBuf>,
    buffer_size: usize,
    mtu: usize,
    verbosity: ProgressVerbosity,
}

impl BraidReceive {
    pub fn new(
        bind: SocketAddr,
        output: Option<PathBuf>,
        buffer_size: usize,
        mtu: usize,
        verbosity: ProgressVerbosity,
    ) -> Self {
        Self {
            bind,
            output,
            buffer_size,
            mtu,
            verbosity,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let shutdown = ShutdownManager::new();
        let shutdown_signal = shutdown.clone();
        tokio::spawn(async move {
            shutdown_signal.await_signals().await;
        });

        // Use the user-specified buffer size. Default to 64MB if not specified.
        let max_inflight = if self.buffer_size > 0 {
            self.buffer_size
        } else {
            64 * 1024 * 1024
        };
        let buffer_pool_size = (max_inflight / 65536).max(16);
        let buffer_pool = BufferPool::new(buffer_pool_size, 65536);

        info!("binding control server on {}", self.bind);
        let server = ControlServer::bind(self.bind)
            .await
            .map_err(|e| format!("control server bind failed: {e}"))?;
        let control_addr = server
            .local_addr()
            .map_err(|e| format!("get local addr: {e}"))?;
        info!("control server listening on {}", control_addr);

        let mut conn = server
            .accept()
            .await
            .map_err(|e| format!("control accept failed: {e}"))?;
        info!("sender connected on control channel");

        let (_config, udp_sockets, result) = accept_negotiation(&mut conn)
            .await
            .map_err(|e| format!("negotiation failed: {e}"))?;
        let channels = result.channels;
        info!("negotiated {} channels", channels.len());

        let (reassembly_tx, reassembly_rx) = mpsc::channel::<Vec<u8>>(DEFAULT_CHANNEL_CAPACITY);
        let (orderer_tx, orderer_rx) = mpsc::channel::<CommitGateInput>(DEFAULT_CHANNEL_CAPACITY);
        let (control_tx, mut control_rx) =
            mpsc::channel::<braid::protocol::ControlMessage>(DEFAULT_CHANNEL_CAPACITY);
        let (queue_status_tx, mut queue_status_rx) =
            mpsc::channel::<braid::protocol::ControlMessage>(DEFAULT_CHANNEL_CAPACITY);

        let mut orderer = ChunkOrderer::new(orderer_tx, 0);
        let mut commit_gate = if let Some(ref path) = self.output {
            info!("writing output to file: {}", path.display());
            CommitGate::with_file(orderer_rx, path)
                .await
                .map_err(|e| format!("commit gate file create: {e}"))?
        } else {
            CommitGate::new(orderer_rx)
        };

        let mut monitor = ReceiverMonitor::new(
            buffer_pool,
            max_inflight,
            control_tx,
            DEFAULT_STATUS_INTERVAL,
        );

        let mut progress = ProgressReporter::new(DEFAULT_PROGRESS_INTERVAL, self.verbosity);
        let progress_counter = progress.bytes_tx();
        progress.set_channel_count(channels.len());

        commit_gate.set_progress_bytes(progress_counter);

        let num_workers = udp_sockets.len();

        // N fragment channels: each UDP worker routes fragments to the correct
        // reassembler by chunk_id % N. This distributes CRC+reassembly across
        // cores regardless of which worker receives which packet.
        let mut fragment_txs: Vec<mpsc::Sender<Vec<u8>>> = Vec::with_capacity(num_workers);
        let mut fragment_rxs: Vec<mpsc::Receiver<Vec<u8>>> = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (tx, rx) = mpsc::channel::<Vec<u8>>(DEFAULT_CHANNEL_CAPACITY);
            fragment_txs.push(tx);
            fragment_rxs.push(rx);
        }

        let mut worker_handles = Vec::with_capacity(udp_sockets.len());
        for (i, socket) in udp_sockets.into_iter().enumerate() {
            let sd = shutdown.clone();
            let txs = fragment_txs.clone();
            let mtu = self.mtu;
            let nw = num_workers;
            let handle = tokio::spawn(async move {
                info!("UDP receive worker {} started", i);
                let mut buf = vec![0u8; mtu];
                let mut consecutive_timeouts = 0;
                const MAX_CONSECUTIVE_TIMEOUTS: u32 = 10;
                loop {
                    if sd.is_shutting_down() {
                        break;
                    }

                    let result =
                        tokio::time::timeout(Duration::from_secs(1), socket.recv_from(&mut buf))
                            .await;

                    match result {
                        Ok(Ok((n, _src))) => {
                            consecutive_timeouts = 0;
                            if n < FragmentHeader::LEN {
                                warn!("short datagram on worker {}, ignoring", i);
                                continue;
                            }
                            if !verify_fragment_crc(
                                &buf[FragmentHeader::LEN..n],
                                u32::from_be_bytes([buf[10], buf[11], buf[12], buf[13]]),
                            ) {
                                warn!("fragment CRC mismatch on worker {}, dropping", i);
                                continue;
                            }
                            let chunk_id = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
                            let shard = (chunk_id as usize) % nw;
                            let fragment = buf[..n].to_vec();
                            if txs[shard].send(fragment).await.is_err() {
                                info!("fragment channel {} closed, stopping worker {}", shard, i);
                                break;
                            }
                        }
                        Ok(Err(e)) => {
                            if sd.is_shutting_down() {
                                break;
                            }
                            warn!("UDP recv error on worker {}: {}", i, e);
                            break;
                        }
                        Err(_) => {
                            consecutive_timeouts += 1;
                            if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS {
                                info!(
                                    "UDP receive worker {} exiting after {}s of inactivity",
                                    i, MAX_CONSECUTIVE_TIMEOUTS
                                );
                                break;
                            }
                            continue;
                        }
                    }
                }
                info!("UDP receive worker {} finished", i);
            });
            worker_handles.push(handle);
        }
        // Drop all sender halves so reassembler tasks see channel close when
        // no workers remain holding a clone.
        drop(fragment_txs);

        // Start N independent reassemblers, one per shard.
        // Each processes fragments for chunk_id % shard_index chunks.
        // All emit assembled payloads to the shared orderer channel.
        let mut reassembler_handles = Vec::with_capacity(num_workers);
        for (i, rx) in fragment_rxs.into_iter().enumerate() {
            let tx = reassembly_tx.clone();
            let mut reassembler =
                FragmentReassembler::new(tx, max_inflight, DEFAULT_CHUNK_TIMEOUT_SECS);
            let handle = tokio::spawn(async move {
                info!("fragment reassembler {} started", i);
                let mut rx = rx;
                loop {
                    match rx.recv().await {
                        Some(data) => {
                            if let Err(e) = reassembler.add_fragment(data).await {
                                error!("reassembler {} error: {}", i, e);
                            }
                        }
                        None => {
                            info!("fragment channel {} closed, reassembler {} stopping", i, i);
                            break;
                        }
                    }
                }
                info!("fragment reassembler {} finished", i);
            });
            reassembler_handles.push(handle);
        }

        let orderer_handle = tokio::spawn(async move {
            info!("chunk orderer started");
            orderer
                .run(reassembly_rx, Some(Duration::from_secs(5)))
                .await;
            info!("chunk orderer finished");
        });

        let commit_handle = tokio::spawn(async move {
            info!("commit gate started");
            commit_gate.run().await;
            info!("commit gate finished");
        });

        let (monitor_cancel_tx, monitor_cancel_rx) = mpsc::channel::<()>(1);
        let monitor_handle = tokio::spawn(async move {
            info!("flow receiver monitor started");
            monitor.run(monitor_cancel_rx).await;
            info!("flow receiver monitor finished");
        });

        // Bridge tunnel: control_rx → queue_status_tx via owned channel
        let _control_bridge_handle = tokio::spawn(async move {
            while let Some(msg) = control_rx.recv().await {
                if queue_status_tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // Forward: bidirectional TCP forwarding over the control connection.
        // Sends QueueStatus messages from the monitor to the sender, and
        // receives any incoming control messages from the sender.
        let _control_forward_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = queue_status_rx.recv() => {
                        match msg {
                            Some(msg) => {
                                if let Err(e) = conn.send_message(&msg).await {
                                    warn!("failed to send control message: {e}");
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    result = conn.recv_message() => {
                        match result {
                            Ok(msg) => {
                                trace!("received control message: {msg:?}");
                            }
                            Err(e) => {
                                warn!("control recv error: {e}");
                                break;
                            }
                        }
                    }
                }
            }
        });

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

        // Wait for all reassemblers to finish (EOS from sender) or timeout.
        // We use a polling loop with shutdown check to avoid depending on the
        // signal handler task being polled by a saturated runtime.
        loop {
            let mut all_done = true;
            for handle in &reassembler_handles {
                if !handle.is_finished() {
                    all_done = false;
                    break;
                }
            }
            if all_done {
                info!("all reassemblers completed");
                break;
            }

            if shutdown.is_shutting_down() {
                info!("reassemblers interrupted by shutdown signal");
                break;
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Await orderer and commit gate with generous timeouts.
        // The orderer may be draining remaining chunks from its heap, and the
        // commit gate must flush its BufWriter before we exit. If we cancel the
        // commit gate task (via timeout), the BufWriter's flush() in run()'s
        // cleanup never executes, causing data loss.
        //
        // Strategy: wait for the orderer to finish (it drains its heap and sends
        // all remaining chunks to the commit gate), then the commit gate's input
        // channel closes naturally, causing run() to exit normally and flush.
        let orderer_timeout = tokio::time::sleep(Duration::from_secs(30));
        tokio::select! {
            _ = orderer_handle => {
                info!("orderer completed normally");
            }
            _ = orderer_timeout => {
                info!("orderer await timed out (expected during shutdown)");
            }
        }

        // After the orderer finishes (or times out), the commit gate's input
        // channel should be closed. Wait generously for it to flush and exit.
        let commit_timeout = tokio::time::sleep(Duration::from_secs(30));
        tokio::select! {
            _ = commit_handle => {
                info!("commit gate completed normally");
            }
            _ = commit_timeout => {
                info!("commit gate await timed out (expected during shutdown)");
            }
        }

        // Cancel the flow monitor so it can finish.
        let _ = monitor_cancel_tx.try_send(());
        let _ = monitor_handle.await;
        let _ = progress_handle.await;
        for handle in worker_handles {
            let _ = handle.await;
        }

        info!("braid receive completed successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use braid::progress::reporter::ProgressVerbosity;
    use std::net::SocketAddr;

    #[test]
    fn creates_braid_receive_with_defaults() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let recv = BraidReceive::new(addr, None, 65536, 1500, ProgressVerbosity::Normal);
        assert_eq!(recv.bind.to_string(), "127.0.0.1:9000");
        assert!(recv.output.is_none());
        assert_eq!(recv.buffer_size, 65536);
        assert_eq!(recv.mtu, 1500);
    }

    #[test]
    fn creates_braid_receive_with_output() {
        let addr: SocketAddr = "[::1]:8080".parse().unwrap();
        let path = PathBuf::from("output.bin");
        let recv = BraidReceive::new(
            addr,
            Some(path.clone()),
            8192,
            9000,
            ProgressVerbosity::Quiet,
        );
        assert_eq!(recv.bind.to_string(), "[::1]:8080");
        assert_eq!(recv.output, Some(path));
        assert_eq!(recv.buffer_size, 8192);
        assert_eq!(recv.mtu, 9000);
    }

    #[test]
    fn creates_braid_receive_verbose() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let recv = BraidReceive::new(addr, None, 0, 1500, ProgressVerbosity::Verbose);
        assert_eq!(recv.verbosity, ProgressVerbosity::Verbose);
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
    fn buffer_pool_creation() {
        let pool = BufferPool::new(10, 1024);
        let guard = pool.get_buffer();
        assert_eq!(guard.len(), 1024);
    }

    #[test]
    fn buffer_pool_reuses_buffers() {
        let pool = BufferPool::new(1, 8);
        {
            let mut guard = pool.get_buffer();
            guard[0] = 42;
        }
        let guard = pool.get_buffer();
        // After return to pool, buffer is cleared and resized
        assert_eq!(guard.len(), 8);
        assert_eq!(guard[0], 0);
    }

    #[test]
    fn fragment_reassembler_creation() {
        let (tx, _rx) = mpsc::channel::<Vec<u8>>(16);
        let reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60);
        assert_eq!(reassembler.in_flight_count(), 0);
        assert_eq!(reassembler.inflight_bytes(), 0);
    }

    #[test]
    fn chunk_orderer_creation() {
        let (tx, _rx) = mpsc::channel::<CommitGateInput>(16);
        let orderer = ChunkOrderer::new(tx, 0);
        let stats = orderer.stats();
        assert_eq!(stats.chunks_received, 0);
        assert_eq!(stats.chunks_emitted, 0);
        assert_eq!(stats.chunks_waiting, 0);
    }

    #[test]
    fn commit_gate_creation() {
        let (_tx, rx) = mpsc::channel::<CommitGateInput>(16);
        let gate = CommitGate::new(rx);
        let stats = gate.stats();
        assert_eq!(stats.chunks_committed, 0);
        assert_eq!(stats.bytes_written, 0);
    }

    #[test]
    fn progress_reporter_creation() {
        let reporter = ProgressReporter::new(Duration::from_secs(1), ProgressVerbosity::Normal);
        let snapshot = reporter.snapshot(Duration::from_secs(0));
        assert_eq!(snapshot.total_bytes, 0);
    }

    #[test]
    fn receiver_monitor_creation() {
        let pool = BufferPool::new(10, 1024);
        let (control_tx, _control_rx) = mpsc::channel::<braid::protocol::ControlMessage>(16);
        let monitor = ReceiverMonitor::new(pool, 10, control_tx, Duration::from_secs(1));
        assert_eq!(
            monitor.controller().level(),
            braid::flow::FullnessLevel::Green
        );
    }
}
