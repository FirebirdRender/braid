use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

use braid::buffer::pool::BufferPool;
use braid::control::negotiation::accept_negotiation;
use braid::control::server::ControlServer;
use braid::file_mode::{output, receiver::FileModeReceiver, sanitize, FileMetadata};
use braid::flow::ReceiverMonitor;
use braid::progress::reporter::{ProgressReporter, ProgressVerbosity};
use braid::protocol::crc::verify_fragment_crc;
use braid::protocol::headers::FragmentHeader;
use braid::protocol::ControlMessage;
use braid::receiver::commit::{CommitGate, CommitGateInput};
use braid::receiver::ordering::ChunkOrderer;
use braid::receiver::reassembly::FragmentReassembler;
use braid::shutdown::manager::ShutdownManager;

use super::Mode;

const DEFAULT_CHUNK_TIMEOUT_SECS: u64 = 60;
/// Large channel capacity to absorb burst rates between sender and receiver.
const DEFAULT_CHANNEL_CAPACITY: usize = 32_768;
const DEFAULT_PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_millis(1000);

pub struct BraidReceive {
    bind: SocketAddr,
    output: Option<PathBuf>,
    buffer_size: usize,
    #[allow(dead_code)]
    mtu: usize,
    verbosity: ProgressVerbosity,
    mode: Mode,
    output_override: Option<PathBuf>,
}

impl BraidReceive {
    #[allow(dead_code)]
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
            mode: Mode::Pipe,
            output_override: None,
        }
    }

    pub fn new_with_mode(
        bind: SocketAddr,
        output: Option<PathBuf>,
        buffer_size: usize,
        mtu: usize,
        verbosity: ProgressVerbosity,
        mode: Mode,
        output_override: Option<PathBuf>,
    ) -> Self {
        Self {
            bind,
            output,
            buffer_size,
            mtu,
            verbosity,
            mode,
            output_override,
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

        let (config, udp_sockets, result) = accept_negotiation(&mut conn)
            .await
            .map_err(|e| format!("negotiation failed: {e}"))?;
        let channels = result.channels;
        info!("negotiated {} channels", channels.len());
        if config.compression_lz4 || config.compression_zstd {
            info!(
                "sender supports compression: lz4={}, zstd={}",
                config.compression_lz4, config.compression_zstd
            );
        }

        // ─── File mode setup (after negotiation, before pipeline) ──────────
        let (file_mode_state, mut conn_opt) = if self.mode == Mode::File {
            let msg = conn
                .recv_message()
                .await
                .map_err(|e| format!("failed to receive FileStart: {e}"))?;
            let fs = match msg {
                ControlMessage::FileStart {
                    filename,
                    filesize,
                    file_crc32c,
                } => {
                    let sanitized = match sanitize::sanitize_filename(&filename) {
                        Ok(name) => name,
                        Err(e) => {
                            // Invalid filename from sender — report failure
                            // before aborting so the sender knows the transfer failed.
                            let fail_msg = ControlMessage::FileComplete {
                                success: false,
                                expected_hash: 0,
                                computed_hash: 0,
                            };
                            if let Err(e) = conn.send_message(&fail_msg).await {
                                warn!("failed to send FileComplete after invalid filename: {e}");
                            }
                            return Err(format!("invalid filename from sender: {e}").into());
                        }
                    };
                    FileMetadata::from_basename(sanitized, filesize, file_crc32c)
                }
                _ => {
                    return Err(format!("expected FileStart in file mode, got {msg:?}").into());
                }
            };
            info!("file mode: receiving {}", fs);
            let receiver_obj = FileModeReceiver::new(self.output_override.clone());
            let output_path = receiver_obj
                .resolve_output_path(&fs)
                .await
                .map_err(|e| format!("failed to resolve output path: {e}"))?;
            info!("writing output to file: {}", output_path.display());
            (
                Some((receiver_obj, output_path, fs.file_crc32c)),
                Some(conn),
            )
        } else {
            (None, Some(conn))
        };

        let (reassembly_tx, reassembly_rx) = mpsc::channel::<Bytes>(DEFAULT_CHANNEL_CAPACITY);
        let (orderer_tx, orderer_rx) = mpsc::channel::<CommitGateInput>(DEFAULT_CHANNEL_CAPACITY);
        let (control_tx, mut control_rx) =
            mpsc::channel::<braid::protocol::ControlMessage>(DEFAULT_CHANNEL_CAPACITY);
        let (queue_status_tx, mut queue_status_rx) =
            mpsc::channel::<braid::protocol::ControlMessage>(DEFAULT_CHANNEL_CAPACITY);

        let mut orderer = ChunkOrderer::new(orderer_tx, 0);
        let mut commit_gate = if let Some((_, ref output_path, _)) = file_mode_state {
            CommitGate::with_file(orderer_rx, &output_path.clone())
                .await
                .map_err(|e| format!("commit gate file create: {e}"))?
        } else if let Some(ref path) = self.output {
            info!("writing output to file: {}", path.display());
            CommitGate::with_file(orderer_rx, path)
                .await
                .map_err(|e| format!("commit gate file create: {e}"))?
        } else {
            CommitGate::new(orderer_rx)
        };

        // Capture stats handle BEFORE moving commit_gate into the spawned task.
        // Used after completion to detect write errors in file mode.
        let commit_gate_stats = commit_gate.stats_arc();

        let monitor_pool = buffer_pool.clone();
        let mut monitor = ReceiverMonitor::new(
            monitor_pool,
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
        let mut fragment_txs: Vec<mpsc::Sender<Bytes>> = Vec::with_capacity(num_workers);
        let mut fragment_rxs: Vec<mpsc::Receiver<Bytes>> = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (tx, rx) = mpsc::channel::<Bytes>(DEFAULT_CHANNEL_CAPACITY);
            fragment_txs.push(tx);
            fragment_rxs.push(rx);
        }

        let mut worker_handles = Vec::with_capacity(udp_sockets.len());
        for (i, socket) in udp_sockets.into_iter().enumerate() {
            let sd = shutdown.clone();
            let txs = fragment_txs.clone();
            let pool = buffer_pool.clone();
            let nw = num_workers;
            let handle = tokio::spawn(async move {
                info!("UDP receive worker {} started", i);
                let mut consecutive_timeouts = 0;
                const MAX_CONSECUTIVE_TIMEOUTS: u32 = 10;
                loop {
                    if sd.is_shutting_down() {
                        break;
                    }

                    let mut pool_buf = pool.acquire().await;

                    let result = tokio::time::timeout(
                        Duration::from_secs(1),
                        socket.recv_from(&mut *pool_buf.buffer),
                    )
                    .await;

                    match result {
                        Ok(Ok((n, _src))) => {
                            consecutive_timeouts = 0;
                            if n < FragmentHeader::LEN {
                                warn!("short datagram on worker {}, ignoring", i);
                                continue;
                            }
                            if !verify_fragment_crc(
                                &pool_buf.buffer[FragmentHeader::LEN..n],
                                u32::from_be_bytes([
                                    pool_buf.buffer[10],
                                    pool_buf.buffer[11],
                                    pool_buf.buffer[12],
                                    pool_buf.buffer[13],
                                ]),
                            ) {
                                warn!("fragment CRC mismatch on worker {}, dropping", i);
                                continue;
                            }
                            let chunk_id = u32::from_be_bytes([
                                pool_buf.buffer[0],
                                pool_buf.buffer[1],
                                pool_buf.buffer[2],
                                pool_buf.buffer[3],
                            ]);
                            let shard = (chunk_id as usize) % nw;
                            let fragment = pool_buf.split_to(n).freeze();
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
        // fragment_txs are cloned into each worker task.
        // The original will be dropped after the pipeline completes (or on reconnect).

        // Start N independent reassemblers, one per shard.
        // Each processes fragments for chunk_id % shard_index chunks.
        // All emit assembled payloads to the shared orderer channel.
        let mut reassembler_handles = Vec::with_capacity(num_workers);
        for (i, rx) in fragment_rxs.into_iter().enumerate() {
            let tx = reassembly_tx.clone();
            let mut reassembler =
                FragmentReassembler::new(tx, max_inflight, DEFAULT_CHUNK_TIMEOUT_SECS, buffer_pool.clone());
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

        // Reconnect trigger channel + outbound relay channel.
        // When the control forward loop receives a Reconnect, it sends it here.
        // When the reconnect handler needs to send Ack/ChannelOpened, it sends
        // via reconnect_out_tx which the forward loop relays on the TCP connection.
        let (reconnect_tx, mut reconnect_rx) =
            mpsc::channel::<ControlMessage>(DEFAULT_CHANNEL_CAPACITY);
        let (reconnect_out_tx, mut reconnect_out_rx) =
            mpsc::channel::<ControlMessage>(DEFAULT_CHANNEL_CAPACITY);

        // Forward: bidirectional TCP forwarding over the control connection.
        // In file mode, conn stays in the main scope for post-EOS FileComplete.
        // In pipe mode, conn moves into this background task.
        let _control_forward_handle = if self.mode == Mode::File {
            None
        } else {
            let mut c = conn_opt
                .take()
                .expect("conn should be present in pipe mode");
            let reconnect_tx = reconnect_tx.clone();
            Some(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;

                        msg = reconnect_out_rx.recv() => {
                            match msg {
                                Some(msg) => {
                                    if let Err(e) = c.send_message(&msg).await {
                                        warn!("failed to send reconnect control message: {e}");
                                        break;
                                    }
                                }
                                None => break,
                            }
                        }
                        msg = queue_status_rx.recv() => {
                            match msg {
                                Some(msg) => {
                                    if let Err(e) = c.send_message(&msg).await {
                                        warn!("failed to send control message: {e}");
                                        break;
                                    }
                                }
                                None => break,
                            }
                        }
                        result = c.recv_message() => {
                            match result {
                                Ok(ControlMessage::FileStart { .. }) => {
                                    warn!("received FileStart in pipe mode, ignoring");
                                    std::process::exit(1);
                                }
                                Ok(msg @ ControlMessage::Reconnect { .. }) => {
                                    info!("control forward: received Reconnect, forwarding to handler");
                                    let _ = reconnect_tx.send(msg).await;
                                }
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
            }))
        };

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

        // Wait for all reassemblers to finish (EOS from sender) or a Reconnect
        // message from the sender.
        let mut need_reconnect = false;
        loop {
            match tokio::time::timeout(Duration::from_millis(100), reconnect_rx.recv()).await {
                Ok(Some(ControlMessage::Reconnect { last_sequence_number })) => {
                    info!(
                        "received Reconnect: last_sequence_number={}",
                        last_sequence_number
                    );
                    need_reconnect = true;
                    break;
                }
                Ok(Some(_)) => {
                    trace!("ignored non-Reconnect message on reconnect channel");
                }
                Ok(None) => {
                    trace!("reconnect channel closed");
                }
                Err(_) => {}
            }

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
        }

        if need_reconnect {
            info!("reconnect: opening new UDP sockets");

            let num_new = worker_handles.len();
            let mut new_udp_sockets = Vec::with_capacity(num_new);
            let mut new_channel_infos = Vec::with_capacity(num_new);
            for channel_id in 0..num_new as u16 {
                match braid::control::negotiation::open_udp_socket().await {
                    Ok(socket) => {
                        let port = socket.local_addr().map(|a| a.port()).unwrap_or(0);
                        info!(
                            "reconnect: opened new UDP socket channel_id={}, port={}",
                            channel_id, port
                        );
                        new_udp_sockets.push(socket);
                        new_channel_infos.push((channel_id, port));
                    }
                    Err(e) => {
                        warn!("reconnect: failed to open UDP socket {}: {}", channel_id, e);
                    }
                }
            }

            let ack_seq = new_channel_infos.first().map(|(_, p)| *p as u64).unwrap_or(0);

            info!("reconnect: sending Ack");
            let _ = reconnect_out_tx
                .send(ControlMessage::Ack {
                    sequence_number: ack_seq,
                })
                .await;

            for (channel_id, port) in &new_channel_infos {
                let msg = ControlMessage::ChannelOpened {
                    channel_id: *channel_id,
                    port: *port,
                };
                let _ = reconnect_out_tx.send(msg).await;
            }

            // Start new UDP workers routing through the same fragment channels.
            let mut new_worker_handles = Vec::with_capacity(new_udp_sockets.len());
            for (i, socket) in new_udp_sockets.into_iter().enumerate() {
                let sd = shutdown.clone();
                let txs = fragment_txs.clone();
                let pool = buffer_pool.clone();
                let nw = num_new;
                let handle = tokio::spawn(async move {
                    info!("reconnect UDP receive worker {} started", i);
                    let mut consecutive_timeouts = 0;
                    const MAX_CONSECUTIVE_TIMEOUTS: u32 = 10;
                    loop {
                        if sd.is_shutting_down() {
                            break;
                        }
                        let mut pool_buf = pool.acquire().await;
                        let result = tokio::time::timeout(
                            Duration::from_secs(1),
                            socket.recv_from(&mut *pool_buf.buffer),
                        )
                        .await;
                        match result {
                            Ok(Ok((n, _src))) => {
                                consecutive_timeouts = 0;
                                if n < FragmentHeader::LEN {
                                    continue;
                                }
                                if !verify_fragment_crc(
                                    &pool_buf.buffer[FragmentHeader::LEN..n],
                                    u32::from_be_bytes([
                                        pool_buf.buffer[10],
                                        pool_buf.buffer[11],
                                        pool_buf.buffer[12],
                                        pool_buf.buffer[13],
                                    ]),
                                ) {
                                    continue;
                                }
                                let chunk_id = u32::from_be_bytes([
                                    pool_buf.buffer[0],
                                    pool_buf.buffer[1],
                                    pool_buf.buffer[2],
                                    pool_buf.buffer[3],
                                ]);
                                let shard = (chunk_id as usize) % nw;
                                let fragment = pool_buf.split_to(n).freeze();
                                if txs[shard].send(fragment).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Err(e)) => {
                                if sd.is_shutting_down() {
                                    break;
                                }
                                warn!("reconnect UDP recv error on worker {}: {}", i, e);
                                break;
                            }
                            Err(_) => {
                                consecutive_timeouts += 1;
                                if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS {
                                    break;
                                }
                            }
                        }
                    }
                    info!("reconnect UDP receive worker {} finished", i);
                });
                new_worker_handles.push(handle);
            }

            worker_handles = new_worker_handles;

            loop {
                let mut all_done = true;
                for handle in &reassembler_handles {
                    if !handle.is_finished() {
                        all_done = false;
                        break;
                    }
                }
                if all_done {
                    info!("reconnect: all reassemblers completed");
                    break;
                }
                if shutdown.is_shutting_down() {
                    info!("reconnect: reassemblers interrupted by shutdown signal");
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }

        // Drop fragment_txs and reassembly_tx so the orderer sees channel close.
        drop(fragment_txs);
        drop(reassembly_tx);

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

        // ─── File mode: post-EOS hash validation + FileComplete ──────────
        if let Some((ref receiver_obj, ref output_path, expected_hash)) = file_mode_state {
            let mut conn = conn_opt
                .take()
                .expect("conn should be available in file mode");

            // Check if CommitGate encountered write errors during data transfer.
            // If a write error occurred (e.g. disk full), we report failure without
            // computing hash (the file may be incomplete or unwritable).
            let gate_stats = commit_gate_stats.snapshot();
            if gate_stats.write_errors > 0 {
                error!(
                    "commit gate encountered {} write errors, aborting file mode",
                    gate_stats.write_errors
                );
                let fail_msg = ControlMessage::FileComplete {
                    success: false,
                    expected_hash: 0,
                    computed_hash: 0,
                };
                if let Err(e) = conn.send_message(&fail_msg).await {
                    warn!("failed to send FileComplete after write error: {e}");
                }
                shutdown.initiate();
                let _ = monitor_cancel_tx.try_send(());
                let _ = monitor_handle.await;
                let _ = progress_handle.await;
                return Err("write error during file transfer".into());
            }

            let computed = receiver_obj
                .compute_hash(output_path)
                .await
                .map_err(|e| format!("failed to compute output file hash: {e}"))?;
            let success = computed == expected_hash;
            info!(
                "file mode: validation result: expected={:08x}, computed={:08x}, success={}",
                expected_hash, computed, success
            );
            let complete_msg = ControlMessage::FileComplete {
                success,
                expected_hash,
                computed_hash: computed,
            };
            if let Err(e) = conn.send_message(&complete_msg).await {
                warn!("failed to send FileComplete: {e}");
            }
            if !success {
                error!(
                    "file CRC32C mismatch: expected 0x{:08X}, got 0x{:08X}",
                    expected_hash, computed
                );
                if let Err(e) = output::delete_output(output_path).await {
                    warn!("failed to delete output file after hash mismatch: {e}");
                }
            } else {
                info!("file transferred successfully: crc32c=0x{:08X}", computed);
                eprintln!("file transferred successfully (crc32c=0x{:08X})", computed);
            }
            shutdown.initiate();
            let result = if success {
                Ok(())
            } else {
                Err("file hash mismatch".into())
            };
            let _ = monitor_cancel_tx.try_send(());
            let _ = monitor_handle.await;
            let _ = progress_handle.await;
            for handle in worker_handles {
                let _ = handle.await;
            }
            return result;
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
        let guard = pool.acquire();
        let guard = tokio::runtime::Runtime::new().unwrap().block_on(guard);
        assert_eq!(guard.buffer.len(), 1024);
    }

    #[test]
    fn buffer_pool_reuses_buffers() {
        let pool = BufferPool::new(1, 8);
        {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let mut guard = rt.block_on(pool.acquire());
            guard.buffer[0] = 42;
        }
        let rt = tokio::runtime::Runtime::new().unwrap();
        let guard = rt.block_on(pool.acquire());
        // After return to pool, buffer is cleared and resized
        assert_eq!(guard.buffer.len(), 8);
        assert_eq!(guard.buffer[0], 0);
    }

    #[test]
    fn fragment_reassembler_creation() {
        let (tx, _rx) = mpsc::channel::<Bytes>(16);
        let pool = braid::buffer::pool::BufferPool::new(4, 65536);
        let reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, pool);
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
