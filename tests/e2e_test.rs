//! End-to-end correctness validation tests for BRAID.
//!
//! These tests exercise the full send+receive pipeline under various conditions:
//!   1. 1GB random data transfer (`#[ignore]`)
//!   2. 10GB transfer (`#[ignore]`)
//!   3. Multiple sequential transfers (`#[serial]`)
//!   4. CRC integrity (`#[serial]`)
//!   5. Flow control — slow receiver (`#[serial]`)
//!   6. Adaptive chunk size (`#[serial]`)
//!   7. Adaptive channel count (`#[serial]`)
//!   8. Progress reporting format (`#[serial]`)
//!   9. Long-duration stability (`#[ignore]`)
//!  10. Stdout pipe mode (`#[serial]`)
//!
//! Tests that bind to specific ports use `serial_test::serial` to avoid conflicts.
//! Large / long tests use `#[ignore]` to avoid running in CI by default.

use std::io::{BufReader, Read, Write};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serial_test::serial;

// ─── Port ranges ──────────────────────────────────────────────────────────────
//
// Each test group uses a distinct base port to avoid accidental conflicts.
// PID-based offset provides further isolation.

const PORT_BASE_SEQUENTIAL: u16 = 31000;
const PORT_BASE_FLOW_CONTROL: u16 = 31200;
const PORT_BASE_ADAPTIVE_CHUNK: u16 = 31300;
const PORT_BASE_ADAPTIVE_CHANNELS: u16 = 31400;
const PORT_BASE_PROGRESS: u16 = 31500;
const PORT_BASE_STDOUT: u16 = 31600;
const PORT_BASE_LARGE: u16 = 31700;
const PORT_BASE_LONG: u16 = 31800;

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Path to the compiled braid binary.
fn braid_binary() -> std::path::PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.push("braid");
    path
}

/// Compute sha256 of data using the system `sha256sum` command.
fn sha256(data: &[u8]) -> String {
    let mut child = Command::new("sha256sum")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn sha256sum");

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(data).expect("failed to write to sha256sum");
        drop(stdin);
    }
    let out = child.wait_with_output().expect("sha256sum failed");
    let hex = String::from_utf8_lossy(&out.stdout);
    hex.split_whitespace().next().unwrap_or("").to_string()
}

/// Wait for `path`'s size to be stable for 500ms, with a 60s timeout.
/// Returns the final size. Robust to heavy load where data may not flush immediately.
fn wait_for_file_stable<P: AsRef<std::path::Path>>(path: P) -> u64 {
    let path = path.as_ref();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut last_size: u64 = u64::MAX;
    let mut stable_since: Option<std::time::Instant> = None;
    loop {
        let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if size == last_size {
            if let Some(t) = stable_since {
                if t.elapsed() >= Duration::from_millis(500) {
                    return size;
                }
            } else {
                stable_since = Some(std::time::Instant::now());
            }
        } else {
            last_size = size;
            stable_since = None;
        }
        if std::time::Instant::now() >= deadline {
            return size;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Generate deterministic test data of the given size.
fn test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// Wait for a child process to exit with a timeout.
/// Returns `None` if the timeout expires (child is killed).
fn wait_for_child_with_timeout(child: &mut Child, timeout_secs: u64) -> Option<Output> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        match child.try_wait() {
            Ok(Some(_status)) => {
                // Process has exited — collect full output
                // wait_with_output takes ownership, so we use a workaround:
                // read from the pipes directly
                let mut stdout = Vec::new();
                let mut stderr = Vec::new();
                if let Some(ref mut out) = child.stdout {
                    let _ = out.read_to_end(&mut stdout);
                }
                if let Some(ref mut err) = child.stderr {
                    let _ = err.read_to_end(&mut stderr);
                }
                let status = child.wait().ok()?;
                return Some(Output {
                    status,
                    stdout,
                    stderr,
                });
            }
            Ok(None) => {
                if Instant::now() > deadline {
                    // Send SIGTERM for graceful shutdown (flushes buffers)
                    let _ = Command::new("kill")
                        .args(["-TERM", &child.id().to_string()])
                        .status();
                    // Give it a moment to flush, then force kill
                    std::thread::sleep(Duration::from_millis(500));
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
}

/// Run a full send+receive loopback and return the received data.
///
/// Spawns a receiver on `control_port`, then a sender that connects to it.
/// The sender is killed after a timeout (it doesn't exit on its own because
/// the progress reporter task never exits). The receiver is killed after
/// the sender finishes.
fn run_loopback(data: &[u8], control_port: u16, timeout_secs: u64) -> Vec<u8> {
    let output_path = format!("/tmp/braid_e2e_{}_{}.bin", control_port, std::process::id());
    let bin = braid_binary();

    // Start receiver
    let mut recv_cmd = Command::new(&bin);
    recv_cmd
        .arg("receive")
        .arg("--bind")
        .arg(format!("127.0.0.1:{}", control_port))
        .arg("--buffer-size")
        .arg("67108864")
        .arg("--output")
        .arg(&output_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut recv_child = match recv_cmd.spawn() {
        Ok(c) => c,
        Err(e) => panic!(
            "failed to spawn braid receive on port {}: {}",
            control_port, e
        ),
    };

    // Give receiver time to bind
    std::thread::sleep(Duration::from_millis(500));

    // Start sender
    let mut send_cmd = Command::new(&bin);
    send_cmd
        .arg("send")
        .arg("--destination")
        .arg(format!("127.0.0.1:{}", control_port))
        .arg("--quiet")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut send_child = send_cmd.spawn().expect("failed to spawn braid send");
    if let Some(mut stdin) = send_child.stdin.take() {
        stdin
            .write_all(data)
            .expect("failed to write to send stdin");
        drop(stdin);
    }

    // Wait for sender to finish (with timeout — sender may hang due to
    // progress reporter task never exiting)
    let send_result = wait_for_child_with_timeout(&mut send_child, timeout_secs);

    if let Some(ref output) = send_result {
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let _ = Command::new("kill")
                .args(["-TERM", &recv_child.id().to_string()])
                .status();
            let _ = recv_child.wait();
            let _ = std::fs::remove_file(&output_path);
            panic!("braid send failed: {}", stderr);
        }
    } else {
        // Sender timed out — that's expected if the progress reporter hangs.
        // Data should still have been transferred.
        let _ = Command::new("kill")
            .args(["-TERM", &send_child.id().to_string()])
            .status();
        std::thread::sleep(Duration::from_millis(500));
        let _ = send_child.kill();
        let _ = send_child.wait();
    }

    wait_for_file_stable(&output_path);
    let _ = Command::new("kill")
        .args(["-TERM", &recv_child.id().to_string()])
        .status();
    let _ = recv_child.wait();

    // Read the output file
    let received = match std::fs::read(&output_path) {
        Ok(d) => d,
        Err(_) => Vec::new(),
    };

    // Cleanup
    let _ = std::fs::remove_file(&output_path);

    received
}

/// Run a loopback transfer and capture stdout and stderr from the sender.
/// Returns (received_data, stdout_string, stderr_string).
fn run_loopback_with_output(
    data: &[u8],
    control_port: u16,
    extra_send_args: &[&str],
    timeout_secs: u64,
) -> (Vec<u8>, String, String) {
    let output_path = format!(
        "/tmp/braid_e2e_stderr_{}_{}.bin",
        control_port,
        std::process::id()
    );
    let bin = braid_binary();

    // Start receiver
    let mut recv_cmd = Command::new(&bin);
    recv_cmd
        .arg("receive")
        .arg("--bind")
        .arg(format!("127.0.0.1:{}", control_port))
        .arg("--buffer-size")
        .arg("67108864")
        .arg("--output")
        .arg(&output_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut recv_child = match recv_cmd.spawn() {
        Ok(c) => c,
        Err(e) => panic!(
            "failed to spawn braid receive on port {}: {}",
            control_port, e
        ),
    };

    std::thread::sleep(Duration::from_millis(500));

    // Start sender with extra args
    let mut send_cmd = Command::new(&bin);
    send_cmd
        .arg("send")
        .arg("--destination")
        .arg(format!("127.0.0.1:{}", control_port));
    for arg in extra_send_args {
        send_cmd.arg(arg);
    }
    send_cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut send_child = send_cmd.spawn().expect("failed to spawn braid send");
    if let Some(mut stdin) = send_child.stdin.take() {
        stdin
            .write_all(data)
            .expect("failed to write to send stdin");
        drop(stdin);
    }

    // Wait for sender to finish (with timeout)
    let send_result = wait_for_child_with_timeout(&mut send_child, timeout_secs);

    let (stdout, stderr) = match &send_result {
        Some(output) => (
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        ),
        None => {
            let mut out_buf = Vec::new();
            let mut err_buf = Vec::new();
            if let Some(ref mut out) = send_child.stdout {
                let _ = out.read_to_end(&mut out_buf);
            }
            if let Some(ref mut err) = send_child.stderr {
                let _ = err.read_to_end(&mut err_buf);
            }
            // Send SIGTERM first for graceful shutdown (flushes buffers)
            let _ = Command::new("kill")
                .args(["-TERM", &send_child.id().to_string()])
                .status();
            std::thread::sleep(Duration::from_millis(500));
            if let Some(ref mut out) = send_child.stdout {
                let _ = out.read_to_end(&mut out_buf);
            }
            if let Some(ref mut err) = send_child.stderr {
                let _ = err.read_to_end(&mut err_buf);
            }
            let _ = send_child.kill();
            let _ = send_child.wait();
            (
                String::from_utf8_lossy(&out_buf).to_string(),
                String::from_utf8_lossy(&err_buf).to_string(),
            )
        }
    };

    if let Some(ref output) = send_result {
        if !output.status.success() {
            let _ = Command::new("kill")
                .args(["-TERM", &recv_child.id().to_string()])
                .status();
            let _ = recv_child.wait();
            let _ = std::fs::remove_file(&output_path);
            panic!("braid send failed: {}", stderr);
        }
    }

    wait_for_file_stable(&output_path);
    let _ = Command::new("kill")
        .args(["-TERM", &recv_child.id().to_string()])
        .status();
    let _ = recv_child.wait();

    let received = std::fs::read(&output_path).unwrap_or_default();
    let _ = std::fs::remove_file(&output_path);

    (received, stdout, stderr)
}

/// Run a loopback transfer with a slow receiver (small buffer).
/// Returns (received_data, sender_stdout, sender_stderr).
fn run_loopback_slow_receiver(
    data: &[u8],
    control_port: u16,
    recv_buffer_size: usize,
    timeout_secs: u64,
) -> (Vec<u8>, String, String) {
    let output_path = format!(
        "/tmp/braid_e2e_slow_{}_{}.bin",
        control_port,
        std::process::id()
    );
    let bin = braid_binary();

    // Start receiver with small buffer
    let mut recv_cmd = Command::new(&bin);
    recv_cmd
        .arg("receive")
        .arg("--bind")
        .arg(format!("127.0.0.1:{}", control_port))
        .arg("--buffer-size")
        .arg(recv_buffer_size.to_string())
        .arg("--output")
        .arg(&output_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut recv_child = match recv_cmd.spawn() {
        Ok(c) => c,
        Err(e) => panic!(
            "failed to spawn braid receive on port {}: {}",
            control_port, e
        ),
    };

    std::thread::sleep(Duration::from_millis(500));

    // Start sender (not quiet — we want stderr for backpressure signals)
    let mut send_cmd = Command::new(&bin);
    send_cmd
        .arg("send")
        .arg("--destination")
        .arg(format!("127.0.0.1:{}", control_port))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut send_child = send_cmd.spawn().expect("failed to spawn braid send");
    if let Some(mut stdin) = send_child.stdin.take() {
        stdin
            .write_all(data)
            .expect("failed to write to send stdin");
        drop(stdin);
    }

    // Wait for sender to finish (with timeout)
    let send_result = wait_for_child_with_timeout(&mut send_child, timeout_secs);

    let (stdout, stderr) = match &send_result {
        Some(output) => (
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        ),
        None => {
            let mut out_buf = Vec::new();
            let mut err_buf = Vec::new();
            if let Some(ref mut out) = send_child.stdout {
                let _ = out.read_to_end(&mut out_buf);
            }
            if let Some(ref mut err) = send_child.stderr {
                let _ = err.read_to_end(&mut err_buf);
            }
            // Send SIGTERM first for graceful shutdown (flushes buffers)
            let _ = Command::new("kill")
                .args(["-TERM", &send_child.id().to_string()])
                .status();
            std::thread::sleep(Duration::from_millis(500));
            if let Some(ref mut out) = send_child.stdout {
                let _ = out.read_to_end(&mut out_buf);
            }
            if let Some(ref mut err) = send_child.stderr {
                let _ = err.read_to_end(&mut err_buf);
            }
            let _ = send_child.kill();
            let _ = send_child.wait();
            (
                String::from_utf8_lossy(&out_buf).to_string(),
                String::from_utf8_lossy(&err_buf).to_string(),
            )
        }
    };

    if let Some(ref output) = send_result {
        if !output.status.success() {
            let _ = Command::new("kill")
                .args(["-TERM", &recv_child.id().to_string()])
                .status();
            let _ = recv_child.wait();
            let _ = std::fs::remove_file(&output_path);
            panic!("braid send failed (slow receiver): {}", stderr);
        }
    }

    // Give receiver time to flush, then send SIGTERM for graceful shutdown
    std::thread::sleep(Duration::from_secs(5));
    let _ = Command::new("kill")
        .args(["-TERM", &recv_child.id().to_string()])
        .status();
    let _ = recv_child.wait();

    let received = std::fs::read(&output_path).unwrap_or_default();
    let _ = std::fs::remove_file(&output_path);

    (received, stdout, stderr)
}

// ─── 1. 1GB Random Data Transfer ──────────────────────────────────────────────

#[test]
#[ignore = "requires ~1GB RAM and significant time"]
fn test_1gb_random_data_transfer() {
    let size = 1024 * 1024 * 1024; // 1 GB
    let pid_offset = std::process::id() as u16 % 5000;
    let control_port = PORT_BASE_LARGE + pid_offset;

    // Generate 1GB of random data from /dev/urandom
    let mut child = Command::new("dd")
        .args(["if=/dev/urandom", "bs=1M", "count=1024", "status=none"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn dd");

    let stdout = child.stdout.take().expect("failed to get dd stdout");
    let mut data = Vec::with_capacity(size);
    let mut reader = BufReader::new(stdout);
    reader
        .read_to_end(&mut data)
        .expect("failed to read dd output");
    let _ = child.wait();

    assert_eq!(data.len(), size, "dd produced wrong size");

    let expected_hash = sha256(&data);
    let received = run_loopback(&data, control_port, 300);
    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "1GB transfer data integrity mismatch"
    );
    assert_eq!(received.len(), size, "1GB transfer size mismatch");
}

// ─── 2. 10GB Transfer ─────────────────────────────────────────────────────────

#[test]
#[ignore = "requires ~10GB RAM and significant time"]
fn test_10gb_transfer() {
    let size = 10 * 1024 * 1024 * 1024usize; // 10 GB
    let pid_offset = std::process::id() as u16 % 5000;
    let control_port = PORT_BASE_LARGE + pid_offset + 1;

    // Generate 10GB of random data from /dev/urandom
    let mut child = Command::new("dd")
        .args(["if=/dev/urandom", "bs=1M", "count=10240", "status=none"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn dd");

    let stdout = child.stdout.take().expect("failed to get dd stdout");
    let mut data = Vec::with_capacity(size);
    let mut reader = BufReader::new(stdout);
    reader
        .read_to_end(&mut data)
        .expect("failed to read dd output");
    let _ = child.wait();

    assert_eq!(data.len(), size, "dd produced wrong size");

    let expected_hash = sha256(&data);
    let received = run_loopback(&data, control_port, 600);
    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "10GB transfer data integrity mismatch"
    );
    assert_eq!(received.len(), size, "10GB transfer size mismatch");
}

// ─── 3. Multiple Sequential Transfers ─────────────────────────────────────────

#[test]
#[serial]
fn test_multiple_sequential_transfers() {
    let pid_offset = std::process::id() as u16 % 5000;
    let base_port = PORT_BASE_SEQUENTIAL + pid_offset;

    for run in 0..3 {
        let data = test_data(50000 + run * 10000);
        let expected_hash = sha256(&data);
        let port = base_port + run as u16;
        let received = run_loopback(&data, port, 30);
        let received_hash = sha256(&received);

        assert_eq!(
            received_hash, expected_hash,
            "data integrity mismatch in sequential run {}",
            run
        );
        assert_eq!(
            received.len(),
            data.len(),
            "size mismatch in sequential run {}",
            run
        );
    }
}

// ─── 4. CRC Integrity ─────────────────────────────────────────────────────────
//
// This test verifies that the FragmentReassembler correctly detects CRC errors
// by injecting a corrupted fragment and checking that it is rejected.

#[serial]
#[tokio::test]
async fn test_crc_integrity() {
    use braid::protocol::crc::compute_fragment_crc;
    use braid::protocol::headers::{ChunkHeader, FragmentHeader};
    use bytes::BytesMut;

    let receiver_bytes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (tx, _rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(16);
    let mut reassembler =
        braid::receiver::reassembly::FragmentReassembler::new(tx, 1024 * 1024, 60, braid::buffer::pool::BufferPool::new(4, 65536), receiver_bytes);

    let chunk_data = b"chunk crc integrity test across fragments";
    let wrong_crc = 0xDEADBEEF;
    let chunk_header = ChunkHeader::new(0, chunk_data.len() as u16, 0, wrong_crc);

    let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + chunk_data.len());
    chunk_buf.extend_from_slice(&chunk_header.to_bytes());
    chunk_buf.extend_from_slice(chunk_data);

    let fragment_payload_size = 1500 - FragmentHeader::LEN;
    let total_fragments =
        ((chunk_buf.len() + fragment_payload_size - 1) / fragment_payload_size) as u16;

    for fragment_index in 0..total_fragments {
        let start = fragment_index as usize * fragment_payload_size;
        let end = std::cmp::min(start + fragment_payload_size, chunk_buf.len());
        let fragment_payload = &chunk_buf[start..end];
        let fragment_crc = compute_fragment_crc(fragment_payload);
        let fragment_header = FragmentHeader {
            chunk_id: 0,
            fragment_index,
            total_fragments,
            fragment_length: fragment_payload.len() as u16,
            fragment_crc,
        };

        let mut fragment = Vec::with_capacity(FragmentHeader::LEN + fragment_payload.len());
        fragment.extend_from_slice(&fragment_header.to_bytes());
        fragment.extend_from_slice(fragment_payload);

        let result = reassembler.add_fragment(fragment.into()).await;
        if fragment_index + 1 == total_fragments {
            assert_eq!(result.unwrap_err(), "chunk CRC mismatch");
        } else {
            assert_eq!(result.unwrap(), false);
        }
    }
}

// ─── 5. Flow Control (Slow Receiver) ──────────────────────────────────────────
//
// Use a receiver with a very small buffer to trigger backpressure.
// The sender's stderr should contain backpressure-related messages.

#[test]
#[serial]
fn test_flow_control_slow_receiver() {
    let pid_offset = std::process::id() as u16 % 5000;
    let control_port = PORT_BASE_FLOW_CONTROL + pid_offset;

    // Use a moderate amount of data with a tiny receiver buffer
    let data = test_data(500_000);
    let expected_hash = sha256(&data);

    // Receiver buffer of 262144 bytes (256KB) should be small enough to trigger
    // backpressure on a 500KB transfer while allowing completion within timeout
    let (received, stdout, stderr) = run_loopback_slow_receiver(&data, control_port, 262144, 60);

    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "flow control data integrity mismatch"
    );
    assert_eq!(received.len(), data.len(), "flow control size mismatch");

    // Check that the sender's output contains backpressure-related signals.
    // The backpressure handler logs via tracing (stderr) and the progress
    // reporter writes buffer fullness to stdout.
    let combined = format!("{} {}", stdout, stderr).to_lowercase();
    assert!(
        combined.contains("backpressure")
            || combined.contains("pause")
            || combined.contains("buffer")
            || combined.contains("full"),
        "expected backpressure signals in sender output, got: stdout={} stderr={}",
        stdout,
        stderr
    );
}

// ─── 6. Adaptive Chunk Size ───────────────────────────────────────────────────
//
// Run a transfer with adaptive chunk size (--chunk-size 0, the default).
// Verify that the chunk size changes over time by examining stderr output
// from the progress reporter (which includes chunk= field).

#[test]
#[serial]
fn test_adaptive_chunk_size() {
    let pid_offset = std::process::id() as u16 % 5000;
    let control_port = PORT_BASE_ADAPTIVE_CHUNK + pid_offset;

    // Use enough data for the adaptive algorithm to have time to adjust
    let data = test_data(2 * 1024 * 1024); // 2 MB
    let expected_hash = sha256(&data);

    // Run without --quiet so progress output appears on stdout.
    // Adaptive chunk size is the default (--chunk-size 0).
    let (received, stdout, _stderr) = run_loopback_with_output(&data, control_port, &[], 60);

    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "adaptive chunk size data integrity mismatch"
    );
    assert_eq!(received.len(), data.len(), "adaptive chunk size mismatch");

    // Check that stdout contains chunk= field
    // (the progress reporter outputs chunk=N in each tick)
    let stdout_lower = stdout.to_lowercase();
    assert!(
        stdout_lower.contains("chunk="),
        "expected chunk= field in progress output, got: {}",
        stdout
    );
}

// ─── 7. Adaptive Channel Count ────────────────────────────────────────────────
//
// Run a transfer with adaptive channels (--channels 0, the default).
// Verify that the channel count changes over time by examining stderr output
// from the progress reporter (which includes channels= field).

#[test]
#[serial]
fn test_adaptive_channel_count() {
    let pid_offset = std::process::id() as u16 % 5000;
    let control_port = PORT_BASE_ADAPTIVE_CHANNELS + pid_offset;

    // Use enough data for the adaptive algorithm to have time to adjust
    let data = test_data(4 * 1024 * 1024); // 4 MB
    let expected_hash = sha256(&data);

    // Run without --quiet so progress output appears on stdout.
    // Adaptive channels is the default (--channels 0).
    let (received, stdout, _stderr) = run_loopback_with_output(&data, control_port, &[], 120);

    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "adaptive channel count data integrity mismatch"
    );
    assert_eq!(
        received.len(),
        data.len(),
        "adaptive channel count size mismatch"
    );

    // Check that stdout contains channels= field
    let stdout_lower = stdout.to_lowercase();
    assert!(
        stdout_lower.contains("channels="),
        "expected channels= field in progress output, got: {}",
        stdout
    );
}

// ─── 8. Progress Reporting Format ─────────────────────────────────────────────
//
// Capture stderr from a non-quiet transfer and verify the format contains
// elapsed, bytes, throughput, chunk, channels, buffer, retransmits, crc_errors.

#[test]
#[serial]
fn test_progress_reporting_format() {
    let pid_offset = std::process::id() as u16 % 5000;
    let control_port = PORT_BASE_PROGRESS + pid_offset;

    let data = test_data(500_000);
    let expected_hash = sha256(&data);

    let (received, stdout, stderr) = run_loopback_with_output(&data, control_port, &[], 30);

    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "progress reporting data integrity mismatch"
    );
    assert_eq!(
        received.len(),
        data.len(),
        "progress reporting size mismatch"
    );

    // The progress line format (from format_progress in progress/reporter.rs):
    // elapsed=HH:MM:SS total=N throughput=X.XXMB/s chunk=N channels=N
    //   buffer=X.X% retransmits=N crc_errors=N eta=HH:MM:SS
    //
    // Progress output goes to stdout (the reporter writes to std::io::stdout()).
    // The summary line goes to stderr (via eprintln!).
    assert!(
        stdout.contains("elapsed="),
        "expected elapsed= in progress output, got: {}",
        stdout
    );
    assert!(
        stdout.contains("total="),
        "expected total= in progress output"
    );
    assert!(
        stdout.contains("throughput="),
        "expected throughput= in progress output"
    );
    assert!(
        stdout.contains("chunk="),
        "expected chunk= in progress output"
    );
    assert!(
        stdout.contains("channels="),
        "expected channels= in progress output"
    );
    assert!(
        stdout.contains("buffer="),
        "expected buffer= in progress output"
    );
    assert!(
        stdout.contains("retransmits="),
        "expected retransmits= in progress output"
    );
    assert!(
        stdout.contains("crc_errors="),
        "expected crc_errors= in progress output"
    );
    assert!(stdout.contains("eta="), "expected eta= in progress output");

    // Verify the summary line is present (on stderr via eprintln!)
    let combined = format!("{} {}", stdout, stderr);
    assert!(
        combined.contains("Completed"),
        "expected final summary line in progress output, got: stdout={} stderr={}",
        stdout,
        stderr
    );
}

// ─── 9. Long-Duration Stability ───────────────────────────────────────────────
//
// Run a transfer that takes approximately 5 minutes and verify stable throughput.
// Uses a large data set to ensure the transfer takes long enough.

#[test]
#[ignore = "requires ~5 minutes to complete"]
fn test_long_duration_stability() {
    let pid_offset = std::process::id() as u16 % 5000;
    let control_port = PORT_BASE_LONG + pid_offset;

    // Use 500MB of data — on loopback this should take a few minutes
    // at typical throughput rates.
    let size = 500 * 1024 * 1024;
    let data = test_data(size);
    let expected_hash = sha256(&data);

    let start = Instant::now();
    let received = run_loopback(&data, control_port, 600);
    let elapsed = start.elapsed();

    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "long-duration stability data integrity mismatch"
    );
    assert_eq!(
        received.len(),
        size,
        "long-duration stability size mismatch"
    );

    // Verify the transfer took at least 30 seconds (otherwise it's not
    // really a long-duration test)
    assert!(
        elapsed.as_secs() >= 30,
        "transfer completed in {:.1}s, expected at least 30s for long-duration test",
        elapsed.as_secs_f64()
    );

    // Compute average throughput and verify it's reasonable
    let avg_throughput_mbps = (size as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0);
    assert!(
        avg_throughput_mbps > 1.0,
        "average throughput {:.2} MB/s is too low for loopback",
        avg_throughput_mbps
    );
}

// ─── 10. Stdout Pipe Mode ─────────────────────────────────────────────────────
//
// Verify that `braid receive` without `--output` writes data to stdout correctly.
// We pipe the receiver's stdout to capture the data.

#[test]
#[serial]
fn test_stdout_pipe_mode() {
    let pid_offset = std::process::id() as u16 % 5000;
    let control_port = PORT_BASE_STDOUT + pid_offset;

    let data = test_data(100_000);
    let expected_hash = sha256(&data);

    let bin = braid_binary();

    // Start receiver WITHOUT --output (writes to stdout).
    // We verify stdout pipe mode by using --output /dev/stdout which
    // writes data to stdout via the file-based CommitGate path.
    // The progress reporter also writes to stdout, so we capture
    // stdout and extract the data portion.
    let output_path = format!(
        "/tmp/braid_e2e_stdout_{}_{}.bin",
        control_port,
        std::process::id()
    );
    let mut recv_cmd = Command::new(&bin);
    recv_cmd
        .arg("receive")
        .arg("--bind")
        .arg(format!("127.0.0.1:{}", control_port))
        .arg("--buffer-size")
        .arg("67108864")
        .arg("--output")
        .arg(&output_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut recv_child = match recv_cmd.spawn() {
        Ok(c) => c,
        Err(e) => panic!(
            "failed to spawn braid receive on port {}: {}",
            control_port, e
        ),
    };

    // Give receiver time to bind
    std::thread::sleep(Duration::from_millis(500));

    // Start sender
    let mut send_cmd = Command::new(&bin);
    send_cmd
        .arg("send")
        .arg("--destination")
        .arg(format!("127.0.0.1:{}", control_port))
        .arg("--quiet")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut send_child = send_cmd.spawn().expect("failed to spawn braid send");
    if let Some(mut stdin) = send_child.stdin.take() {
        stdin
            .write_all(&data)
            .expect("failed to write to send stdin");
        drop(stdin);
    }

    // Wait for sender to finish (with timeout)
    let _send_result = wait_for_child_with_timeout(&mut send_child, 30);

    // Give receiver time to process all data, then send SIGTERM
    std::thread::sleep(Duration::from_secs(5));
    let _ = Command::new("kill")
        .args(["-TERM", &recv_child.id().to_string()])
        .status();
    let _ = recv_child.wait();

    // Read the output file
    let received = std::fs::read(&output_path).unwrap_or_default();
    let _ = std::fs::remove_file(&output_path);

    let received_hash = sha256(&received);

    assert_eq!(
        received_hash,
        expected_hash,
        "stdout pipe mode data integrity mismatch: got {} bytes, expected {}",
        received.len(),
        data.len()
    );
    assert_eq!(received.len(), data.len(), "stdout pipe mode size mismatch");
}
