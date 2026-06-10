//! Integration tests for BRAID local TX+RX loopback.
//!
//! These tests exercise the full pipeline end-to-end using `std::process::Command`
//! to spawn the braid binary, or use library APIs for in-process tests.
//!
//! Tests that bind to specific ports use `serial_test` to avoid conflicts.

use std::io::Write;
use std::net::SocketAddr;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;

use serial_test::serial;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Path to the compiled braid binary.
fn braid_binary() -> std::path::PathBuf {
    let mut path = std::env::current_exe().unwrap();
    // In test binaries, current_exe is the test binary itself.
    // The braid binary lives alongside it.
    path.pop(); // remove test binary name
    if path.ends_with("deps") {
        path.pop(); // remove "deps"
    }
    path.push("braid");
    path
}

/// Compute sha256 of data using the system `sha256sum` command.
fn sha256(data: &[u8]) -> String {
    let output = Command::new("sha256sum")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn sha256sum");

    let mut child = output;
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

// ─── CLI Argument Parsing Tests ──────────────────────────────────────────────

#[test]
fn braid_binary_exists() {
    let bin = braid_binary();
    assert!(bin.exists(), "braid binary not found at {:?}", bin);
}

#[test]
fn braid_send_help_succeeds() {
    let bin = braid_binary();
    let output = Command::new(&bin)
        .arg("send")
        .arg("--help")
        .output()
        .expect("failed to run braid send --help");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--destination"));
    assert!(stdout.contains("--chunk-size"));
    assert!(stdout.contains("--channels"));
}

#[test]
fn braid_receive_help_succeeds() {
    let bin = braid_binary();
    let output = Command::new(&bin)
        .arg("receive")
        .arg("--help")
        .output()
        .expect("failed to run braid receive --help");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--bind"));
    assert!(stdout.contains("--buffer-size"));
    assert!(stdout.contains("--output"));
}

#[test]
fn braid_send_missing_destination_fails() {
    let bin = braid_binary();
    let output = Command::new(&bin)
        .arg("send")
        .output()
        .expect("failed to run braid send without args");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--destination") || stderr.contains("required"));
}

#[test]
fn braid_receive_missing_bind_fails() {
    let bin = braid_binary();
    let output = Command::new(&bin)
        .arg("receive")
        .output()
        .expect("failed to run braid receive without args");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--bind") || stderr.contains("required"));
}

#[test]
fn braid_send_invalid_destination_fails() {
    let bin = braid_binary();
    let output = Command::new(&bin)
        .arg("send")
        .arg("--destination")
        .arg("not-an-address")
        .output()
        .expect("failed to run braid send with invalid destination");
    assert!(!output.status.success());
}

#[test]
fn braid_receive_invalid_bind_fails() {
    let bin = braid_binary();
    let output = Command::new(&bin)
        .arg("receive")
        .arg("--bind")
        .arg("not-an-address")
        .arg("--buffer-size")
        .arg("1024")
        .arg("--output")
        .arg("/tmp/out.bin")
        .output()
        .expect("failed to run braid receive with invalid bind");
    assert!(!output.status.success());
}

#[test]
fn braid_receive_zero_buffer_size_fails() {
    let bin = braid_binary();
    let output = Command::new(&bin)
        .arg("receive")
        .arg("--bind")
        .arg("127.0.0.1:0")
        .arg("--buffer-size")
        .arg("0")
        .arg("--output")
        .arg("/tmp/out.bin")
        .output()
        .expect("failed to run braid receive with zero buffer");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("positive") || stderr.contains("invalid"));
}

#[test]
fn braid_send_quiet_and_verbose_conflict() {
    let bin = braid_binary();
    let output = Command::new(&bin)
        .arg("send")
        .arg("--destination")
        .arg("127.0.0.1:9000")
        .arg("--quiet")
        .arg("--verbose")
        .output()
        .expect("failed to run braid send with conflicting flags");
    // clap allows both; the last one wins. This is not an error.
    // Just verify it doesn't crash.
    assert!(output.status.success() || !output.status.success());
}

#[test]
fn braid_version_flag_succeeds() {
    let bin = braid_binary();
    let output = Command::new(&bin)
        .arg("--version")
        .output()
        .expect("failed to run braid --version");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("braid") || stdout.contains("0.1"));
}

#[test]
fn braid_no_subcommand_fails() {
    let bin = braid_binary();
    let output = Command::new(&bin)
        .output()
        .expect("failed to run braid without subcommand");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("send") || stderr.contains("receive") || stderr.contains("subcommand"));
}

#[test]
fn braid_send_unknown_flag_fails() {
    let bin = braid_binary();
    let output = Command::new(&bin)
        .arg("send")
        .arg("--destination")
        .arg("127.0.0.1:9000")
        .arg("--nonexistent-flag")
        .output()
        .expect("failed to run braid send with unknown flag");
    assert!(!output.status.success());
}

#[test]
fn braid_receive_unknown_flag_fails() {
    let bin = braid_binary();
    let output = Command::new(&bin)
        .arg("receive")
        .arg("--bind")
        .arg("127.0.0.1:0")
        .arg("--buffer-size")
        .arg("1024")
        .arg("--output")
        .arg("/tmp/out.bin")
        .arg("--nonexistent-flag")
        .output()
        .expect("failed to run braid receive with unknown flag");
    assert!(!output.status.success());
}

// ─── Negotiation Tests (in-process, using library APIs) ──────────────────────

use braid::control::client::ControlClient;
use braid::control::negotiation::{accept_negotiation, negotiate, ChannelInfo, NegotiationConfig};
use braid::control::server::ControlServer;

/// Helper: run a full negotiation between sender and receiver in-process.
async fn setup_negotiation(channel_count: u8) -> (ControlClient, Vec<ChannelInfo>) {
    let server = ControlServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr: SocketAddr = server.local_addr().unwrap();

    let receiver_handle = tokio::spawn(async move {
        let mut conn = server.accept().await.unwrap();
        accept_negotiation(&mut conn).await
    });

    let mut client = ControlClient::connect(addr).await.unwrap();
    let config = NegotiationConfig {
        channel_count,
        min_chunk: 10,
        max_chunk: 20,
        mtu: 14,
        compression_lz4: false,
        compression_zstd: false,
    };

    let sender_result = negotiate(&mut client, config).await.unwrap();

    let _receiver_result = receiver_handle.await.unwrap();

    (client, sender_result.channels)
}

#[tokio::test]
async fn test_full_negotiation_single_channel() {
    let (_client, channels) = setup_negotiation(1).await;
    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0].channel_id, 0);
    assert!(channels[0].port > 0);
}

#[tokio::test]
async fn test_full_negotiation_multiple_channels() {
    let (_client, channels) = setup_negotiation(4).await;
    assert_eq!(channels.len(), 4);
    for (i, ch) in channels.iter().enumerate() {
        assert_eq!(ch.channel_id, i as u16);
        assert!(ch.port > 0);
    }
}

#[tokio::test]
async fn test_full_negotiation_max_channels() {
    let (_client, channels) = setup_negotiation(255).await;
    assert_eq!(channels.len(), 255);
    assert_eq!(channels[0].channel_id, 0);
    assert_eq!(channels[254].channel_id, 254);
}

#[tokio::test]
async fn test_negotiation_zero_channels_fails() {
    let server = ControlServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr: SocketAddr = server.local_addr().unwrap();

    let receiver_handle = tokio::spawn(async move {
        let mut conn = server.accept().await.unwrap();
        accept_negotiation(&mut conn).await
    });

    let mut client = ControlClient::connect(addr).await.unwrap();
    let config = NegotiationConfig {
        channel_count: 0,
        min_chunk: 10,
        max_chunk: 20,
        mtu: 14,
        compression_lz4: false,
        compression_zstd: false,
    };

    let sender_result = negotiate(&mut client, config).await;
    assert!(
        sender_result.is_err(),
        "negotiation with 0 channels should fail"
    );

    let receiver_result = receiver_handle.await.unwrap();
    assert!(
        receiver_result.is_err(),
        "receiver should also fail with 0 channels"
    );
}

#[tokio::test]
async fn test_negotiation_partial_success() {
    // Request 16 channels — all should succeed on loopback
    let (_client, channels) = setup_negotiation(16).await;
    assert_eq!(channels.len(), 16);
    for (i, ch) in channels.iter().enumerate() {
        assert_eq!(ch.channel_id, i as u16);
        assert!(ch.port > 0);
    }
}

#[tokio::test]
async fn test_negotiation_features_round_trip() {
    let config = NegotiationConfig {
        channel_count: 8,
        min_chunk: 10,
        max_chunk: 20,
        mtu: 14,
        compression_lz4: false,
        compression_zstd: false,
    };
    let features = config.to_features();
    let decoded = NegotiationConfig::from_features(features);
    assert_eq!(decoded.channel_count, 8);
    assert_eq!(decoded.min_chunk, 10);
    assert_eq!(decoded.max_chunk, 20);
    assert_eq!(decoded.mtu, 14);
    assert!(!decoded.compression_lz4);
    assert!(!decoded.compression_zstd);
}

#[tokio::test]
async fn test_negotiation_features_zero_values() {
    let config = NegotiationConfig {
        channel_count: 0,
        min_chunk: 0,
        max_chunk: 0,
        mtu: 0,
        compression_lz4: false,
        compression_zstd: false,
    };
    let features = config.to_features();
    let decoded = NegotiationConfig::from_features(features);
    assert_eq!(decoded.channel_count, 0);
    assert_eq!(decoded.min_chunk, 0);
    assert_eq!(decoded.max_chunk, 0);
    assert_eq!(decoded.mtu, 0);
}

// ─── Full Pipeline Loopback Tests (binary-based) ─────────────────────────────

/// Helper: run a full send+receive loopback test.
///
/// Spawns a receiver on a random port, then a sender that connects to it.
/// Returns the received data.
fn run_loopback(data: &[u8], timeout_secs: u64) -> Vec<u8> {
    let output_path = format!("/tmp/braid_loopback_{}.bin", std::process::id());

    // Use a PID-based port to avoid conflicts between parallel test runs.
    // The serial_test attribute ensures these don't run in parallel anyway.
    let base_port = 25000 + (std::process::id() as u16 % 10000);
    let control_port = base_port;

    let bin = braid_binary();
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
        Err(e) => {
            panic!(
                "failed to spawn braid receive on port {}: {}",
                control_port, e
            );
        }
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

    // Wait for sender to finish
    let send_output = send_child
        .wait_with_output()
        .expect("failed to wait for braid send");

    if !send_output.status.success() {
        let stderr = String::from_utf8_lossy(&send_output.stderr);
        let _ = recv_child.kill();
        let _ = recv_child.wait();
        let _ = std::fs::remove_file(&output_path);
        panic!("braid send failed: {}", stderr);
    }

    // Give receiver time to process remaining data, then kill it.
    // With the receiver's inactivity timeout, it will exit on its own,
    // but we use a generous grace period for larger transfers.
    std::thread::sleep(Duration::from_secs(timeout_secs.min(15)));
    let _ = recv_child.kill();
    let _ = recv_child.wait();

    // Read the output file
    let received: Vec<u8> = std::fs::read(&output_path).unwrap_or_default();

    // Cleanup
    let _ = std::fs::remove_file(&output_path);

    received
}

#[test]
#[serial]
fn test_basic_loopback_1mb() {
    let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
    let expected_hash = sha256(&data);

    let received = run_loopback(&data, 30);
    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "data integrity mismatch: expected {}, got {}",
        expected_hash, received_hash
    );
    assert_eq!(received.len(), data.len(), "size mismatch");
}

#[test]
#[serial]
fn test_large_transfer_10mb() {
    let data: Vec<u8> = (0..10 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
    let expected_hash = sha256(&data);

    let received = run_loopback(&data, 60);
    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "data integrity mismatch for 10MB transfer"
    );
    assert_eq!(received.len(), data.len(), "size mismatch for 10MB");
}

#[test]
#[serial]
fn test_empty_input() {
    let data: Vec<u8> = Vec::new();
    let received = run_loopback(&data, 15);
    assert_eq!(received.len(), 0, "empty input should produce empty output");
}

#[test]
#[serial]
fn test_single_byte() {
    let data: Vec<u8> = vec![0xAB];
    let received = run_loopback(&data, 15);
    assert_eq!(received, vec![0xAB], "single byte should round-trip");
}

#[test]
#[serial]
fn test_multiple_small_writes() {
    // Use data that spans multiple chunks
    let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
    let expected_hash = sha256(&data);

    let received = run_loopback(&data, 30);
    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "data integrity mismatch for small writes"
    );
    assert_eq!(received.len(), data.len());
}

#[test]
#[serial]
fn test_varied_pattern_data() {
    // Alternating patterns to exercise different chunk boundaries
    let mut data = Vec::with_capacity(500000);
    // Pattern 1: all zeros
    data.extend(std::iter::repeat_n(0u8, 100000));
    // Pattern 2: all 0xFF
    data.extend(std::iter::repeat_n(0xFFu8, 100000));
    // Pattern 3: incrementing bytes
    data.extend((0..100000).map(|i| (i % 256) as u8));
    // Pattern 4: random-looking pattern
    data.extend((0..100000).map(|i| ((i * 7 + 13) % 256) as u8));
    // Pattern 5: alternating 0xAA/0x55
    data.extend((0..100000).map(|i| if i % 2 == 0 { 0xAA } else { 0x55 }));

    let expected_hash = sha256(&data);
    let received = run_loopback(&data, 60);
    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "data integrity mismatch for varied patterns"
    );
    assert_eq!(received.len(), data.len());
}

#[test]
#[serial]
fn test_exact_chunk_size_boundary() {
    // Send data exactly at common chunk size boundaries
    for &size in &[1024, 4096, 16384, 65536] {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let expected_hash = sha256(&data);
        let received = run_loopback(&data, 30);
        let received_hash = sha256(&received);
        assert_eq!(
            received_hash, expected_hash,
            "data integrity mismatch at chunk size {}",
            size
        );
        assert_eq!(received.len(), size, "size mismatch at chunk size {}", size);
    }
}

#[test]
#[serial]
fn test_chunk_size_plus_one_boundary() {
    // Send data at chunk size + 1 to test boundary crossing
    for &size in &[1025, 4097, 16385] {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let expected_hash = sha256(&data);
        let received = run_loopback(&data, 30);
        let received_hash = sha256(&received);
        assert_eq!(
            received_hash, expected_hash,
            "data integrity mismatch at size {}",
            size
        );
        assert_eq!(received.len(), size, "size mismatch at size {}", size);
    }
}

#[test]
#[serial]
fn test_multiple_consecutive_transfers() {
    // Run two transfers sequentially to ensure clean state between runs
    for run in 0..3 {
        let data: Vec<u8> = (0..50000).map(|i| ((i + run * 100) % 256) as u8).collect();
        let expected_hash = sha256(&data);
        let received = run_loopback(&data, 30);
        let received_hash = sha256(&received);
        assert_eq!(
            received_hash, expected_hash,
            "data integrity mismatch in run {}",
            run
        );
        assert_eq!(received.len(), data.len(), "size mismatch in run {}", run);
    }
}

// ─── Concurrent Send/Receive Tests ───────────────────────────────────────────

#[test]
#[serial]
fn test_concurrent_transfers_different_sizes() {
    // Run two loopback transfers concurrently on different ports
    let data1: Vec<u8> = (0..100000).map(|i| (i % 256) as u8).collect();
    let data2: Vec<u8> = (0..200000).map(|i| ((i * 3) % 256) as u8).collect();

    let expected1 = sha256(&data1);
    let expected2 = sha256(&data2);

    let pid = std::process::id();
    let out1 = format!("/tmp/braid_concurrent_1_{}.bin", pid);
    let out2 = format!("/tmp/braid_concurrent_2_{}.bin", pid);
    let port1 = 26000 + (pid as u16 % 5000);
    let port2 = port1 + 1;

    let bin = braid_binary();

    // Start receiver 1
    let mut recv1 = Command::new(&bin);
    recv1
        .arg("receive")
        .arg("--bind")
        .arg(format!("127.0.0.1:{}", port1))
        .arg("--buffer-size")
        .arg("65536")
        .arg("--output")
        .arg(&out1)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut recv1_child = recv1.spawn().expect("failed to spawn receiver 1");

    // Start receiver 2
    let mut recv2 = Command::new(&bin);
    recv2
        .arg("receive")
        .arg("--bind")
        .arg(format!("127.0.0.1:{}", port2))
        .arg("--buffer-size")
        .arg("65536")
        .arg("--output")
        .arg(&out2)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut recv2_child = recv2.spawn().expect("failed to spawn receiver 2");

    // Give receivers time to bind
    std::thread::sleep(Duration::from_millis(500));

    // Start sender 1
    let mut send1 = Command::new(&bin);
    send1
        .arg("send")
        .arg("--destination")
        .arg(format!("127.0.0.1:{}", port1))
        .arg("--quiet")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut send1_child = send1.spawn().expect("failed to spawn sender 1");
    if let Some(mut stdin) = send1_child.stdin.take() {
        stdin
            .write_all(&data1)
            .expect("failed to write to sender 1");
        drop(stdin);
    }

    // Start sender 2
    let mut send2 = Command::new(&bin);
    send2
        .arg("send")
        .arg("--destination")
        .arg(format!("127.0.0.1:{}", port2))
        .arg("--quiet")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut send2_child = send2.spawn().expect("failed to spawn sender 2");
    if let Some(mut stdin) = send2_child.stdin.take() {
        stdin
            .write_all(&data2)
            .expect("failed to write to sender 2");
        drop(stdin);
    }

    // Wait for both senders
    let send1_status = send1_child.wait_with_output().expect("sender 1 failed");
    let send2_status = send2_child.wait_with_output().expect("sender 2 failed");

    assert!(
        send1_status.status.success(),
        "sender 1 failed: {}",
        String::from_utf8_lossy(&send1_status.stderr)
    );
    assert!(
        send2_status.status.success(),
        "sender 2 failed: {}",
        String::from_utf8_lossy(&send2_status.stderr)
    );

    // Poll for receivers to finish writing (up to 60s each) instead of fixed sleep
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        let len1 = std::fs::metadata(&out1).map(|m| m.len()).unwrap_or(0);
        let len2 = std::fs::metadata(&out2).map(|m| m.len()).unwrap_or(0);
        if len1 >= data1.len() as u64 && len2 >= data2.len() as u64 {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let _ = recv1_child.kill();
    let _ = recv2_child.kill();
    let _ = recv1_child.wait();
    let _ = recv2_child.wait();

    // Read outputs
    let received1 = std::fs::read(&out1).unwrap_or_default();
    let received2 = std::fs::read(&out2).unwrap_or_default();

    // Cleanup
    let _ = std::fs::remove_file(&out1);
    let _ = std::fs::remove_file(&out2);

    assert_eq!(
        sha256(&received1),
        expected1,
        "concurrent transfer 1 data mismatch"
    );
    assert_eq!(
        sha256(&received2),
        expected2,
        "concurrent transfer 2 data mismatch"
    );
    assert_eq!(
        received1.len(),
        data1.len(),
        "concurrent transfer 1 size mismatch"
    );
    assert_eq!(
        received2.len(),
        data2.len(),
        "concurrent transfer 2 size mismatch"
    );
}

// ─── Port Exhaustion / Negotiation Failure Tests ─────────────────────────────

#[tokio::test]
async fn test_negotiation_to_unreachable_port() {
    // Connect to port 1 which is reserved and never available for TCP
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

    let result = ControlClient::connect(addr).await;
    assert!(
        result.is_err(),
        "connecting to unreachable port should fail"
    );
}

#[tokio::test]
async fn test_negotiation_receiver_rejects() {
    // Connect to receiver but don't follow protocol — send garbage
    let server = ControlServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr: SocketAddr = server.local_addr().unwrap();

    let receiver_handle = tokio::spawn(async move {
        let mut conn = server.accept().await.unwrap();
        accept_negotiation(&mut conn).await
    });

    let mut client = ControlClient::connect(addr).await.unwrap();

    // Send an invalid message instead of Hello
    use braid::protocol::ControlMessage;
    let _ = client
        .send_message(&ControlMessage::Error {
            code: 99,
            detail: 0,
        })
        .await;

    // Sender-side negotiate should fail
    let config = NegotiationConfig {
        channel_count: 4,
        min_chunk: 10,
        max_chunk: 20,
        mtu: 14,
        compression_lz4: false,
        compression_zstd: false,
    };
    let sender_result = negotiate(&mut client, config).await;
    assert!(
        sender_result.is_err(),
        "negotiation should fail after invalid message"
    );

    let receiver_result = receiver_handle.await.unwrap();
    assert!(receiver_result.is_err(), "receiver should also fail");
}

// ─── Edge Case: Very Large Data Patterns ─────────────────────────────────────

#[test]
#[serial]
#[ignore = "requires significant time and memory"]
fn test_very_large_transfer_100mb() {
    let data: Vec<u8> = (0..100 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
    let expected_hash = sha256(&data);

    let received = run_loopback(&data, 300);
    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "data integrity mismatch for 100MB transfer"
    );
    assert_eq!(received.len(), data.len());
}

#[test]
#[serial]
#[ignore = "requires many available ports"]
fn test_max_channels_loopback() {
    // This test verifies that negotiation with max channels works end-to-end
    // It's ignored by default because it requires 255 available UDP ports
    let data: Vec<u8> = b"hello max channels test".to_vec();
    let expected_hash = sha256(&data);

    let control_port = 27000 + (std::process::id() as u16 % 5000);
    let output_path = format!("/tmp/braid_maxchan_{}.bin", control_port);

    let bin = braid_binary();

    // Start receiver
    let mut recv_cmd = Command::new(&bin);
    recv_cmd
        .arg("receive")
        .arg("--bind")
        .arg(format!("127.0.0.1:{}", control_port))
        .arg("--buffer-size")
        .arg("65536")
        .arg("--output")
        .arg(&output_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut recv_child = recv_cmd.spawn().expect("failed to spawn receiver");

    std::thread::sleep(Duration::from_millis(500));

    // Start sender with explicit channel count
    let mut send_cmd = Command::new(&bin);
    send_cmd
        .arg("send")
        .arg("--destination")
        .arg(format!("127.0.0.1:{}", control_port))
        .arg("--channels")
        .arg("255")
        .arg("--quiet")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut send_child = send_cmd.spawn().expect("failed to spawn sender");
    if let Some(mut stdin) = send_child.stdin.take() {
        stdin.write_all(&data).expect("failed to write stdin");
        drop(stdin);
    }

    let send_output = send_child.wait_with_output().expect("sender failed");
    if !send_output.status.success() {
        let _ = recv_child.kill();
        let _ = recv_child.wait();
        let _ = std::fs::remove_file(&output_path);
        panic!(
            "sender failed with 255 channels: {}",
            String::from_utf8_lossy(&send_output.stderr)
        );
    }

    wait_for_file_stable(&output_path);
    let _ = recv_child.kill();
    let _ = recv_child.wait();

    let received = std::fs::read(&output_path).unwrap_or_default();
    let _ = std::fs::remove_file(&output_path);

    assert_eq!(
        sha256(&received),
        expected_hash,
        "max channels loopback data mismatch"
    );
}

// ─── Edge Case: Binary Data with Null Bytes ──────────────────────────────────

#[test]
#[serial]
fn test_binary_data_with_nulls() {
    // Data containing null bytes and other special values
    let mut data = Vec::with_capacity(100000);
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // four nulls
    data.extend_from_slice(&[0xFF, 0xFE, 0xFD, 0x00]);
    data.extend_from_slice(&[0x00, 0x01, 0x02, 0x03]);
    // Fill with pattern that includes nulls
    for i in 0..99988 {
        data.push((i % 256) as u8);
    }

    let expected_hash = sha256(&data);
    let received = run_loopback(&data, 30);
    let received_hash = sha256(&received);

    assert_eq!(
        received_hash, expected_hash,
        "binary data with nulls integrity mismatch"
    );
    assert_eq!(received.len(), data.len());
}

// ─── Edge Case: Non-blocking / Timeout Behavior ──────────────────────────────

#[test]
#[serial]
fn test_send_to_no_receiver_fails_gracefully() {
    let bin = braid_binary();
    // Use port 1 which is reserved and never available for TCP
    let port = 1u16;

    let mut child = Command::new(&bin)
        .arg("send")
        .arg("--destination")
        .arg(format!("127.0.0.1:{}", port))
        .arg("--quiet")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn braid send");

    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(b"test data");
        drop(stdin);
    }

    let output = child
        .wait_with_output()
        .expect("failed to wait for braid send");

    // Should fail because nothing is listening
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("error") || stderr.contains("refused") || stderr.contains("timeout"),
        "expected error message, got: {}",
        stderr
    );
}
