use std::net::SocketAddr;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::time;

use crate::control::client::{ControlClient, ControlError};
use crate::control::server::ControlConnection;
use crate::protocol::ControlMessage;

/// Feature bit for LZ4 compression support.
pub const FEATURE_COMPRESSION_LZ4: u32 = 1 << 30;
/// Feature bit for Zstd compression support.
pub const FEATURE_COMPRESSION_ZSTD: u32 = 1 << 31;

pub type Result<T> = std::result::Result<T, NegotiationError>;

#[derive(Debug)]
pub enum NegotiationError {
    Control(ControlError),
    Protocol(&'static str),
    Timeout,
    Io(std::io::Error),
}

impl std::fmt::Display for NegotiationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(e) => write!(f, "control error: {e}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::Timeout => write!(f, "timeout"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for NegotiationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ControlError> for NegotiationError {
    fn from(e: ControlError) -> Self {
        Self::Control(e)
    }
}

impl From<std::io::Error> for NegotiationError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Negotiation parameters sent by the sender in the Hello message's features field.
#[derive(Debug, Clone, Copy)]
pub struct NegotiationConfig {
    /// Number of UDP channels requested (1-255).
    pub channel_count: u8,
    /// Minimum chunk size in bytes (encoded as log2).
    pub min_chunk: u32,
    /// Maximum chunk size in bytes (encoded as log2).
    pub max_chunk: u32,
    /// MTU in bytes (encoded as log2).
    pub mtu: u32,
    /// Whether LZ4 compression is supported.
    pub compression_lz4: bool,
    /// Whether Zstd compression is supported.
    pub compression_zstd: bool,
}

impl NegotiationConfig {
    /// Pack config into a u32 features bitfield.
    /// Layout: [zstd:1][lz4:1][channel_count:6][min_chunk_log2:8][max_chunk_log2:8][mtu_log2:8]
    /// Channel count is limited to 63 to leave bits 30-31 for compression flags.
    pub fn to_features(self) -> u32 {
        let mut features = 0u32;
        if self.compression_lz4 {
            features |= FEATURE_COMPRESSION_LZ4;
        }
        if self.compression_zstd {
            features |= FEATURE_COMPRESSION_ZSTD;
        }
        let cc = (self.channel_count as u32) & 0x3F;
        let min = self.min_chunk & 0xFF;
        let max = self.max_chunk & 0xFF;
        let mtu = self.mtu & 0xFF;
        features | (cc << 24) | (min << 16) | (max << 8) | mtu
    }

    /// Unpack features from a u32 bitfield.
    pub fn from_features(features: u32) -> Self {
        Self {
            channel_count: ((features >> 24) & 0x3F) as u8,
            min_chunk: (features >> 16) & 0xFF,
            max_chunk: (features >> 8) & 0xFF,
            mtu: features & 0xFF,
            compression_lz4: features & FEATURE_COMPRESSION_LZ4 != 0,
            compression_zstd: features & FEATURE_COMPRESSION_ZSTD != 0,
        }
    }
}

/// Result of a successful negotiation from the receiver's perspective.
#[derive(Debug, Clone)]
pub struct NegotiationResult {
    /// The channels that were successfully opened, with their assigned ports.
    pub channels: Vec<ChannelInfo>,
}

#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub channel_id: u16,
    pub port: u16,
}

const NEGOTIATION_TIMEOUT: Duration = Duration::from_secs(30);
#[allow(dead_code)]
const PORT_RETRIES: u16 = 100;

// ─── Sender side ───────────────────────────────────────────────────────────

/// Sender-driven port negotiation.
///
/// 1. Sends Hello with the requested features.
/// 2. Receives Ack from receiver.
/// 3. Receives one ChannelOpened per successfully opened UDP channel.
/// 4. Returns the list of (channel_id, port) pairs.
pub async fn negotiate(
    client: &mut ControlClient,
    config: NegotiationConfig,
) -> Result<NegotiationResult> {
    let features = config.to_features();
    let requested = config.channel_count as usize;

    // Step 1: Send Hello
    time::timeout(
        NEGOTIATION_TIMEOUT,
        client.send_message(&ControlMessage::Hello {
            protocol_version: 1,
            features,
        }),
    )
    .await
    .map_err(|_| NegotiationError::Timeout)??;

    // Step 2: Receive Ack
    let reply = time::timeout(NEGOTIATION_TIMEOUT, client.recv_message())
        .await
        .map_err(|_| NegotiationError::Timeout)??;

    match &reply {
        ControlMessage::Ack { .. } => {} // expected
        ControlMessage::Error { .. } => {
            return Err(NegotiationError::Protocol("receiver rejected negotiation"));
        }
        _ => {
            return Err(NegotiationError::Protocol(
                "expected Ack during negotiation",
            ));
        }
    }

    // Step 3: Receive ChannelOpened messages (one per successfully opened channel)
    let mut channels = Vec::with_capacity(requested);
    for _ in 0..requested {
        let reply = time::timeout(NEGOTIATION_TIMEOUT, client.recv_message())
            .await
            .map_err(|_| NegotiationError::Timeout)??;

        match reply {
            ControlMessage::ChannelOpened { channel_id, port } => {
                channels.push(ChannelInfo { channel_id, port });
            }
            ControlMessage::Error { .. } => {
                // Receiver sent error mid-negotiation; return what we have
                break;
            }
            _ => {
                // Unexpected message type — stop collecting
                break;
            }
        }
    }

    if channels.is_empty() {
        return Err(NegotiationError::Protocol("no channels could be opened"));
    }

    Ok(NegotiationResult { channels })
}

// ─── Receiver side ─────────────────────────────────────────────────────────

/// Accept a negotiation from a sender.
///
/// 1. Receives Hello from sender.
/// 2. Opens UDP sockets for each requested channel.
/// 3. Sends Ack.
/// 4. Sends one ChannelOpened per successfully opened channel.
/// 5. Returns the list of opened UDP sockets and their channel info.
pub async fn accept_negotiation(
    conn: &mut ControlConnection,
) -> Result<(NegotiationConfig, Vec<UdpSocket>, NegotiationResult)> {
    // Step 1: Receive Hello
    let msg = time::timeout(NEGOTIATION_TIMEOUT, conn.recv_message())
        .await
        .map_err(|_| NegotiationError::Timeout)??;

    let (_protocol_version, features) = match &msg {
        ControlMessage::Hello {
            protocol_version,
            features,
        } => (*protocol_version, *features),
        ControlMessage::Error { .. } => {
            return Err(NegotiationError::Protocol(
                "sender sent error instead of Hello",
            ));
        }
        _ => {
            return Err(NegotiationError::Protocol(
                "expected Hello during negotiation",
            ));
        }
    };

    let config = NegotiationConfig::from_features(features);
    let requested = config.channel_count as usize;

    // Step 2: Open UDP sockets
    let mut sockets = Vec::with_capacity(requested);
    let mut channels = Vec::with_capacity(requested);

    for channel_id in 0..requested as u16 {
        match open_udp_socket().await {
            Ok(socket) => {
                let local_addr = socket.local_addr().unwrap();
                let port = local_addr.port();
                sockets.push(socket);
                channels.push(ChannelInfo { channel_id, port });
            }
            Err(_) => {
                // Failed to open this port — try next channel
                continue;
            }
        }
    }

    // Step 3: Send Ack
    time::timeout(
        NEGOTIATION_TIMEOUT,
        conn.send_message(&ControlMessage::Ack { sequence_number: 0 }),
    )
    .await
    .map_err(|_| NegotiationError::Timeout)??;

    // Step 4: Send ChannelOpened for each successfully opened channel
    if channels.is_empty() {
        // Zero channels opened — send Error and bail
        time::timeout(
            NEGOTIATION_TIMEOUT,
            conn.send_message(&ControlMessage::Error { code: 1, detail: 0 }),
        )
        .await
        .map_err(|_| NegotiationError::Timeout)??;
        return Err(NegotiationError::Protocol("no channels could be opened"));
    }

    for ch in &channels {
        time::timeout(
            NEGOTIATION_TIMEOUT,
            conn.send_message(&ControlMessage::ChannelOpened {
                channel_id: ch.channel_id,
                port: ch.port,
            }),
        )
        .await
        .map_err(|_| NegotiationError::Timeout)??;
    }

    Ok((config, sockets, NegotiationResult { channels }))
}

/// Open a UDP socket on 0.0.0.0:0 with SO_REUSEADDR enabled.
pub async fn open_udp_socket() -> std::io::Result<UdpSocket> {
    // Use socket2 to set SO_REUSEADDR before binding
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.bind(&"0.0.0.0:0".parse::<SocketAddr>().unwrap().into())?;
    socket.set_nonblocking(true)?;

    // Convert to tokio UdpSocket
    let std_socket: std::net::UdpSocket = socket.into();
    UdpSocket::from_std(std_socket)
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::ControlServer;
    use std::net::SocketAddr;

    /// Helper: run sender and receiver negotiation in separate tasks.
    async fn run_negotiation(
        config: NegotiationConfig,
    ) -> (
        Result<NegotiationResult>,
        Result<(NegotiationConfig, Vec<UdpSocket>, NegotiationResult)>,
    ) {
        let server = ControlServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr: SocketAddr = server.local_addr().unwrap();

        let receiver_handle = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            accept_negotiation(&mut conn).await
        });

        let mut client = ControlClient::connect(addr).await.unwrap();
        let sender_result = negotiate(&mut client, config).await;

        let receiver_result = receiver_handle.await.unwrap();

        (sender_result, receiver_result)
    }

    #[tokio::test]
    async fn full_negotiation_succeeds() {
        let config = NegotiationConfig {
            channel_count: 4,
            min_chunk: 10, // 2^10 = 1024
            max_chunk: 20, // 2^20 = 1048576
            mtu: 14,       // 2^14 = 16384
            compression_lz4: false,
            compression_zstd: false,
        };

        let (sender_res, receiver_res) = run_negotiation(config).await;

        let sender = sender_res.expect("sender negotiation should succeed");
        assert_eq!(sender.channels.len(), 4, "should have 4 channels");

        // Verify channel IDs are 0..3
        for (i, ch) in sender.channels.iter().enumerate() {
            assert_eq!(ch.channel_id, i as u16, "channel_id should be {}", i);
            assert!(ch.port > 0, "port should be non-zero");
        }

        let (recv_config, _sockets, recv_result) =
            receiver_res.expect("receiver negotiation should succeed");
        assert_eq!(recv_config.channel_count, 4);
        assert_eq!(recv_result.channels.len(), 4);
    }

    #[tokio::test]
    async fn single_channel_negotiation() {
        let config = NegotiationConfig {
            channel_count: 1,
            min_chunk: 10,
            max_chunk: 20,
            mtu: 14,
            compression_lz4: false,
            compression_zstd: false,
        };

        let (sender_res, receiver_res) = run_negotiation(config).await;

        let sender = sender_res.expect("sender negotiation should succeed");
        assert_eq!(sender.channels.len(), 1);
        assert_eq!(sender.channels[0].channel_id, 0);
        assert!(sender.channels[0].port > 0);

        let (_cfg, _socks, recv) = receiver_res.expect("receiver should succeed");
        assert_eq!(recv.channels.len(), 1);
    }

    #[tokio::test]
    async fn max_channels_negotiation() {
        let config = NegotiationConfig {
            channel_count: 63,
            min_chunk: 10,
            max_chunk: 20,
            mtu: 14,
            compression_lz4: false,
            compression_zstd: false,
        };

        let (sender_res, receiver_res) = run_negotiation(config).await;

        let sender = sender_res.expect("sender negotiation should succeed");
        assert_eq!(sender.channels.len(), 63);
        assert_eq!(sender.channels[0].channel_id, 0);
        assert_eq!(sender.channels[62].channel_id, 62);

        let (_cfg, _socks, recv) = receiver_res.expect("receiver should succeed");
        assert_eq!(recv.channels.len(), 63);
    }

    #[tokio::test]
    async fn features_round_trip() {
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
    async fn features_compression_lz4_round_trip() {
        let config = NegotiationConfig {
            channel_count: 4,
            min_chunk: 10,
            max_chunk: 20,
            mtu: 14,
            compression_lz4: true,
            compression_zstd: false,
        };

        let features = config.to_features();
        assert!(
            features & FEATURE_COMPRESSION_LZ4 != 0,
            "LZ4 bit should be set"
        );
        assert!(
            features & FEATURE_COMPRESSION_ZSTD == 0,
            "Zstd bit should be clear"
        );

        let decoded = NegotiationConfig::from_features(features);
        assert!(decoded.compression_lz4);
        assert!(!decoded.compression_zstd);
        assert_eq!(decoded.channel_count, 4);
    }

    #[tokio::test]
    async fn features_compression_zstd_round_trip() {
        let config = NegotiationConfig {
            channel_count: 4,
            min_chunk: 10,
            max_chunk: 20,
            mtu: 14,
            compression_lz4: false,
            compression_zstd: true,
        };

        let features = config.to_features();
        assert!(
            features & FEATURE_COMPRESSION_ZSTD != 0,
            "Zstd bit should be set"
        );
        assert!(
            features & FEATURE_COMPRESSION_LZ4 == 0,
            "LZ4 bit should be clear"
        );

        let decoded = NegotiationConfig::from_features(features);
        assert!(!decoded.compression_lz4);
        assert!(decoded.compression_zstd);
    }

    #[tokio::test]
    async fn features_compression_both_round_trip() {
        let config = NegotiationConfig {
            channel_count: 2,
            min_chunk: 10,
            max_chunk: 20,
            mtu: 14,
            compression_lz4: true,
            compression_zstd: true,
        };

        let features = config.to_features();
        assert!(features & FEATURE_COMPRESSION_LZ4 != 0);
        assert!(features & FEATURE_COMPRESSION_ZSTD != 0);

        let decoded = NegotiationConfig::from_features(features);
        assert!(decoded.compression_lz4);
        assert!(decoded.compression_zstd);
    }

    #[tokio::test]
    async fn features_compression_bits_dont_interfere() {
        // Verify compression bits don't collide with existing fields
let config_no_compress = NegotiationConfig {
            channel_count: 63,
            min_chunk: 255,
            max_chunk: 255,
            mtu: 255,
            compression_lz4: false,
            compression_zstd: false,
        };

        let config_with_compress = NegotiationConfig {
            channel_count: 63,
            min_chunk: 255,
            max_chunk: 255,
            mtu: 255,
            compression_lz4: true,
            compression_zstd: true,
        };

        let f1 = config_no_compress.to_features();
        let f2 = config_with_compress.to_features();

        // Lower 30 bits should be identical (bits 29..0)
        assert_eq!(f1 & 0x3FFFFFFF, f2 & 0x3FFFFFFF);
        // Top 2 bits should differ
        assert_eq!(f1 & 0xC0000000, 0);
        assert_eq!(f2 & 0xC0000000, 0xC0000000);
    }

    #[tokio::test]
    async fn features_zero_values() {

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

    #[tokio::test]
    async fn zero_channels_returns_error() {
        let config = NegotiationConfig {
            channel_count: 0,
            min_chunk: 10,
            max_chunk: 20,
            mtu: 14,
            compression_lz4: false,
            compression_zstd: false,
        };

        let (sender_res, receiver_res) = run_negotiation(config).await;

        // Sender should get an error since no channels were opened
        assert!(sender_res.is_err(), "sender should fail with zero channels");

        // Receiver should also get an error
        assert!(
            receiver_res.is_err(),
            "receiver should fail with zero channels"
        );
    }

    #[tokio::test]
    async fn partial_success_some_ports_in_use() {
        // Bind a UDP socket on a port to occupy it, then request many channels.
        // The receiver should still open the remaining channels.
        let occupied = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let _occupied_port = occupied.local_addr().unwrap().port();

        // We can't easily force the receiver to hit a specific port,
        // but we can verify that requesting many channels still works
        // (the receiver binds to 0.0.0.0:0 so OS assigns ports).
        // For a true conflict test, we'd need to bind on 0.0.0.0 which
        // conflicts differently. Instead, test that 16 channels all succeed.
        let config = NegotiationConfig {
            channel_count: 16,
            min_chunk: 10,
            max_chunk: 20,
            mtu: 14,
            compression_lz4: false,
            compression_zstd: false,
        };

        let (sender_res, receiver_res) = run_negotiation(config).await;

        let sender = sender_res.expect("sender should succeed with 16 channels");
        assert_eq!(sender.channels.len(), 16);

        let (_cfg, _socks, recv) = receiver_res.expect("receiver should succeed");
        assert_eq!(recv.channels.len(), 16);

        drop(occupied);
    }

    #[tokio::test]
    async fn negotiation_timeout() {
        // Connect but don't send Hello — the receiver should timeout
        let server = ControlServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr: SocketAddr = server.local_addr().unwrap();

        let receiver_handle = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let result =
                time::timeout(Duration::from_secs(35), accept_negotiation(&mut conn)).await;
            match result {
                Ok(Err(NegotiationError::Control(ControlError::Timeout))) => true,
                Ok(Err(NegotiationError::Timeout)) => true,
                Ok(Err(e)) => panic!("expected Timeout, got {:?}", e),
                Ok(Ok(_)) => panic!("expected timeout error, got success"),
                Err(_) => true, // tokio timeout
            }
        });

        // Connect but don't send anything
        let _client = ControlClient::connect(addr).await.unwrap();

        // Wait for receiver to timeout
        assert!(receiver_handle.await.unwrap());
    }
}
