use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time;

use crate::protocol::ControlMessage;

pub type Result<T> = std::result::Result<T, ControlError>;

#[derive(Debug)]
pub enum ControlError {
    Io(std::io::Error),
    Protocol(&'static str),
    Timeout,
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::Timeout => write!(f, "timeout"),
        }
    }
}

impl std::error::Error for ControlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ControlError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

#[derive(Debug)]
pub struct ControlClient {
    stream: TcpStream,
    timeout: Duration,
    heartbeat: Duration,
    last_activity: std::time::Instant,
}

impl ControlClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        Self::connect_with_timeout(addr, Duration::from_secs(10)).await
    }

    pub async fn connect_with_timeout(addr: SocketAddr, timeout: Duration) -> Result<Self> {
        let stream = time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ControlError::Timeout)??;
        Ok(Self {
            stream,
            timeout,
            heartbeat: Duration::from_secs(5),
            last_activity: std::time::Instant::now(),
        })
    }

    pub async fn send_message(&mut self, msg: &ControlMessage) -> Result<()> {
        self.send_frame(&msg.to_bytes()).await
    }

    pub async fn recv_message(&mut self) -> Result<ControlMessage> {
        self.recv_frame().await.and_then(|bytes| {
            ControlMessage::try_from(bytes.as_slice()).map_err(ControlError::Protocol)
        })
    }

    async fn send_frame(&mut self, body: &[u8]) -> Result<()> {
        let len =
            u32::try_from(body.len()).map_err(|_| ControlError::Protocol("message too large"))?;
        let mut frame = Vec::with_capacity(4 + body.len());
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(body);
        time::timeout(self.timeout, self.stream.write_all(&frame))
            .await
            .map_err(|_| ControlError::Timeout)??;
        self.last_activity = std::time::Instant::now();
        Ok(())
    }

    async fn recv_frame(&mut self) -> Result<Vec<u8>> {
        self.maybe_send_heartbeat().await?;
        let mut len = [0u8; 4];
        time::timeout(self.timeout, self.stream.read_exact(&mut len))
            .await
            .map_err(|_| ControlError::Timeout)??;
        let body_len = u32::from_be_bytes(len) as usize;
        let mut body = vec![0u8; body_len];
        time::timeout(self.timeout, self.stream.read_exact(&mut body))
            .await
            .map_err(|_| ControlError::Timeout)??;
        self.last_activity = std::time::Instant::now();
        Ok(body)
    }

    async fn maybe_send_heartbeat(&mut self) -> Result<()> {
        if self.last_activity.elapsed() < self.heartbeat {
            return Ok(());
        }
        self.send_frame(&ControlMessage::Ack { sequence_number: 0 }.to_bytes())
            .await
    }
}
