use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time;

use crate::protocol::ControlMessage;

use super::client::{ControlError, Result};

#[derive(Debug)]
pub struct ControlServer {
    listener: TcpListener,
    timeout: Duration,
    heartbeat: Duration,
}

impl ControlServer {
    pub async fn bind(addr: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            timeout: Duration::from_secs(10),
            heartbeat: Duration::from_secs(5),
        })
    }

    /// Returns the local address the server is listening on.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn accept(&self) -> Result<ControlConnection> {
        let (stream, _) = time::timeout(self.timeout, self.listener.accept())
            .await
            .map_err(|_| ControlError::Timeout)??;
        Ok(ControlConnection {
            stream,
            timeout: self.timeout,
            _heartbeat: self.heartbeat,
            last_activity: std::time::Instant::now(),
        })
    }
}

#[derive(Debug)]
pub struct ControlConnection {
    stream: TcpStream,
    timeout: Duration,
    _heartbeat: Duration,
    last_activity: std::time::Instant,
}

impl ControlConnection {
    pub async fn send_message(&mut self, msg: &ControlMessage) -> Result<()> {
        let body = msg.to_bytes();
        let len =
            u32::try_from(body.len()).map_err(|_| ControlError::Protocol("message too large"))?;
        let mut frame = Vec::with_capacity(4 + body.len());
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&body);
        time::timeout(self.timeout, self.stream.write_all(&frame))
            .await
            .map_err(|_| ControlError::Timeout)??;
        self.last_activity = std::time::Instant::now();
        Ok(())
    }

    pub async fn recv_message(&mut self) -> Result<ControlMessage> {
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
        ControlMessage::try_from(body.as_slice()).map_err(ControlError::Protocol)
    }
}
