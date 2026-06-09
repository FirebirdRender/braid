use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time;
use tracing::warn;

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

    #[cfg(test)]
    pub fn with_accept_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
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

    /// Attempt to accept a connection with retries and exponential backoff.
    ///
    /// Tries `accept()` up to `max_retries` times. On timeout, waits with
    /// exponential backoff (`delay` doubled each attempt, capped at 60s)
    /// and retries. Logs each retry at `warn!` level.
    ///
    /// Returns `ControlError::Timeout` after all retries are exhausted.
    pub async fn accept_with_retry(
        &self,
        max_retries: u32,
        initial_delay: Duration,
    ) -> Result<ControlConnection> {
        let mut delay = initial_delay;
        for attempt in 1..=max_retries {
            match self.accept().await {
                Ok(conn) => return Ok(conn),
                Err(ControlError::Timeout) => {
                    if attempt == max_retries {
                        warn!("accept timed out after {max_retries} retries, giving up");
                        return Err(ControlError::Timeout);
                    }
                    warn!(
                        "accept timed out (attempt {attempt}/{max_retries}), retrying in {delay:?}"
                    );
                    time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(60));
                }
                Err(e) => return Err(e),
            }
        }
        Err(ControlError::Timeout)
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
