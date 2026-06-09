use std::net::SocketAddr;
use std::time::Duration;

use crate::control::client::{ControlClient, ControlError};
use crate::control::server::ControlServer;
use crate::protocol::ControlMessage;

#[tokio::test]
async fn tcp_control_round_trip() {
    let server = ControlServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr: SocketAddr = server.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let mut conn = server.accept().await.unwrap();
        let got = conn.recv_message().await.unwrap();
        assert_eq!(
            got,
            ControlMessage::Hello {
                protocol_version: 1,
                features: 0xAA
            }
        );
        conn.send_message(&ControlMessage::Ack { sequence_number: 7 })
            .await
            .unwrap();
    });

    let mut client = ControlClient::connect(addr).await.unwrap();
    client
        .send_message(&ControlMessage::Hello {
            protocol_version: 1,
            features: 0xAA,
        })
        .await
        .unwrap();
    let got = client.recv_message().await.unwrap();
    assert_eq!(got, ControlMessage::Ack { sequence_number: 7 });

    server_task.await.unwrap();
}

#[tokio::test]
async fn tcp_control_connection_timeout() {
    let addr: SocketAddr = "127.0.0.1:9".parse().unwrap();
    let err = ControlClient::connect_with_timeout(addr, Duration::from_millis(50))
        .await
        .err()
        .unwrap();
    match err {
        ControlError::Timeout | ControlError::Io(_) => {}
        other => panic!("unexpected error: {:?}", other),
    }
}

#[tokio::test]
async fn accept_with_retry_returns_connection_on_success() {
    let server = ControlServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr: SocketAddr = server.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let conn = server
            .accept_with_retry(3, Duration::from_millis(10))
            .await
            .unwrap();
        // Connection accepted successfully
        assert!(std::mem::size_of_val(&conn) > 0);
    });

    let mut client = ControlClient::connect(addr).await.unwrap();
    client
        .send_message(&ControlMessage::Hello {
            protocol_version: 1,
            features: 0,
        })
        .await
        .unwrap();

    server_task.await.unwrap();
}

#[tokio::test]
async fn accept_with_retry_retries_on_timeout_then_succeeds() {
    let server = ControlServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap()
        .with_accept_timeout(Duration::from_millis(50));
    let addr: SocketAddr = server.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let conn = server
            .accept_with_retry(3, Duration::from_millis(10))
            .await
            .unwrap();
        assert!(std::mem::size_of_val(&conn) > 0);
    });

    // Wait longer than the accept timeout so the first attempt times out,
    // then connect before all retries are exhausted.
    tokio::time::sleep(Duration::from_millis(70)).await;
    let mut client = ControlClient::connect(addr).await.unwrap();
    client
        .send_message(&ControlMessage::Hello {
            protocol_version: 1,
            features: 0,
        })
        .await
        .unwrap();

    server_task.await.unwrap();
}

#[tokio::test]
async fn accept_with_retry_exhausts_retries_and_returns_timeout() {
    let server = ControlServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap()
        .with_accept_timeout(Duration::from_millis(20));

    let err = server
        .accept_with_retry(2, Duration::from_millis(1))
        .await
        .err()
        .unwrap();

    assert!(matches!(err, ControlError::Timeout));
}
