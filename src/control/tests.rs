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
