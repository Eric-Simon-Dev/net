use std::{thread, time::Duration};

use bytes::BytesMut;

use crate::{client, server};

/// Ensure that a server and a client can successfully bind, listen, and connect.
#[test]
fn connection() {
    let mut server = server::Server::new("0:12012").unwrap();
    let (_, _) = server.listen().unwrap();
    let mut client = client::Client::new("0:12013").unwrap();
    let (_, _) = client.connect("0:12012").unwrap();
}

/// Perform multiple message exchanges between a client and a server.
///
/// The client and server run on separate threads and bounce a single-byte
/// message back and forth, incrementing it each time.
#[test]
fn multiple_exchanges() {
    // Spawn server first.
    thread::spawn(|| {
        run_server();
    });

    // Give the server time to start listening so the client connection succeeds.
    thread::sleep(Duration::from_millis(100));

    thread::spawn(|| {
        run_client();
    });
}

fn run_server() {
    use crate::server::Server;

    let mut server = Server::new("0:12014").unwrap();
    let (sender, receiver) = server.listen().unwrap();

    // Receive a message and send back `msg + 1` sixteen times.
    for _ in 0..16 {
        let mut msg = receiver.recv().unwrap();
        msg.data[0] += 1;
        sender.send(msg).unwrap();
    }

    // Final receive: ensure the expected value is reached.
    let msg = receiver.recv().unwrap();
    assert_eq!(msg.data[0], 32);
}

fn run_client() {
    use crate::client::{Client, Guarantees, Message};

    let mut client = Client::new("0:12015").unwrap();
    let (sender, receiver) = client.connect("0:12014").unwrap();

    // Initial message with value 0.
    let msg = Message {
        data: BytesMut::zeroed(1),
        channel: 0,
        guarantees: Guarantees::None,
    };
    sender.send(msg).unwrap();

    // Receive and send back `msg + 1` sixteen times.
    for _ in 0..16 {
        let mut msg = receiver.recv().unwrap();
        msg.data[0] += 1;
        sender.send(msg).unwrap();
    }
}
