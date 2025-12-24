use std::{thread, time::Duration};

use bytes::BytesMut;

use crate::{client, server};

#[test]
fn connection() {
    let mut server = server::Server::new("0:12012").unwrap();
    let (_, _) = server.listen().unwrap();
    let mut client = client::Client::new("0:12013").unwrap();
    let (_, _) = client.connect("0:12012").unwrap();
}

#[test]
fn multiple_exchanges() {
    thread::spawn(|| {
        server_side();
    });

    // Wait for server to listen
    // so that client connection doesn't fail.
    thread::sleep(Duration::from_millis(100));

    thread::spawn(|| {
        client_side();
    });
}

fn server_side() {
    use crate::server::Server;

    let mut server = Server::new("0:12014").unwrap();
    let (sender, receiver) = server.listen().unwrap();

    // Receive and return msg + 1 16 times
    for _ in 0..16 {
        let mut msg = receiver.recv().unwrap();
        msg.data[0] += 1;
        sender.send(msg).unwrap();
    }

    // Last recv
    let msg = receiver.recv().unwrap();
    assert_eq!(msg.data[0], 32);
}

fn client_side() {
    use crate::client::{Client, Guarantees, Message};

    let mut client = Client::new("0:12015").unwrap();
    let (sender, receiver) = client.connect("0:12014").unwrap();

    // First send
    let msg = Message {
        data: BytesMut::zeroed(1),
        channel: 0,
        guarantees: Guarantees::None,
    };
    sender.send(msg).unwrap();

    // Receive and return msg + 1 16 times
    for _ in 0..16 {
        let mut msg = receiver.recv().unwrap();
        msg.data[0] += 1;
        sender.send(msg).unwrap();
    }
}
