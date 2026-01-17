use std::{net::ToSocketAddrs, thread, time::Duration};

use bytes::BytesMut;

use crate::{client, server};

/// Perform multiple message exchanges between a client and a server.
///
/// The client and server run on separate threads and bounce a single-byte
/// message back and forth, incrementing it each time.
#[test]
fn multiple_exchanges_no_guarantees() {
    // ---- Addresses ----
    let server_addr = "0:12013";
    let client_addr = "0:0";

    // Spawn server first.
    thread::spawn(move || {
        run_server(server_addr, server::Guarantees::None);
    });

    // Give the server time to start listening,
    // so the client connection succeeds.
    thread::sleep(Duration::from_millis(100));

    // Spawn client.
    thread::spawn(move || {
        run_client(client_addr, server_addr, client::Guarantees::None);
    });
}

/// Perform multiple message exchanges between a client and a server.
///
/// The client and server run on separate threads and bounce a single-byte
/// message back and forth, incrementing it each time.
#[test]
fn multiple_exchanges_delivery_guarantee() {
    // ---- Addresses ----
    let server_addr = "0:12014";
    let client_addr = "0:0";

    // Spawn server first.
    thread::spawn(move || {
        run_server(server_addr, server::Guarantees::Delivery);
    });

    // Give the server time to start listening,
    // so the client connection succeeds.
    thread::sleep(Duration::from_millis(100));

    // Spawn client.
    thread::spawn(move || {
        run_client(client_addr, server_addr, client::Guarantees::Delivery);
    });
}

fn run_server(server_addr: impl ToSocketAddrs, guarantees: server::Guarantees) {
    use server::{Incoming, Outgoing, OutgoingMessage, listen};

    let (outgoing, incoming, waker) = listen(server_addr).unwrap();

    // Receive and send back `msg + 1` sixteen times.
    for _ in 0..16 {
        let Incoming::Message(mut msg) = incoming.recv().unwrap() else {
            panic!("receive a notification");
        };
        msg.data[0] += 1;
        outgoing
            .send(Outgoing::Message(OutgoingMessage {
                data: msg.data,
                channel: msg.channel,
                client_id: msg.client_id,
                guarantees,
            }))
            .unwrap();
        waker.notify_reactor().unwrap();
    }

    // Final receive: ensure the expected value is reached.
    let Incoming::Message(msg) = incoming.recv().unwrap() else {
        panic!("receive a notification");
    };
    assert_eq!(msg.data[0], 32);
}

fn run_client(
    client_addr: impl ToSocketAddrs,
    server_addr: impl ToSocketAddrs,
    guarantees: client::Guarantees,
) {
    use client::{Incoming, Outgoing, OutgoingMessage, connect};

    let (outgoing, incoming, waker) = connect(client_addr, server_addr).unwrap();

    // Initial message with value 0.
    outgoing
        .send(Outgoing::Message(OutgoingMessage {
            data: BytesMut::zeroed(1),
            channel: 0,
            guarantees,
        }))
        .unwrap();
    waker.notify_reactor().unwrap();

    // Receive and send back `msg + 1` sixteen times.
    for _ in 0..16 {
        let Incoming::Message(mut msg) = incoming.recv().unwrap();
        msg.data[0] += 1;
        outgoing
            .send(Outgoing::Message(OutgoingMessage {
                data: msg.data,
                channel: msg.channel,
                guarantees,
            }))
            .unwrap();
        waker.notify_reactor().unwrap();
    }
}
