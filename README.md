Client-Server network library intended for realtime multiplayer games.

Provides a small [transport layer](https://en.wikipedia.org/wiki/Transport_layer)
over TCP and UDP. Goals are:
- **Simplicity**: Keep the API easy to use and specialized in realtime.
- **Performance**: Investigate the tips and tricks to optimize network performance.

I develop this for learning purposes and to have a simple API to work with.
If this project gets more serious, I will do benchmarks and comparisons
(another similar crate for example is [laminar](https://crates.io/crates/laminar)).

To ensure compatibility, use the same version of this crate on your client and server.

# Features

## Message-based API

TCP frames and UDP packets are abstracted away in favor of abstract messages.
They have a *channel byte for multiplexing* and guarantees.

Always guaranteed:
- Integrity: Data is not malformed (already ensured by TCP and UDP protocols).
- Bounds: Messages are not segmented or concatenated (only internally eventually).
- Deduplication: The same message cannot be received multiple times.

Optionally guaranteed:
- Delivery: Ensure message delivery with ACK and timers.
- DeliveryOrder: Ensure message delivery and order *relative to the channel*.

# Architecture

[Reactor-based design](https://en.wikipedia.org/wiki/Reactor_pattern):
Upon a successfull call to `listen(..)` or `connect(..)`, a reactor thread is spawned.
It waits for IO events (`polling` crate) and processes them (should dispatch work to other threads eventually i keep it simple for now).
All operations are non-blocking.

# API

The reactor/request_handler thread is spawned upon a successfull listen/connect operation.

Interfacing with it is done using channels and a *waker*
(mechanism used to wake it up from main thread).

# Usage

```rust
use std::{thread, time};
const SERVER_ADDR: &'static str = "0:12012";
const CLIENT_ADDR: &'static str = "0:0";

thread::scope(|s| {
    // Spawn server
    thread::Builder::new().name("server".to_string()).spawn_scoped(s, server_side);

    // Wait and spawn client
    thread::sleep(time::Duration::from_millis(100));
    thread::Builder::new().name("client".to_string()).spawn_scoped(s, client_side);
});

fn server_side() {
    use net::server::{listen, Incoming, Outgoing, Command, Notification}; // using `net::server`

    let (outgoing, incoming, waker) = listen(SERVER_ADDR).unwrap();

    // Interface:
    // - `outgoing`: To send either to the network (messages) or to the reactor (commands).
    // - `incoming`: To receive either from the network (messages) or from the reactor (notifications).
    // - `waker` : To wake the reactor (sending does not wake it).

    // --- Test ----

    // 1. Connection notification
    let Incoming::Internal(Notification::Connection { .. }) = incoming.recv().unwrap() else {
        panic!("should receive a connection notification");
    };

    // 2. Receive "hello" message
    let Incoming::Network(message) = incoming.recv().unwrap() else {
        panic!("should receive a message");
    };
    assert_eq!(message.data[..], "hello".as_bytes()[..]);

    // 3. Disconnection notification
    let Incoming::Internal(Notification::Disconnection { .. }) = incoming.recv().unwrap() else {
        panic!("should receive a disconnection notification");
    };

    // 4. Shutdown
    outgoing.send(Outgoing::Internal(Command::Shutdown)).unwrap();
    waker.wake_reactor().unwrap();
}

fn client_side() {
    use net::client::{connect, OutgoingMessage, Outgoing, Command, Guarantees}; // using `net::client`
    use bytes::BytesMut;

    let (outgoing, incoming, waker) = connect(CLIENT_ADDR, SERVER_ADDR).unwrap();

    // Test:
    
    // 1. Send "hello" message.
    outgoing.send(Outgoing::Network(OutgoingMessage {
        data: BytesMut::from("hello".as_bytes()),
        channel: 0,
        guarantees : Guarantees::Delivery,
    })).unwrap();
    waker.wake_reactor().unwrap();

    // 2. Wait
    // Immediate shutdown can prevent sending because messages have to wait for socket availability.
    std::thread::sleep(std::time::Duration::from_millis(1));
    
    // 3. Shutdown 
    outgoing.send(Outgoing::Internal(Command::Shutdown)).unwrap();
    waker.wake_reactor().unwrap();
}
```

# Roadmap

**Questions**:
- Remove dependancy on [`bytes::BytesMut`] ?
Quite practical to use and well supported so idk.
- Remove custom protocols for public specifications ?
Depends on whether I need specialized protocols.

**Improvements**:
- Parallelize network operations handling.
- Mixing IPv4 and IPv6 sockets.
- Remove waker.