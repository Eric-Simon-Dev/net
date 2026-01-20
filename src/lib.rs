//! Network library for client-server focused on simplicity.
//!
//! - **Custom protocols**: This crate defines custom transport protocols
//! over both TCP and UDP. These protocols are not public specifications
//! thus both your client and server should use this crate and the same version of it.
//!
//! - **Separate single thread**: Network-related operations are run
//! on a single separate thread called the reactor.
//!
//! # Example
//!
//! ```
//! # std::thread::scope(|s| {
//! #     std::thread::Builder::new().name("server".to_string()).spawn_scoped(s, server);
//! #     // Give server time to start so client connection succeeds.
//! #     std::thread::sleep(std::time::Duration::from_millis(100));
//! #     std::thread::Builder::new().name("client".to_string()).spawn_scoped(s, client);
//! # });
//! const SERVER_ADDR: &'static str = "0:12012";
//! const CLIENT_ADDR: &'static str = "0:0";
//!
//! fn server() {
//!     // Use `net::server` for server...
//!     use net::server::{listen, Incoming, Outgoing, Command, Notification};
//!
//!     // Interface:
//!     // - `incoming`: For receiving messages from network or notifications from reactor.
//!     // - `outgoing`: For sending messages to network or commands to reactor.
//!     // - `waker` : For waking reactor (sending to `outgoing` does not wake it).
//!
//!     let (outgoing, incoming, waker) = listen(SERVER_ADDR).unwrap();
//!
//!     // Test:
//!     // 1. Wait for connection notification.
//!     // 2. Wait for "hello" message.
//!     // 3. Wait for disconnection notification.
//!     // 4. Shutdown reactor.
//!
//!     let Incoming::Internal(Notification::Connection { .. }) = incoming.recv().unwrap() else {
//!         panic!("should receive a connection notification");
//!     };
//!
//!     let Incoming::Network(message) = incoming.recv().unwrap() else {
//!         panic!("should receive a message");
//!     };
//!     assert_eq!(message.data[..], "hello".as_bytes()[..]);
//!
//!     let Incoming::Internal(Notification::Disconnection { .. }) = incoming.recv().unwrap() else {
//!         panic!("should receive a disconnection notification");
//!     };
//!
//!     outgoing.send(Outgoing::Internal(Command::Shutdown)).unwrap();
//!     waker.wake_reactor().unwrap();
//! }
//!
//! fn client() {
//!     // ... and `net::client` for client.
//!     use net::client::{connect, OutgoingMessage, Outgoing, Command, Guarantees};
//!     use bytes::BytesMut;
//!
//!     let (outgoing, incoming, waker) = connect(CLIENT_ADDR, SERVER_ADDR).unwrap();
//!
//!     // Test:
//!     // 1. Send "hello" message.
//!     // 2. Shutdown reactor.
//!
//!     outgoing.send(Outgoing::Network(OutgoingMessage {
//!         data: BytesMut::from("hello".as_bytes()),
//!         channel: 0,
//!         guarantees : Guarantees::Delivery,
//!     })).unwrap();
//!     waker.wake_reactor().unwrap();
//!
//!     // Messages are sent only when socket is ready.
//!     // Shutting down immediately after can drop to-be-sent messages.
//!     std::thread::sleep(std::time::Duration::from_millis(1));
//!
//!     outgoing.send(Outgoing::Internal(Command::Shutdown)).unwrap();
//!     waker.wake_reactor().unwrap();
//! }
//! ```
//!
//! # Roadmap
//!
//! **Questions**:
//!
//! - Remove dependancy on [`bytes::BytesMut`] ?
//! Quite practical to use and well supported so idk.
//!
//! - Remove custom protocols for public specifications ?
//! Depends on whether I need specialized protocols.
//!
//! **Improvements**:
//!
//! - Parallelize network operations handling.
//!
//! - Mixing IPv4 and IPv6 sockets.
//!
//! - Remove waker.

mod doc {
    //! # Naming convention
    //!
    //! Shorter socket names:
    //! - `tcp`: `TcpListener`.
    //! - `tcp_stream`: `TcpStream`.
    //! - `udp`: `UdpSocket`.
    //!
    //! Interface:
    //! - `Outgoing`: An outgoing message to send or an internal command to execute.
    //! - `Incoming`: An incoming message or a internal notification to process.
}

mod memo {
    //! # Sockets
    //!
    //! Sockets are assigned to each:
    //! - `TcpListener`.
    //! - `UdpSocket`.
    //! - `TcpStream`.
    //!
    //! # Polling
    //!
    //! OS provide us a way to know when a given socket is ready for writing or reading.
    //! To avoid waiting or spinning on the sockets.
    //!
    //! Use [`polling`] crate for that.
    //!
    //! Problem: Can't easely wait on channel and on OS polling at the same time...
    //!
    //! # Protocols
    //!
    //! TCP and UDP protocols are our primitives:
    //! - UDP is a basic, bare metal, protocol that provides almost no guarantees.
    //! - TCP is a complex protocol that provides many guarantees.
    //!
    //! The more guarantees you have, the less "performance" you get.
    //! Performance metrics depends on situation: Can be latency, throughput, behavior, etc.
    //!
    //! RUDP (Reliable UDP) are protocol overlays over UDP:
    //! They implement guarantees between UDP and TCP and optimize performance for them.
    //!
    //! # TCP/UDP Guarantees
    //!
    //! **Integrity** is guaranteed by both.
    //!
    //! TCP guarantees:
    //! - **Deduplication**: Each message is received exactly once.
    //! - **Delivery**: Sender is notified of delivery.
    //! - **Order**: Messages arrive in sending order.
    //!
    //! TCP does *not* guarantee:
    //! - **Boundaries**: Messages are merged into a continuous stream.
    //!
    //! UDP guarantees:
    //! - **Boundaries**: Messages are not merged or split.
    //!
    //! UDP does *not* guarantee:
    //! - **Deduplication**: Duplicated messages may be received.
    //! - **Delivery**: Sender is not notified of delivery. (fire-and-forget).
    //! - **Order**: Messages may be received out of sending order.
    //!
    //! ## IPv4 & IPv6
    //!
    //! - **Dual-stack socket**: Handles both IP versions, but not always available.
    //! - **Separate sockets per version**: Most common approach.
    //!
    //! ## OSI: Session & Transport layers
    //!
    //! - **Transport**: Transport guarantees, segmentation, encryption, etc.
    //! - **Session**: Continuity of the exchanges (authentication, reconnection, etc.).  
}

pub mod client;
pub mod server;

mod protocol;

#[cfg(test)]
mod tests;
