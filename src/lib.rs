//! Client-Server network library intended for realtime multiplayer games.
//! 
//! Provides a small [transport layer](https://en.wikipedia.org/wiki/Transport_layer)
//! over TCP and UDP. Goals are:
//! - **Simplicity**: Keep the API easy to use and specialized in realtime.
//! - **Performance**: Investigate the tips and tricks to optimize network performance.
//! 
//! I develop this for learning purposes and to have a simple API to work with.
//! If this project gets more serious, I will do benchmarks and comparisons
//! (another similar crate for example is [laminar](https://crates.io/crates/laminar)).
//! 
//! To ensure compatibility, use the same version of this crate on your client and server.
//! 
//! # Features
//! 
//! ## Message-based API
//! 
//! TCP frames and UDP packets are abstracted away in favor of abstract messages.
//!
//! They have a *channel byte for multiplexing* and guarantees.
//! 
//! Always guaranteed:
//! - Integrity: Data is not malformed (already ensured by TCP and UDP protocols).
//! - Bounds: Messages are not segmented or concatenated (only internally eventually).
//! - Deduplication: The same message cannot be received multiple times.
//! 
//! Optionally guaranteed:
//! - Delivery: Ensure message delivery with ACK and timers.
//! - DeliveryOrder: Ensure message delivery and order *relative to the channel*.
//! 
//! ## Reliability mechanisms
//! 
//! 
//! # Architecture
//! 
//!- [Reactor-based design](https://en.wikipedia.org/wiki/Reactor_pattern):
//! Upon a successfull call to [`listen(..)`](crate::server::listen) or [`connect(..)`](`crate::client::connect`),
//! a reactor thread is spawned.
//! It waits for IO events ([`polling`] crate) and currently also processes them (simpler for now).
//! All operations are non-blocking.
//! 
//! # API
//! 
//! The thread is spawned upon a successfull listen/connect operation.
//! 
//! Interfacing with the  is done using channels and a *waker*
//! (mechanism used to wake up network thread from the main thread).
//! 
//! # Usage
//! 
//! ## Server side
//! 
//! ```
//! // Use `net::server` module.
//! use net::server::{listen, Incoming, Outgoing, Command, Notification};
//!
//! let (outgoing, incoming, waker) = listen(SERVER_ADDR).unwrap();
//! 
//! // Interface:
//! // - `outgoing`: To send either to the network (messages) or to the reactor (commands).
//! // - `incoming`: To receive either from the network (messages) or from the reactor (notifications).
//! // - `waker` : To wake the reactor (sending does not wake it).
//!
//! 
//!
//! // Test:
//! // 1. Wait for connection notification.
//! // 2. Wait for "hello" message.
//! // 3. Wait for disconnection notification.
//! // 4. Shutdown reactor.
//!
//! let Incoming::Internal(Notification::Connection { .. }) = incoming.recv().unwrap() else {
//!     panic!("should receive a connection notification");
//! };
//!
//! let Incoming::Network(message) = incoming.recv().unwrap() else {
//!     panic!("should receive a message");
//! };
//! assert_eq!(message.data[..], "hello".as_bytes()[..]);
//!
//! let Incoming::Internal(Notification::Disconnection { .. }) = incoming.recv().unwrap() else {
//!     panic!("should receive a disconnection notification");
//! };
//!
//! outgoing.send(Outgoing::Internal(Command::Shutdown)).unwrap();
//! waker.wake_reactor().unwrap();
//! ```
//!
//! ```
//! # use std::{thread, time};
//! 
//! // Simulate communication by running a server and a client on separate threads.
//! thread::scope(|s| {
//!     thread::Builder::new().name("server".to_string()).spawn_scoped(s, server_side);
//!     thread::sleep(time::Duration::from_millis(100));
//!     thread::Builder::new().name("client".to_string()).spawn_scoped(s, client_side);
//! });
//! 
//! const SERVER_ADDR: &'static str = "0:12012";
//! const CLIENT_ADDR: &'static str = "0:0";
//! 
//! fn server_side() {
//!     // Use `net::server` module on server side.
//!     use net::server::{listen, Incoming, Outgoing, Command, Notification};
//!
//!     // Interface:
//!     // - `outgoing`: To send messages to network or *commands* (internal, to reactor).
//!     // - `incoming`: To receive *messages* (external, from network) or *notifications* (internal, from reactor).
//!     // - `waker` : For waking reactor (sending with `outgoing` does not wake it).
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
//! fn client_side() {
//!     // Use `net::client` module on client side.
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
    //! # Architecture
    //! 
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

// TODO: Recheck server module later, correct and copy to client (it still has old code).

pub mod client;
pub mod server;

mod protocol;

#[cfg(test)]
mod tests;
