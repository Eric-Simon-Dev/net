//! Transport layer interface for client/server.
//!
//! - Doesn't support mixing IPv4 and IPv6 sockets.
//!
//! ## Example
//!
//! ```
//! const SERVER_ADDR: &'static str = "0:12012";
//! const CLIENT_ADDR: &'static str = "0:0";
//!
//! fn server() {
//!     use net::server::{listen, Incoming, Outgoing, Command, Notification};
//!
//!     // bind to `SERVER_ADDR`
//!     let (outgoings, incomings, waker) = listen(SERVER_ADDR).unwrap();
//!
//!     // wait for connection
//!     assert!(matches!(incomings.recv().unwrap(), Incoming::Internal(Notification::Connection { .. })));
//!
//!     // wait for "hello" message
//!     let Incoming::Network(message) = incomings.recv().unwrap() else {
//!         panic!("should receive a message");
//!     };
//!     assert_eq!(message.data[..], "hello".as_bytes()[..]);
//!
//!     // wait for disconnection
//!     assert!(matches!(incomings.recv().unwrap(), Incoming::Internal(Notification::Disconnection { .. })));
//!
//!     // shutdown
//!     outgoings.send(Outgoing::Internal(Command::Shutdown)).unwrap();
//!     waker.wake_reactor().unwrap();
//! }
//!
//! fn client() {
//!     use net::client::{connect, OutgoingMessage, Outgoing, Command, Guarantees};
//!     use bytes::BytesMut;
//!
//!     // bind to `CLIENT_ADDR` & connect to `SERVER_ADDR`
//!     let (outgoings, incomings, waker) = connect(CLIENT_ADDR, SERVER_ADDR).unwrap();
//!
//!     // send "hello" message
//!     outgoings.send(Outgoing::Network(OutgoingMessage {
//!         data: BytesMut::from("hello".as_bytes()),
//!         channel: 0,
//!         guarantees : Guarantees::Delivery,
//!     })).unwrap();
//!     waker.wake_reactor().unwrap();
//!
//!     // Wait for socket to be ready and thus messages to be sent.
//!     std::thread::sleep(std::time::Duration::from_millis(100));
//! 
//!     // shutdown
//!     outgoings.send(Outgoing::Internal(Command::Shutdown)).unwrap();
//!     waker.wake_reactor().unwrap();
//! }
//!
//! # std::thread::scope(|s| {
//! #     std::thread::Builder::new().name("server".to_string()).spawn_scoped(s, server);
//! #     // Give server time to start so client connection succeeds.
//! #     std::thread::sleep(std::time::Duration::from_millis(100));
//! #     std::thread::Builder::new().name("client".to_string()).spawn_scoped(s, client);
//! # });
//! ```

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
    //! Sockets are:
    //! - `TcpListener`.
    //! - `UdpSocket`.
    //! - Each `TcpStream`.
    //!
    //! # Polling
    //!
    //! OS provide us a way to know when a given socket is ready for writing or reading.
    //! To avoid waiting or spinning on the sockets.
    //!
    //! Use [`polling`] crate for that.
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
