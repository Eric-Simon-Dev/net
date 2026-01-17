//! Transport layer interface for client/server.
//!
//! - Doesn't support mixing IPv4 and IPv6 sockets.
//! - Exposes [`bytes::BytesMut`] for data management.
//!
//! ## Example
//!
//! ```
//! const SERVER_ADDR: &'static str = "0:12012";
//! const CLIENT_ADDR: &'static str = "0:0";
//!
//! /// - Bind to `SERVER_ADDR`.
//! /// - Wait for a hello message.
//! fn server() {
//!     use net::server::{listen, Incoming, Outgoing, Command, Notification};
//!
//!     let (outgoing, incoming, waker) = listen(SERVER_ADDR).unwrap();
//!
//!     // Receive a connection notification first.
//!     assert!(matches!(incoming.recv().unwrap(), Incoming::Internal(Notification::Connection { .. })));
//!
//!     // Receive a "hello" message second.
//!     if let Incoming::Network(message) = incoming.recv().unwrap() {
//!         assert_eq!(message.data[..], "hello".as_bytes()[..]);
//!     }
//!     else {
//!         panic!("shoud receive a message");
//!     }
//!
//!     // Receive a disconnection notification third.
//!     assert!(matches!(incoming.recv().unwrap(), Incoming::Internal(Notification::Disconnection { .. })));
//!
//!     outgoing.send(Outgoing::Internal(Command::Shutdown)).unwrap();
//!     waker.notify_reactor().unwrap();
//! }
//!
//! /// - Bind to `CLIENT_ADDR` & Connect to `SERVER_ADDR`.
//! /// - Send a hello message.
//! fn client() {
//!     use net::client::{connect, OutgoingMessage, Outgoing, Command, Guarantees};
//!     use bytes::BytesMut;
//!
//!     let (outgoing, incoming, waker) = connect(CLIENT_ADDR, SERVER_ADDR).unwrap();
//!
//!     outgoing.send(Outgoing::Network(OutgoingMessage {
//!         data: BytesMut::from("hello".as_bytes()),
//!         channel: 0,
//!         guarantees : Guarantees::Delivery,
//!     })).unwrap();
//!     waker.notify_reactor().unwrap();
//!
//!     // Sending messages will actually queue them.
//!     // They will be sent whenever their socket is ready, in a next iteration.
//!     // Disconnecting or shutting down before will prevent them from being sent.
//!     // So we wait a bit to ensure it has been sent.
//!     //
//!     // TODO: Remove this constraint by checking for queued messages
//!     // and continue sending & ignoring channels before shutting down once all sent.
//!     std::thread::sleep(std::time::Duration::from_millis(100));
//!
//!     outgoing.send(Outgoing::Internal(Command::Shutdown)).unwrap();
//!     waker.notify_reactor().unwrap();
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
    //! # Memos
    //!
    //! ## TCP / UDP / RUDP
    //!
    //! TCP and UDP protocols are our primitives.
    //!
    //! RUDP (Reliable UDP) is a protocol overlay over UDP
    //! to have more guarantees without compromising performance as much as TCP.
    //!
    //! ## TCP / UDP Guarantees
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
    //! - **Delivery**: No acknowledgment of delivery for sender (fire-and-forget).
    //! - **Order**: Messages may arrive out of order.
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
