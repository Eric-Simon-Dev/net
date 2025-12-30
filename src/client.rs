mod handler_thread;
mod receiver_thread;
mod sender_thread;

use std::{
    io,
    net::{ToSocketAddrs, UdpSocket},
};

use bytes::BytesMut;
use crossbeam::channel::{Receiver, Sender, bounded};

use crate::protocol::{MAX_PACKET_SIZE, MAX_PAYLOAD_SIZE, PacketType};

// ---- Buffering constants ----

const MAX_PACKET_IN_FLIGHT: usize = 1024;
const BUFFER_SIZE: usize = MAX_PACKET_SIZE * MAX_PACKET_IN_FLIGHT;
const RING_SIZE: usize = BUFFER_SIZE * 16;

// ==================================================================
// Client
// ==================================================================

pub struct Client {
    socket: UdpSocket,
}

// ---- Constructor ----

impl Client {
    /// Creates a new client bound to the given `addr`.
    ///
    /// # Errors
    /// `UdpSocket` cannot be bound to the address.
    pub fn new(addr: impl ToSocketAddrs) -> io::Result<Client> {
        Ok(Self {
            socket: UdpSocket::bind(addr)?,
        })
    }
}

// ---- Methods ----

impl Client {
    /// Connect client to `server_addr` and start pumping network data asynchronously.
    ///
    /// # Guarantees
    ///
    /// If a returned endpoint receives `Err(Disconnected)`,
    /// all network threads have stopped.
    ///
    /// It is safe to call this method again to relaunch the network.
    ///
    /// # Errors
    /// - Fail to connect to server address.
    /// - Fail to clone client's UDP socket.
    pub fn connect(
        &mut self,
        server_addr: impl ToSocketAddrs,
    ) -> io::Result<(Sender<Message>, Receiver<Message>)> {
        // ---- Connection ----

        // Connect socket to given server address.

        self.socket.connect(server_addr)?;

        // ---- Channels ----

        // Create channels to pass network data between threads.

        let cap = MAX_PACKET_IN_FLIGHT;
        let incoming_packet = bounded(cap);
        let outgoing_packet = bounded(cap);
        let incoming_message = bounded(cap);
        let outgoing_message = bounded(cap);

        // ---- Threads ----

        // Spawn threads to pump network data asynchronuously.

        receiver_thread::spawn(self.socket.try_clone()?, incoming_packet.0);
        sender_thread::spawn(self.socket.try_clone()?, outgoing_packet.1);
        handler_thread::spawn(
            incoming_packet.1,
            outgoing_message.1,
            outgoing_packet.0,
            incoming_message.0,
        );

        // ----

        Ok((outgoing_message.0, incoming_message.1))
    }
}

// ==================================================================
// Network data
// ==================================================================

// ---- Message (public) ----

#[derive(Debug, Clone)]
pub struct Message {
    pub data: BytesMut,
    pub channel: u8,
    pub guarantees: Guarantees,
}

impl Message {
    /// Maximum size (in bytes) for data.
    pub const MAX_DATA: usize = MAX_PAYLOAD_SIZE;
}

/// Reliability guarantees for a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Guarantees {
    None,
}

// ---- Packet (private) ----

#[derive(Debug)]
struct Packet {
    data: BytesMut,
}
