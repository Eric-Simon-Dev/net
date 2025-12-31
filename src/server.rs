mod protocol_thread;
mod receiver_thread;
mod sender_thread;

use std::{
    io,
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
};

use bytes::BytesMut;
use crossbeam::channel::{Receiver, Sender, bounded};

use crate::protocol::{MAX_PACKET_SIZE, MAX_PAYLOAD_SIZE, PacketType};

// ---- Buffering constants ----

const MAX_PACKETS_PER_CHANNEL: usize = 64;
const BUFFER_SIZE: usize = MAX_PACKET_SIZE * MAX_PACKETS_PER_CHANNEL;
const RING_SIZE: usize = BUFFER_SIZE * 256;

// ==================================================================
// Server
// ==================================================================

pub struct Server {
    socket: UdpSocket,
}

// ---- Constructor ----

impl Server {
    /// Creates a new server bound to the given `addr`.
    ///
    /// # Errors
    /// `UdpSocket` cannot be bound to the address.
    pub fn new(addr: impl ToSocketAddrs) -> io::Result<Server> {
        Ok(Self {
            socket: UdpSocket::bind(addr)?,
        })
    }
}

// ---- Methods ----

impl Server {
    /// Start pumping network data asynchronuously.
    ///
    /// # Guarantees
    ///
    /// If a returned endpoint receives `Err(Disconnected)`,
    /// all network threads have stopped.
    ///
    /// It is safe to call this method again to relaunch the network,
    /// but previous client IDs are no longer valid.
    ///
    /// # Errors
    /// Fail to clone server's UDP socket.
    pub fn listen(&mut self) -> io::Result<(Sender<Message>, Receiver<Message>)> {
        // ---- Channels ----

        // Create channels to pass network data between threads.

        let incoming_packets = bounded(MAX_PACKETS_PER_CHANNEL);
        let outgoing_packets = bounded(MAX_PACKETS_PER_CHANNEL);
        let incoming_messages = bounded(MAX_PACKETS_PER_CHANNEL);
        let outgoing_messages = bounded(MAX_PACKETS_PER_CHANNEL);

        // ---- Threads ----

        // Spawn threads to pump network data asynchronuously.

        receiver_thread::spawn(self.socket.try_clone()?, incoming_packets.0);
        sender_thread::spawn(self.socket.try_clone()?, outgoing_packets.1);
        protocol_thread::spawn(
            incoming_packets.1,
            outgoing_messages.1,
            outgoing_packets.0,
            incoming_messages.0,
        );

        // ----

        Ok((outgoing_messages.0, incoming_messages.1))
    }
}

// ==================================================================
// Network data
// ==================================================================

// ---- Message (public) ----

#[derive(Debug, Clone)]
pub struct Message {
    pub data: BytesMut,
    pub client_addr: SocketAddr,
    pub channel: u8,
    pub guarantees: Guarantees,
}

impl Message {
    /// Maximum size (in bytes) for `data`.
    pub const MAX_DATA_SIZE: usize = MAX_PAYLOAD_SIZE;
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
    client_addr: SocketAddr,
}
