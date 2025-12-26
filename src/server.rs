mod handle;
mod recv;
mod send;

use std::{
    io,
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
    thread,
};

use bytes::BytesMut;
use crossbeam::channel::{Receiver, Sender, bounded};

use super::{MAX_PACKET_SIZE, MAX_PAYLOAD_SIZE, PacketType};
use handle::Handler;
use recv::UdpReceiver;
use send::UdpSender;

// ---- Buffering constants ----

const MAX_PACKET_IN_FLIGHT: usize = 1024;
const BUFFER_SIZE: usize = MAX_PACKET_SIZE * MAX_PACKET_IN_FLIGHT;
const RING_SIZE: usize = BUFFER_SIZE * 16;

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
    /// - `UdpSocket` cannot be bound to the address.
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
    /// - Fail to clone server's UDP socket.
    pub fn listen(&mut self) -> io::Result<(Sender<Message>, Receiver<Message>)> {
        // ---- Channels ----

        // Create channels to pass network data between threads.

        let cap = MAX_PACKET_IN_FLIGHT;
        let incoming_packet = bounded(cap);
        let outgoing_packet = bounded(cap);
        let incoming_message = bounded(cap);
        let outgoing_message = bounded(cap);

        // ---- Threads ----

        // Spawn threads to pump network data asynchronuously.

        self.spawn_receiver_thread(incoming_packet.0)?;
        self.spawn_sender_thread(outgoing_packet.1)?;
        self.spawn_handler_thread(
            incoming_packet.1,
            outgoing_message.1,
            outgoing_packet.0,
            incoming_message.0,
        );

        // ----

        Ok((outgoing_message.0, incoming_message.1))
    }

    /// # Errors
    /// - Fail to clone server's UDP socket.
    fn spawn_receiver_thread(&mut self, incoming_packet: Sender<Packet>) -> io::Result<()> {
        let mut receiver = UdpReceiver::new(self.socket.try_clone()?, incoming_packet);
        thread::spawn(move || {
            while receiver.recv().is_ok() {
                continue;
            }
        });
        Ok(())
    }

    /// # Errors
    /// - Fail to clone server's UDP socket.
    fn spawn_sender_thread(&mut self, outgoing_packet: Receiver<Packet>) -> io::Result<()> {
        let mut sender = UdpSender::new(self.socket.try_clone()?, outgoing_packet);
        thread::spawn(move || {
            while sender.send().is_ok() {
                continue;
            }
        });
        Ok(())
    }

    fn spawn_handler_thread(
        &mut self,
        incoming_packet: Receiver<Packet>,
        outgoing_message: Receiver<Message>,
        outgoing_packet: Sender<Packet>,
        incoming_message: Sender<Message>,
    ) {
        let mut handler = Handler::new(
            incoming_packet,
            outgoing_message,
            outgoing_packet,
            incoming_message,
        );
        thread::spawn(move || {
            while handler.handle().is_ok() {
                continue;
            }
        });
    }
}

// ==================================================================
// Network data
// ==================================================================

// ---- Message (public) ----

/// High-level message for application use.
#[derive(Debug, Clone)]
pub struct Message {
    pub data: BytesMut,
    pub client: ClientId,
    pub channel: u8,
    pub guarantees: Guarantees,
}

/// Unique identifier for a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(usize);

/// Reliability guarantees for a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Guarantees {
    None,
}

impl Message {
    /// Maximum allowed payload size for a message.
    pub const MAX_DATA_SIZE: usize = MAX_PAYLOAD_SIZE;
}

// ---- Packet (private) ----

/// Raw network packet for internal use.
#[derive(Debug)]
struct Packet {
    data: BytesMut,
    addr: SocketAddr,
}
