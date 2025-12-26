use std::net::UdpSocket;

use bytes::BytesMut;
use crossbeam::channel::Sender;

use super::{BUFFER_SIZE, MAX_PACKET_SIZE, Packet, RING_SIZE};

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

/// Handles incoming UDP packets with a blocking functions.
///
/// It buffers incoming packets and forwards them to the handler thread.
pub struct UdpReceiver {
    socket: UdpSocket,

    /// Internal buffer for incoming UDP packets.
    /// Its size defines the maximum packet size.
    /// Excess bytes are discarded according to the socket's `.recv_from()` specification.
    buffer: BytesMut,

    incoming_packet: Sender<Packet>,
}

impl UdpReceiver {
    /// Creates a new UDP receiver.
    ///
    /// Internal buffer is initialized with a capacity of `RING_SIZE`
    /// and a size of `MAX_PACKET_SIZE`.
    pub fn new(socket: UdpSocket, incoming_packet: Sender<Packet>) -> Self {
        let mut buffer = BytesMut::with_capacity(RING_SIZE);
        buffer.resize(MAX_PACKET_SIZE, 0);
        Self {
            socket,
            buffer,
            incoming_packet,
        }
    }

    /// Receive a single UDP packet.
    ///
    /// # Behavior
    /// - Blocks on `socket.recv_from()` until a packet arrives.
    /// - Blocks if `incoming_packet` is full until space is available.
    ///
    /// # Errors
    /// - `incoming_packet` is disconnected.
    /// - Receiving from the socket fails.
    pub fn recv(&mut self) -> Result<()> {
        // ---- Receive packet ----

        // Receive a packet from the UDP socket.
        // Bytes beyond `buffer` size are discarded.
        let data_len = self.socket.recv(&mut self.buffer)?;
        let data = self.buffer.split_to(data_len);
        let packet = Packet { data };
        self.incoming_packet.send(packet)?;

        // ---- Resize buffer ----

        // Ensure the buffer is properly sized for the next packet.
        // Reallocate eventually.

        if self.buffer.capacity() < MAX_PACKET_SIZE {
            self.buffer.reserve(BUFFER_SIZE - self.buffer.capacity());
        }
        self.buffer.resize(MAX_PACKET_SIZE, 0);

        // ----

        Ok(())
    }
}
