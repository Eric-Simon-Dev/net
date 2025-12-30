use std::{net::UdpSocket, thread};

use bytes::BytesMut;
use crossbeam::channel::Sender;

use super::{BUFFER_SIZE, MAX_PACKET_SIZE, Packet, RING_SIZE};

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub fn spawn(socket: UdpSocket, incoming_packet: Sender<Packet>) {
    thread::spawn(move || {
        let mut receiver = UdpReceiver::new(socket, incoming_packet);
        while receiver.recv().is_ok() {
            continue;
        }
    });
}

/// Handles incoming UDP packets with a blocking function.
///
/// It buffers incoming packets and forwards them to the handler thread.
struct UdpReceiver {
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
    fn new(socket: UdpSocket, incoming_packet: Sender<Packet>) -> Self {
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
    fn recv(&mut self) -> Result<()> {
        // Receive a packet from the UDP socket.
        // Bytes beyond buffer size are discarded.
        let data_len = self.socket.recv(&mut self.buffer)?;
        let data = self.buffer.split_to(data_len);
        let packet = Packet { data };

        // Forward packet.
        self.incoming_packet.send(packet)?;

        // Maintain buffer for next packet.
        if self.buffer.capacity() < MAX_PACKET_SIZE {
            self.buffer.reserve(BUFFER_SIZE - self.buffer.capacity());
        }
        self.buffer.resize(MAX_PACKET_SIZE, 0);

        Ok(())
    }
}
