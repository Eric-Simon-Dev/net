use std::net::UdpSocket;

use bytes::BytesMut;
use crossbeam::channel::Sender;

use super::{BUFFER_SIZE, MAX_PACKET_SIZE, Packet, RING_SIZE};

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub struct PacketReceiver {
    socket: UdpSocket,
    buffer: BytesMut,
    incoming_packet: Sender<Packet>,
}

impl PacketReceiver {
    pub fn new(socket: UdpSocket, incoming_packet: Sender<Packet>) -> Self {
        let mut buffer = BytesMut::with_capacity(RING_SIZE);
        buffer.resize(MAX_PACKET_SIZE, 0);
        Self {
            socket,
            buffer,
            incoming_packet,
        }
    }

    /// Block <=> or :
    /// - Wait `socket`.
    /// - Wait `incoming_packet` for space.
    ///
    /// `Err(_)` <=> or :
    /// - `incoming_packet` disconnect.
    /// - `socket` fail while receiving.
    pub fn recv(&mut self) -> Result<()> {
        //------// Receive packet //------//

        // Bytes out of buffer size bounds are discarded.
        let data_len = self.socket.recv(&mut self.buffer)?;
        let data = self.buffer.split_to(data_len);
        let packet = Packet { data };
        self.incoming_packet.send(packet)?;

        //------// Resize buffer //------//

        if self.buffer.capacity() < MAX_PACKET_SIZE {
            self.buffer.reserve(BUFFER_SIZE - self.buffer.capacity());
        }
        self.buffer.resize(MAX_PACKET_SIZE, 0);

        //------//

        Ok(())
    }
}
