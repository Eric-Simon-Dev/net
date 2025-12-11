use std::net::{SocketAddr, UdpSocket};

use bytes::BytesMut;
use crossbeam::channel::Sender;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

const REALLOCATION_CAPACITY: usize = 1_048_576; // = 2^20
const MAX_PACKET_SIZE: usize = 1024;

/// Receive UDP packets :
/// 1. Receive/Buffer (It's the same action) them.
/// 2. Send them through `incoming_packet`.
///
/// ## Usage
///
/// Meant to be used in its own thread looping over `recv()`.
///
/// ```ignore
/// loop {
///     match receiver.recv() {
///         Ok(_) => continue,
///         Err(_) => break,
///     }
/// }
/// ```
////////////////////////////////////////////////////////////////////////////////////
//
// Buffering :
//
// Packets are buffered in front.
// Their ownership can then be extracted (as `BytesMut`), reducing buffer size.
// Once the packet owns its data (but still same place in memory), it can be sent.
// When the packet is dropped, data is freed.
//
pub struct UdpPacketReceiver {
    socket: UdpSocket,
    buffer: BytesMut,
    incoming_packet: Sender<(BytesMut, SocketAddr)>,
}

impl UdpPacketReceiver {
    pub fn new(socket: UdpSocket, incoming_packet: Sender<(BytesMut, SocketAddr)>) -> Self {
        Self {
            socket,
            buffer: BytesMut::zeroed(MAX_PACKET_SIZE),
            incoming_packet,
        }
    }

    /// `Err(_)` <=> or :
    /// - Socket error.
    /// - Channel disconnection.
    ///
    /// Blocks <=> or :
    /// - Wait for a packet.
    /// - Wait for `incoming_packet` to have space.
    pub fn recv(&mut self) -> Result<()> {
        //------// Receive/Buffer packet //------//

        // Receive/Buffer a packet (fallible, blocking).
        // Bytes out of buffer size bounds are discarded.

        let (packet_len, client_addr) = self.socket.recv_from(&mut self.buffer)?;

        //------// Update buffer //------//

        // Move packet ownership out of buffer.
        // This reduce buffer size.
        //
        // Manually check and reallocate buffer capacity if necessary.
        // Allow us to reallocate as big as we want.
        //
        // Resize buffer for next message.

        let packet = self.buffer.split_to(packet_len);

        if self.buffer.capacity() < MAX_PACKET_SIZE {
            self.buffer.reserve(REALLOCATION_CAPACITY);
        }

        self.buffer.resize(MAX_PACKET_SIZE, 0);

        //------// Send packet //------//

        // Send packet through `incoming_packet` (fallible, blocking).

        self.incoming_packet.send((packet, client_addr))?;

        //------//

        Ok(())
    }
}
