use std::net::{SocketAddr, UdpSocket};

use bytes::BytesMut;
use crossbeam::channel::Receiver;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

/// Send UDP packets :
/// 1. Receive them from `outgoing_packet`.
/// 2. Send them.
///
/// ## Usage
///
/// Meant to be used in its own thread looping over `send()`.
///
/// ```ignore
/// loop {
///     match sender.send() {
///         Ok(_) => continue,
///         Err(_) => break,
///     }
/// }
/// ```
pub struct UdpPacketSender {
    socket: UdpSocket,
    outgoing_packet: Receiver<(BytesMut, SocketAddr)>,
}

impl UdpPacketSender {
    pub fn new(socket: UdpSocket, outgoing_packet: Receiver<(BytesMut, SocketAddr)>) -> Self {
        Self {
            socket,
            outgoing_packet,
        }
    }

    /// `Err(_)` <=> or :
    /// - Socket error.
    /// - Channel disconnection.
    ///
    /// Blocking <=> Wait for a packet.
    pub fn send(&mut self) -> Result<()> {
        //------// Receive packet //------//

        // Receive packet from `outgoing_packet` (fallible, blocking).

        let (packet, client_addr) = self.outgoing_packet.recv()?;

        //------// Send packet //------//

        // Send packet to client address (fallible).

        self.socket.send_to(&packet, client_addr)?;

        //------//

        Ok(())
    }
}
