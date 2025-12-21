use std::net::UdpSocket;

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
    outgoing_packet: Receiver<BytesMut>,
}

impl UdpPacketSender {
    pub fn new(socket: UdpSocket, outgoing_packet: Receiver<BytesMut>) -> Self {
        Self {
            socket,
            outgoing_packet,
        }
    }

    /// Send a packet (blocking).
    ///
    /// Blocking <=> Wait channel for a packet to send.
    ///
    /// `Err(_)` <=> or :
    /// - Socket error.
    /// - Channel disconnection.
    pub fn send(&mut self) -> Result<()> {
        //------// Receive packet //------//

        // Receive packet from `outgoing_packet` (fallible, blocking).

        let packet = self.outgoing_packet.recv()?;

        //------// Send packet //------//

        // Send packet (fallible).

        self.socket.send(&packet)?;

        //------//

        Ok(())
    }
}
