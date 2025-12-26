use std::net::UdpSocket;

use crossbeam::channel::Receiver;

use super::Packet;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

/// Handles outgoing UDP packets with a blocking function.
pub struct UdpSender {
    socket: UdpSocket,
    outgoing_packet: Receiver<Packet>,
}

impl UdpSender {
    pub fn new(socket: UdpSocket, outgoing_packet: Receiver<Packet>) -> Self {
        Self {
            socket,
            outgoing_packet,
        }
    }

    /// Send a single UDP packet fetch from `outgoing_packet`.
    ///
    /// # Behavior
    /// - Blocks on `outgoing_packet` until a packet arrives.
    ///
    /// # Errors
    /// - `outgoing_packet` is disconnected.
    /// - Sending from the socket fails.
    pub fn send(&mut self) -> Result<()> {
        let Packet { data, addr } = self.outgoing_packet.recv()?;
        self.socket.send_to(&data, addr)?;
        Ok(())
    }
}
