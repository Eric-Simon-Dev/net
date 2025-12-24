use std::net::UdpSocket;

use crossbeam::channel::Receiver;

use super::Packet;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub struct PacketSender {
    socket: UdpSocket,
    outgoing_packet: Receiver<Packet>,
}

impl PacketSender {
    pub fn new(socket: UdpSocket, outgoing_packet: Receiver<Packet>) -> Self {
        Self {
            socket,
            outgoing_packet,
        }
    }

    /// Block <=> Wait `outgoing_packet`.
    ///
    /// `Err(_)` <=> or :
    /// - `outgoing_packet` disconnect.
    /// - `socket` fail while sending.
    pub fn send(&mut self) -> Result<()> {
        let Packet { data, addr } = self.outgoing_packet.recv()?;
        self.socket.send_to(&data, addr)?;
        Ok(())
    }
}
