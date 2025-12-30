use std::{net::UdpSocket, thread};

use crossbeam::channel::Receiver;

use super::Packet;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub fn spawn(socket: UdpSocket, outgoing_packet: Receiver<Packet>) {
    thread::spawn(move || {
        let mut sender = UdpSender::new(socket, outgoing_packet);
        while sender.send().is_ok() {
            continue;
        }
    });
}

/// Handles outgoing UDP packets with a blocking function.
struct UdpSender {
    socket: UdpSocket,
    outgoing_packet: Receiver<Packet>,
}

impl UdpSender {
    fn new(socket: UdpSocket, outgoing_packet: Receiver<Packet>) -> Self {
        Self {
            socket,
            outgoing_packet,
        }
    }

    /// Send a single UDP packet fetched from `outgoing_packet`.
    ///
    /// # Behavior
    /// - Blocks on `outgoing_packet` until a packet arrives.
    ///
    /// # Errors
    /// - `outgoing_packet` is disconnected.
    /// - Sending from the socket fails.
    fn send(&mut self) -> Result<()> {
        let Packet { data, client_addr } = self.outgoing_packet.recv()?;
        self.socket.send_to(&data, client_addr)?;
        Ok(())
    }
}
