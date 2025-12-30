use std::{net::UdpSocket, thread};

use crossbeam::channel::Receiver;

use super::Packet;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub fn spawn(socket: UdpSocket, outgoing_packets: Receiver<Packet>) {
    thread::spawn(move || {
        let mut sender = UdpSender::new(socket, outgoing_packets);
        while sender.send().is_ok() {
            continue;
        }
    });
}

/// Handles outgoing UDP packets with a blocking function.
struct UdpSender {
    socket: UdpSocket,
    outgoing_packets: Receiver<Packet>,
}

impl UdpSender {
    fn new(socket: UdpSocket, outgoing_packets: Receiver<Packet>) -> Self {
        Self {
            socket,
            outgoing_packets,
        }
    }

    /// Send a single UDP packet fetched from `outgoing_packets`.
    ///
    /// # Behavior
    /// - Blocks on `outgoing_packets` until a packet arrives.
    ///
    /// # Errors
    /// - `outgoing_packets` is disconnected.
    /// - Sending from the socket fails.
    fn send(&mut self) -> Result<()> {
        let Packet { data, client_addr } = self.outgoing_packets.recv()?;
        self.socket.send_to(&data, client_addr)?;
        Ok(())
    }
}
