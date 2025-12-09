pub mod clients;

use std::net::SocketAddr;

use bytes::BytesMut;
use crossbeam::{
    channel::{Receiver, Sender},
    select,
};

use clients::Clients;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

/// Interface between UDP packets and messages.
///
/// Apply transport protocols.
///
/// ## Usage
///
/// Meant to be used in its own thread looping over `handle()`.
///
/// ```ignore
/// loop {
///     match handler.handle() {
///         Ok(_) => continue,
///         Err(_) => break,
///     }
/// }
/// ```
pub struct Handler {
    clients: Clients,

    // channels
    incoming_packet: Receiver<(BytesMut, SocketAddr)>,
    outgoing_message: Receiver<(BytesMut, usize)>,
    outgoing_packet: Sender<(BytesMut, SocketAddr)>,
    incoming_message: Sender<(BytesMut, usize)>,
}

impl Handler {
    pub fn new(
        incoming_packet: Receiver<(BytesMut, SocketAddr)>,
        outgoing_message: Receiver<(BytesMut, usize)>,
        outgoing_packet: Sender<(BytesMut, SocketAddr)>,
        incoming_message: Sender<(BytesMut, usize)>,
    ) -> Self {
        Self {
            clients: Clients::new(),
            incoming_packet,
            outgoing_packet,
            incoming_message,
            outgoing_message,
        }
    }

    /// `Err(_)` <=> Channel disconnection.
    pub fn handle(&mut self) -> Result<()> {
        select! {
            recv(self.incoming_packet) -> packet => {
                let (packet, client_addr) = packet?;
                self.handle_incoming_packet(packet, client_addr)?;
            }
            recv(self.outgoing_message) -> message => {
                let (message, client_index) = message?;
                self.handle_outgoing_message(message, client_index)?;
            }
        }
        Ok(())
    }

    /// `Err(_)` <=> Channel disconnection.
    ///
    /// Try add client if client unknown.
    /// Drop packet if not possible.
    fn handle_incoming_packet(&mut self, packet: BytesMut, client_addr: SocketAddr) -> Result<()> {
        let client_index = match self.clients.addr_to_index(client_addr) {
            Some(client_index) => client_index,
            None => match self.clients.add(client_addr) {
                Ok(client_index) => client_index,
                Err(_) => return Ok(()),
            },
        };

        self.incoming_message.send((packet, client_index))?;

        Ok(())
    }

    /// `Err(_)` <=> Channel disconnection.
    ///
    /// Drop message if client unknown.
    fn handle_outgoing_message(&mut self, message: BytesMut, client_index: usize) -> Result<()> {
        let Some(client_addr) = self.clients.index_to_addr(client_index) else {
            return Ok(());
        };

        self.outgoing_packet.send((message, client_addr))?;

        Ok(())
    }
}
