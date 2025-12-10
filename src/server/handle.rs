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

/// Interface between UDP packets and messages (incoming & outgoing).
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

    //------// Channels //------//
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
    /// Client unknown => Try adding client, else drop packet.
    fn handle_incoming_packet(&mut self, packet: BytesMut, client_addr: SocketAddr) -> Result<()> {
        //------// Client handling //------//

        let client_index = match self.clients.addr_to_index(client_addr) {
            Some(client_index) => client_index,
            None => match self.clients.add(client_addr) {
                Ok(client_index) => client_index,
                Err(_) => return Ok(()),
            },
        };

        //------// Conversion : Packet -> Message //------//

        let message = packet;

        //------//

        self.incoming_message.send((message, client_index))?;

        Ok(())
    }

    /// `Err(_)` <=> Channel disconnection.
    ///
    /// Client unknown => Drop message.
    fn handle_outgoing_message(&mut self, message: BytesMut, client_index: usize) -> Result<()> {
        //------// Client handling //------//

        let Some(client_addr) = self.clients.index_to_addr(client_index) else {
            return Ok(());
        };

        //------// Conversion : Message -> Packet //------//

        let packet = message;

        //------//

        self.outgoing_packet.send((packet, client_addr))?;

        Ok(())
    }
}
