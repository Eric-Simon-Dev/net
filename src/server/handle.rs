pub mod clients;

use std::net::SocketAddr;

use bytes::{BufMut, BytesMut};
use crossbeam::{
    channel::{Receiver, Sender},
    select,
};

use clients::Clients;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

const REALLOCATION_CAPACITY: usize = 1_048_576; // = 2^20
const REALLOCATION_THRESHOLD: usize = 1024;

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
    outgoing_buffer: BytesMut,

    //------// Channels //------//
    incoming_packet: Receiver<(BytesMut, SocketAddr)>,
    outgoing_message: Receiver<(BytesMut, usize, u8)>,
    outgoing_packet: Sender<(BytesMut, SocketAddr)>,
    incoming_message: Sender<(BytesMut, usize, u8)>,
}

impl Handler {
    pub fn new(
        incoming_packet: Receiver<(BytesMut, SocketAddr)>,
        outgoing_message: Receiver<(BytesMut, usize, u8)>,
        outgoing_packet: Sender<(BytesMut, SocketAddr)>,
        incoming_message: Sender<(BytesMut, usize, u8)>,
    ) -> Self {
        Self {
            clients: Clients::new(),
            outgoing_buffer: BytesMut::with_capacity(REALLOCATION_CAPACITY),
            incoming_packet,
            outgoing_packet,
            incoming_message,
            outgoing_message,
        }
    }

    /// Handle network protocols (blocking).
    ///
    /// Blocks <=> Wait channels for a packet to handle.
    ///
    /// `Err(_)` <=> Channel disconnection.
    pub fn handle(&mut self) -> Result<()> {
        select! {
            recv(self.incoming_packet) -> packet => {
                let (packet, client_addr) = packet?;
                self.handle_incoming_packet(packet, client_addr)?;
            }
            recv(self.outgoing_message) -> message => {
                let (message, client_index, channel) = message?;
                self.handle_outgoing_message(message, client_index, channel)?;
            }
        }
        Ok(())
    }

    /// `Err(_)` <=> Channel disconnection.
    ///
    /// Client unknown => Try adding client, else drop packet.
    fn handle_incoming_packet(
        &mut self,
        mut packet: BytesMut,
        client_addr: SocketAddr,
    ) -> Result<()> {
        //------// Client handling //------//

        let client_index = match self.clients.addr_to_index(client_addr) {
            Some(client_index) => client_index,
            None => match self.clients.add(client_addr) {
                Ok(client_index) => client_index,
                Err(_) => return Ok(()),
            },
        };

        //------// Conversion : Packet -> Message //------//

        // Extract header then truncate it to get message.

        let channel = packet[0];
        let message = packet.split_off(1);

        //------//

        self.incoming_message
            .send((message, client_index, channel))?;

        Ok(())
    }

    /// `Err(_)` <=> Channel disconnection.
    ///
    /// Client unknown => Drop message.
    fn handle_outgoing_message(
        &mut self,
        message: BytesMut,
        client_index: usize,
        channel: u8,
    ) -> Result<()> {
        //------// Client handling //------//

        // Fetch client address or drop `message` if unknown.

        let Some(client_addr) = self.clients.index_to_addr(client_index) else {
            return Ok(());
        };

        //------// Conversion : Message -> Packet //------//

        // Add header then copy message after.
        // Header = channel.
        //
        // Eventually reserve more capacity for buffer.

        self.outgoing_buffer.put_u8(channel);
        self.outgoing_buffer.put(message);
        let packet = self.outgoing_buffer.split();

        if self.outgoing_buffer.capacity() < REALLOCATION_THRESHOLD {
            self.outgoing_buffer.reserve(REALLOCATION_CAPACITY);
        }

        //------//

        self.outgoing_packet.send((packet, client_addr))?;

        Ok(())
    }
}
