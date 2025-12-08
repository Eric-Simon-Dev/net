use std::net::SocketAddr;

use bytes::Bytes;
use crossbeam::{
    channel::{Receiver, Sender},
    select,
};
use rustc_hash::FxHashMap;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

const CLIENT_CAPACITY: usize = 256;

/// Handles app-level protocols. Handles clients.
///
/// ## Usage
///
/// Meant to be used in its own thread looping over `manage()`.
///
/// Blocks <=> or :
/// - Wait for an incoming packet + client to manage.
/// - Wait for an outgoing packet + client to manage.
///
/// ```ignore
/// loop {
///     match handle.handle() {
///         Ok(_) => continue,
///         Err(_) => break,
///     }
/// }
/// ```
pub struct Handle {
    // clients
    addr_to_index: FxHashMap<SocketAddr, usize>,
    available_client_indices: Vec<usize>,
    clients: Vec<Option<SocketAddr>>,

    // channels
    packet_incoming: Receiver<(Bytes, SocketAddr)>,
    packet_outgoing: Sender<(Bytes, SocketAddr)>,
    message_incoming: Sender<(Bytes, usize)>,
    message_outgoing: Receiver<(Bytes, usize)>,
}

impl Handle {
    pub fn new(
        packet_incoming: Receiver<(Bytes, SocketAddr)>,
        packet_outgoing: Sender<(Bytes, SocketAddr)>,
        message_incoming: Sender<(Bytes, usize)>,
        message_outgoing: Receiver<(Bytes, usize)>,
    ) -> Self {
        Self {
            addr_to_index: FxHashMap::with_capacity_and_hasher(CLIENT_CAPACITY, Default::default()),
            available_client_indices: (0..CLIENT_CAPACITY).rev().collect(),
            clients: vec![None; CLIENT_CAPACITY],
            packet_incoming,
            packet_outgoing,
            message_incoming,
            message_outgoing,
        }
    }

    /// `Err(_)` <=> or :
    /// - A channel fail while receiving.
    pub fn handle(&mut self) -> Result<()> {
        select! {
            recv(self.packet_incoming) -> result => {
                let (packet, client_addr) = result?;
                self.handle_incoming_packet(packet, client_addr)?;
            }
            recv(self.message_outgoing) -> result => {
                let (message, client_index) = result?;
                self.handle_outgoing_message(message, client_index)?;
            }
        }
        Ok(())
    }

    fn handle_incoming_packet(&mut self, packet: Bytes, client_addr: SocketAddr) -> Result<()> {
        let client_index = match self.addr_to_index.get(&client_addr).copied() {
            Some(client_index) => client_index,
            None => match self.available_client_indices.pop() {
                Some(client_index) => {
                    self.addr_to_index
                        .insert(client_addr, client_index);
                    self.clients[client_index] = Some(client_addr);
                    client_index
                }
                None => return Err("client max capacity reached".into()),
            },
        };

        self.message_incoming.send((packet, client_index))?;

        Ok(())
    }

    fn handle_outgoing_message(&mut self, message: Bytes, client_index: usize) -> Result<()> {
        let Some(client_addr) = self.clients[client_index] else {
            return Err("unknown client".into());
        };
        self.packet_outgoing.send((message, client_addr))?;
        Ok(())
    }
}
