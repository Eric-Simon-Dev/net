//! Datagram = header + payload.

mod client_state;

use std::{
    collections::{HashMap, VecDeque},
    io,
    net::{SocketAddr, UdpSocket},
};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};
use slab::Slab;
use thiserror::Error;

use crate::protocol::udp::{Header, MAX_PACKET_SIZE};

use super::{IncomingMessage, OutgoingMessage};

use client_state::ClientState;

// ===================================================================================
// Handler
// ===================================================================================

pub struct UdpHandler {
    // ---- Socket ----
    socket: UdpSocket,
    interest: Event,

    // ---- States ----
    clients: Slab<ClientState>,
    addr_to_key: HashMap<SocketAddr, usize>,

    // ---- Buffers ----
    recv_buf: BytesMut,
    recv_queue: VecDeque<IncomingMessage>,
    send_buf: BytesMut,
    send_queue: VecDeque<(Bytes, SocketAddr)>,
}

impl UdpHandler {
    pub fn create(socket: UdpSocket, poller: &Poller, key: usize) -> io::Result<Self> {
        // ---- I/O Setup ----

        // - Set socket to non-blocking.
        // - Add socket to poller with read interest.

        socket.set_nonblocking(true)?;

        let interest = Event::readable(key);
        unsafe {
            poller.add_with_mode(&socket, interest, PollMode::Level)?;
        }

        // ----

        Ok(Self {
            socket,
            interest,
            clients: Slab::new(),
            addr_to_key: HashMap::new(),
            recv_buf: BytesMut::new(),
            recv_queue: VecDeque::new(),
            send_buf: BytesMut::new(),
            send_queue: VecDeque::new(),
        })
    }

    pub fn destroy(&mut self, poller: &Poller) -> io::Result<()> {
        // ---- I/O Shutdown ----

        // - Remove socket from poller.

        poller.delete(&self.socket)?;

        // ----

        Ok(())
    }
}

// ==========================================================================
// Utils
// ==========================================================================

impl UdpHandler {
    fn update_interest(&mut self, poller: &Poller, writable: bool) -> io::Result<()> {
        if self.interest.writable != writable {
            self.interest.writable = writable;
            poller.modify(&self.socket, self.interest)?;
        }
        Ok(())
    }
}

// ==========================================================================
// Client management
// ==========================================================================

impl UdpHandler {
    pub fn add_client_state(&mut self, addr: SocketAddr) -> usize {
        let key = self.clients.insert(ClientState::new(addr));
        self.addr_to_key.insert(addr, key);
        key
    }

    pub fn remove_client_state(&mut self, key: usize) -> ClientState {
        let client = self.clients.remove(key);
        self.addr_to_key.remove(&client.addr);
        client
    }
}

// ==========================================================================
// Read
// ==========================================================================

impl UdpHandler {
    /// Buffer `socket` incoming packets.
    pub fn read(&mut self) -> Result<(), ReadError> {
        let mut buf = [0; MAX_PACKET_SIZE];
        loop {
            match self.socket.recv_from(&mut buf) {
                // Receive `n` bytes from `addr`.
                Ok((n, addr)) => {
                    self.process_incoming_datagram(&buf[..n], addr);
                }

                // Socket/Data unavailable (no read).
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::Interrupted =>
                {
                    return Ok(());
                }

                // Socket broken.
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn process_incoming_datagram(&mut self, buf: &[u8], addr: SocketAddr) {
        // Drop if `addr` unregistered.
        let Some(key) = self.addr_to_key.get(&addr).copied() else {
            return;
        };
        let client = &mut self.clients[key];

        // Drop if header undecodable.
        let Ok((header, header_wire_size)) = Header::decode_from(buf) else {
            return;
        };
        let payload = &buf[header_wire_size..];

        // Drop if header seq number invalid.
        if !client.seq_window.accept(header.seq) {
            return;
        }

        // Buffer payload.
        self.recv_buf.put(payload);
        let payload = self.recv_buf.split();

        self.recv_queue.push_front(IncomingMessage {
            client_id: key,
            data: payload,
            channel: header.channel,
        });
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
#[error(transparent)]
pub struct ReadError(#[from] io::Error);

// ==========================================================================
// Next incoming
// ==========================================================================

impl UdpHandler {
    pub fn next_incoming_message(&mut self) -> Option<IncomingMessage> {
        self.recv_queue.pop_back()
    }
}

// ==========================================================================
// Write
// ==========================================================================

impl UdpHandler {
    /// Send queued outgoing data.
    pub fn write(&mut self, poller: &Poller) -> Result<(), WriteError> {
        while let Some((datagram, addr)) = self.send_queue.pop_back() {
            match self.socket.send_to(&datagram, addr) {
                // Write.
                Ok(_) => (),

                // Socket unavailable (no write).
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::Interrupted =>
                {
                    self.send_queue.push_back((datagram, addr));
                    return Ok(());
                }

                // Socket broken.
                Err(e) => return Err(e.into()),
            }
        }

        // Exiting while loop => All data was sent.
        self.update_interest(poller, false)?;

        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
#[error(transparent)]
pub struct WriteError(#[from] io::Error);

// ==========================================================================
// Enqueue outgoing message
// ==========================================================================

impl UdpHandler {
    pub fn enqueue_outgoing_message(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), EnqueueOutgoingMessageError> {
        let client = &mut self.clients[message.client_id];

        // Create & Encode header.
        let header = Header {
            seq: client.seq,
            channel: message.channel,
        };
        client.seq += 1;
        header.encode_into(&mut self.send_buf);

        // Buffer payload.
        self.send_buf.put(message.data);

        // Enqueue datagram (header + payload).
        self.send_queue
            .push_front((self.send_buf.split().freeze(), client.addr));

        self.update_interest(poller, true)?;

        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EnqueueOutgoingMessageError {
    #[error("failed to update interest: {0}")]
    UpdateInterest(#[from] io::Error),
}
