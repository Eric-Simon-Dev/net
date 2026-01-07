//! Datagram = header + payload.

mod client;

use std::{
    collections::VecDeque,
    io,
    net::UdpSocket,
    sync::mpsc::{SendError, Sender},
};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};
use thiserror::Error;

use crate::protocol::udp::{Header, MAX_PACKET_SIZE};

use super::{IncomingMessage, OutgoingMessage};

use client::ClientRegistry;

pub struct UdpHandler {
    // ---- Clients ----
    pub clients: ClientRegistry,

    // ---- Socket ----
    socket: UdpSocket,
    current_interest: Event,

    // ---- Buffers ----
    read_buf: BytesMut,
    write_buf: BytesMut,
    datagrams_queue: VecDeque<AddressedDatagram>,
}

struct AddressedDatagram {
    datagram: Bytes,
    key: usize,
}

impl UdpHandler {
    pub fn new(socket: UdpSocket, poller: &Poller, key: usize) -> io::Result<Self> {
        // Set socket to non-blocking.
        socket.set_nonblocking(true)?;

        // Set readable interest.
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&socket, current_interest, PollMode::Level) })?;

        Ok(Self {
            clients: ClientRegistry::new(),
            socket,
            current_interest,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            datagrams_queue: VecDeque::new(),
        })
    }
}

// ==========================================================================
// Queue outgoing message
// ==========================================================================

impl UdpHandler {
    pub fn queue_outgoing_message(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), QueueOutgoingMessageError> {
        // Get client or drop.
        let Some(client) = self.clients.get_mut_by_key(message.client_key) else {
            return Ok(());
        };

        // Create header.
        let header = Header::Classic {
            channel: message.channel,
            seq: client.send_seq,
        };
        client.send_seq += 1;

        // Buffer and queue datagram.
        header.put_into(&mut self.write_buf);
        self.write_buf.put(message.data);
        let datagram = self.write_buf.split().freeze();
        self.datagrams_queue.push_front(AddressedDatagram {
            datagram,
            key: message.client_key,
        });

        // Set writable interest.
        if !self.current_interest.writable {
            self.current_interest.writable = true;
            poller.modify(&self.socket, self.current_interest)?;
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum QueueOutgoingMessageError {
    #[error("failed to update poller interest: {0}")]
    PollerInterest(#[from] io::Error),
}

// ==========================================================================
// Handle event
// ==========================================================================

impl UdpHandler {
    pub fn handle_event(
        &mut self,
        poller: &Poller,
        event: Event,
        incoming: &mut Sender<IncomingMessage>,
    ) -> Result<(), HandleEventError> {
        if event.readable {
            while let Some((header, payload)) = self.decode_next_datagram_from_socket()? {
                incoming.send(IncomingMessage {
                    data: payload,
                    channel: header.channel(),
                    client_key: event.key,
                })?;
            }
        }
        if event.writable {
            self.fill_socket_from_write_buf(poller)?;
        }
        Ok(())
    }

    fn decode_next_datagram_from_socket(&mut self) -> io::Result<Option<(Header, BytesMut)>> {
        let mut buf = [0; MAX_PACKET_SIZE];
        loop {
            // Receive packet.
            let (n, peer_addr) = match self.socket.recv_from(&mut buf) {
                Ok(recv) => recv,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) => return Err(e),
            };

            // Drop packet if client is unknown.
            let Some(client) = self.clients.get_mut_by_addr(peer_addr) else {
                continue;
            };

            // Buffer datagram.
            self.read_buf.put(&buf[..n]);
            let mut datagram = self.read_buf.split();

            // Parse header or drop.
            let header = match Header::split_from(&mut datagram) {
                Ok(header) => header,
                Err(_) => continue,
            };
            let payload = datagram;

            // Drop packet if not in client window.
            if !client.recv_seq_window.check_and_mark(header.seq()) {
                continue;
            }

            return Ok(Some((header, payload)));
        }
    }

    fn fill_socket_from_write_buf(&mut self, poller: &Poller) -> io::Result<()> {
        while let Some(AddressedDatagram { datagram, key }) = self.datagrams_queue.pop_back() {
            // Get client or drop.
            let Some(client) = self.clients.get_mut_by_key(key) else {
                continue;
            };

            match self.socket.send_to(&datagram, client.addr) {
                // Complete write.
                Ok(_) => (),

                // Socket full: Push back into queue and return without error.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.datagrams_queue
                        .push_back(AddressedDatagram { datagram, key });
                    return Ok(());
                }

                // Fatal error.
                Err(e) => return Err(e),
            }
        }
        // Exiting loop => All queued datagrams have been sent.

        // Remove writable interest.
        self.current_interest.writable = false;
        poller.modify(&self.socket, self.current_interest)?;

        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleEventError {
    #[error("failed to send message into channel: {0}")]
    Channel(#[from] SendError<IncomingMessage>),

    #[error("failed to read/write socket or update poller interest: {0}")]
    Io(#[from] io::Error),
}
