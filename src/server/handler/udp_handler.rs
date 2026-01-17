//! Datagram = header + payload.

mod client;

use std::{
    collections::VecDeque,
    io,
    net::{SocketAddr, UdpSocket},
    sync::mpsc::{SendError, Sender},
};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};
use thiserror::Error;

use crate::protocol::udp::{Header, MAX_PACKET_SIZE};

use super::{Incoming, IncomingMessage, OutgoingMessage};

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
    write_queue: VecDeque<(Bytes, SocketAddr)>,
}

impl UdpHandler {
    pub fn create(socket: UdpSocket, poller: &Poller, key: usize) -> io::Result<Self> {
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
            write_queue: VecDeque::new(),
        })
    }

    pub fn destroy(&mut self, poller: &Poller) -> io::Result<()> {
        poller.delete(&self.socket)?;
        Ok(())
    }
}

// ==========================================================================
// Queue outgoing message
// ==========================================================================

impl UdpHandler {
    /// # Preconditions
    ///
    /// `message.client_key` is a registered client.
    pub fn queue_outgoing_message(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), QueueOutgoingMessageError> {
        let client = &mut self.clients[message.client_id];

        // Create header.
        let header = Header::Classic {
            channel: message.channel,
            seq: client.send_seq,
        };
        client.send_seq += 1;

        // Buffer datagram.
        header.put_into(&mut self.write_buf);
        self.write_buf.put(message.data);
        let datagram = self.write_buf.split().freeze();

        // Queue datagram.
        self.write_queue.push_front((datagram, client.addr));

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
        incoming: &mut Sender<Incoming>,
    ) -> Result<(), HandleEventError> {
        if event.readable {
            while let Some((datagram, key)) = self.next_datagram()? {
                if let Some(message) = self.validate_datagram(datagram, key) {
                    incoming.send(message)?;
                }
            }
        }
        if event.writable {
            self.send_datagrams(poller)?;
        }
        Ok(())
    }

    /// Drop datagrams from unregistered clients.
    fn next_datagram(&mut self) -> io::Result<Option<(BytesMut, usize)>> {
        let mut buf = [0; MAX_PACKET_SIZE];
        loop {
            let (n, addr) = match self.socket.recv_from(&mut buf) {
                Ok(recv) => recv,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) => return Err(e),
            };

            // Get client key or drop.
            let Some(key) = self.clients.get_key(&addr) else {
                continue;
            };

            // Buffer datagram.
            self.read_buf.put(&buf[..n]);
            let datagram = self.read_buf.split();

            return Ok(Some((datagram, key)));
        }
    }

    /// # Preconditions
    ///
    /// `key` is a registered client.
    fn validate_datagram(&mut self, mut datagram: BytesMut, key: usize) -> Option<Incoming> {
        let client = &mut self.clients[key];

        // Parse header or drop.
        let header = match Header::split_from(&mut datagram) {
            Ok(header) => header,
            Err(_) => return None,
        };
        let payload = datagram;

        // Validate seq or drop.
        if !client.recv_seq_window.check_and_mark(header.seq()) {
            return None;
        }

        Some(Incoming::Network(IncomingMessage {
            data: payload,
            channel: header.channel(),
            client_id: key,
        }))
    }

    fn send_datagrams(&mut self, poller: &Poller) -> io::Result<()> {
        while let Some((datagram, addr)) = self.write_queue.pop_back() {
            match self.socket.send_to(&datagram, addr) {
                // Successfully send.
                Ok(_) => (),

                // Socket full.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.write_queue.push_back((datagram, addr));
                    return Ok(());
                }

                // Error.
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
    Channel(#[from] SendError<Incoming>),

    #[error("failed to read/write socket or update poller interest: {0}")]
    Io(#[from] io::Error),
}
