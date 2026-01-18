//! Datagram = header + payload.

mod client;

use std::{
    collections::VecDeque,
    io,
    net::{SocketAddr, UdpSocket},
};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};
use thiserror::Error;

use crate::protocol::udp::{Header, MAX_PACKET_SIZE};

use super::{IncomingMessage, OutgoingMessage};

use client::ClientRegistry;

// ===================================================================================
// Handler
// ===================================================================================

pub struct UdpHandler {
    // ---- Clients ----
    pub clients: ClientRegistry,

    // ---- Socket ----
    socket: UdpSocket,
    interest: Event,

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
            clients: ClientRegistry::new(),
            socket,
            interest,
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
// Read
// ==========================================================================

impl UdpHandler {
    /// Drain `socket` data into `recv_buf`.
    pub fn read(&mut self) -> Result<(), ReadError> {
        let mut buf = [0; MAX_PACKET_SIZE];
        loop {
            match self.socket.recv_from(&mut buf) {
                // Receive `n` bytes from `addr`.
                Ok((n, addr)) => {
                    self.process_incoming_datagram(&buf, n, addr);
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

    fn process_incoming_datagram(&mut self, buf: &[u8], n: usize, addr: SocketAddr) {
        // Drop if `addr` unregistered.
        let Some(key) = self.clients.get_key(&addr) else {
            return;
        };
        let client = &mut self.clients[key];

        // Buffer datagram.
        self.recv_buf.put(&buf[..n]);
        let mut datagram = self.recv_buf.split();

        // Drop if header unparsable.
        let Ok(header) = Header::split_from(&mut datagram) else {
            return;
        };
        let payload = datagram;

        // Drop if header seq number invalid.
        if !client.recv_seq_window.check_and_mark(header.seq()) {
            return;
        }

        self.recv_queue.push_front(IncomingMessage {
            client_id: key,
            data: payload,
            channel: header.channel(),
        });
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
#[error(transparent)]
pub struct ReadError(#[from] io::Error);

// ==========================================================================
// Incoming
// ==========================================================================

impl UdpHandler {
    /// Return message from `recv_buf`.
    pub fn incoming(&mut self) -> Option<IncomingMessage> {
        self.recv_queue.pop_back()
    }
}

// ==========================================================================
// Write
// ==========================================================================

impl UdpHandler {
    /// Fill `socket` from `send_buf` data.
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
// Outgoing
// ==========================================================================

impl UdpHandler {
    /// Enqueue message into `send_buf`.
    pub fn outgoing(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), OutgoingError> {
        let client = &mut self.clients[message.client_id];

        // Create & Buffer header.
        let header = Header::Classic {
            channel: message.channel,
            seq: client.send_seq,
        };
        client.send_seq += 1;
        header.put_into(&mut self.send_buf);

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
pub enum OutgoingError {
    #[error("failed to update interest: {0}")]
    UpdateInterest(#[from] io::Error),
}
