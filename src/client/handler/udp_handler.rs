//! Datagram = header + payload.

use std::{
    collections::VecDeque,
    io,
    net::UdpSocket,
    sync::mpsc::{SendError, Sender},
};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};
use thiserror::Error;

use crate::protocol::udp::{Header, MAX_PACKET_SIZE, SlidingWindow};

use super::{IncomingMessage, OutgoingMessage};

pub struct UdpHandler {
    send_seq: u64,
    recv_seq_window: SlidingWindow,

    // ---- Socket ----
    socket: UdpSocket,
    current_interest: Event,

    // ---- Buffers ----
    read_buf: BytesMut,
    write_buf: BytesMut,
    write_queue: VecDeque<Bytes>,
}

impl UdpHandler {
    pub fn new(socket: UdpSocket, poller: &Poller, key: usize) -> io::Result<Self> {
        // Set socket to non-blocking.
        socket.set_nonblocking(true)?;

        // Set readable interest.
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&socket, current_interest, PollMode::Level) })?;

        Ok(Self {
            send_seq: 0,
            recv_seq_window: SlidingWindow::new(),
            socket,
            current_interest,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            write_queue: VecDeque::new(),
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
        // Create header.
        let header = Header::Classic {
            channel: message.channel,
            seq: self.send_seq,
        };
        self.send_seq += 1;

        // Buffer datagram.
        header.put_into(&mut self.write_buf);
        self.write_buf.put(message.data);
        let datagram = self.write_buf.split().freeze();

        // Queue datagram.
        self.write_queue.push_front(datagram);

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
            while let Some(datagram) = self.next_datagram()? {
                if let Some(message) = self.validate_datagram(datagram) {
                    incoming.send(message)?;
                }
            }
        }
        if event.writable {
            self.send_datagrams(poller)?;
        }
        Ok(())
    }

    fn next_datagram(&mut self) -> io::Result<Option<BytesMut>> {
        let mut buf = [0; MAX_PACKET_SIZE];
        loop {
            let n = match self.socket.recv(&mut buf) {
                Ok(recv) => recv,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) => return Err(e),
            };

            // Buffer datagram.
            self.read_buf.put(&buf[..n]);
            let datagram = self.read_buf.split();

            return Ok(Some(datagram));
        }
    }

    fn validate_datagram(&mut self, mut datagram: BytesMut) -> Option<IncomingMessage> {
        // Parse header or drop.
        let header = match Header::split_from(&mut datagram) {
            Ok(header) => header,
            Err(_) => return None,
        };
        let payload = datagram;

        // Validate seq or drop.
        if !self.recv_seq_window.check_and_mark(header.seq()) {
            return None;
        }

        Some(IncomingMessage {
            data: payload,
            channel: header.channel(),
        })
    }

    fn send_datagrams(&mut self, poller: &Poller) -> io::Result<()> {
        while let Some(datagram) = self.write_queue.pop_back() {
            match self.socket.send(&datagram) {
                // Successfully send.
                Ok(_) => (),

                // Socket full.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.write_queue.push_back(datagram);
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
    Channel(#[from] SendError<IncomingMessage>),

    #[error("failed to read/write socket or update poller interest: {0}")]
    Io(#[from] io::Error),
}
