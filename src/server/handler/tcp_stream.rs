//! Frame = header + payload.

use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::{Shutdown, TcpStream},
};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};
use thiserror::Error;

use crate::protocol::tcp::{DecodeHeaderError, EncodeHeaderError, Header};

use super::{IncomingMessage, OutgoingMessage};

// ===================================================================================
// Handler
// ===================================================================================

pub struct TcpStreamHandler {
    // ---- I/O ----
    socket: TcpStream,
    key: usize,
    interest: Event,

    // ---- Buffers ----
    recv_buf: BytesMut,
    send_buf: BytesMut,
    send_queue: VecDeque<Bytes>,
}

impl TcpStreamHandler {
    pub fn create(poller: &Poller, socket: TcpStream, key: usize) -> io::Result<Self> {
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
            key,
            interest,
            recv_buf: BytesMut::new(),
            send_buf: BytesMut::new(),
            send_queue: VecDeque::new(),
        })
    }

    pub fn destroy(&mut self, poller: &Poller) -> io::Result<()> {
        // ---- I/O Shutdown ----

        // - Remove socket from poller.
        // - Shutdown socket.

        poller.delete(&self.socket)?;

        let _ = self.socket.shutdown(Shutdown::Both);

        // ----

        Ok(())
    }
}

// ==========================================================================
// Utils
// ==========================================================================

impl TcpStreamHandler {
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

impl TcpStreamHandler {
    /// Buffer `socket` incoming data.
    pub fn read(&mut self) -> Result<(), ReadError> {
        let mut buf = [0; 4096];
        loop {
            match self.socket.read(&mut buf) {
                // Connection closed.
                Ok(0) => return Err(ReadError::ConnectionClosed),

                // Read `n` bytes.
                Ok(n) => self.recv_buf.put(&buf[..n]),

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
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReadError {
    #[error("connection closed")]
    ConnectionClosed,

    #[error(transparent)]
    Other(#[from] io::Error),
}

// ==========================================================================
// Next incoming message
// ==========================================================================

impl TcpStreamHandler {
    pub fn next_incoming_message(
        &mut self,
    ) -> Result<Option<IncomingMessage>, NextIncomingMessageError> {
        match Header::decode_from(&self.recv_buf) {
            // Decoded and complete frame available.
            Ok((header, wire_size))
                if self.recv_buf.len() >= header.payload_length as usize + wire_size =>
            {
                let _header = self.recv_buf.split_to(wire_size);
                let payload = self.recv_buf.split_to(header.payload_length);
                Ok(Some(IncomingMessage {
                    client_id: self.key,
                    data: payload,
                    channel: header.channel,
                }))
            }

            // Decoded but complete frame unavailable.
            Ok(_) => Ok(None),

            // Not enough bytes to decode.
            Err(DecodeHeaderError::BufferTooSmall) => Ok(None),

            // Invalid header data.
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum NextIncomingMessageError {
    #[error("failed to decode header: {0}")]
    DecodeHeader(#[from] DecodeHeaderError),
}

// ==========================================================================
// Write
// ==========================================================================

impl TcpStreamHandler {
    /// Send queued outgoing data.
    pub fn write(&mut self, poller: &Poller) -> Result<(), WriteError> {
        while let Some(mut frame) = self.send_queue.pop_back() {
            match self.socket.write(&frame) {
                // Connection closed.
                Ok(0) => return Err(WriteError::ConnectionClosed),

                // Write `n` bytes.
                Ok(n) => {
                    if n < frame.len() {
                        self.send_queue.push_back(frame.split_off(n));
                    }
                }

                // Socket unavailable (no write).
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::Interrupted =>
                {
                    self.send_queue.push_back(frame);
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
pub enum WriteError {
    #[error("connection closed")]
    ConnectionClosed,

    #[error(transparent)]
    Other(#[from] io::Error),
}

// ==========================================================================
// Enqueue outgoing message
// ==========================================================================

impl TcpStreamHandler {
    pub fn enqueue_outgoing_message(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), EnqueueOutgoingMessageError> {
        // Create & Encode header.
        let header = Header {
            payload_length: message.data.len(),
            channel: message.channel,
        };
        header.encode_into(&mut self.send_buf)?;

        // Buffer payload.
        self.send_buf.put(message.data);

        // Enqueue frame (header + payload).
        self.send_queue.push_front(self.send_buf.split().freeze());

        self.update_interest(poller, true)?;

        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EnqueueOutgoingMessageError {
    #[error("failed to encode header: {0}")]
    EncodeHeader(#[from] EncodeHeaderError),

    #[error("failed to update interest: {0}")]
    UpdateInterest(#[from] io::Error),
}
