//! Frame = header + payload.

use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::{Shutdown, TcpStream},
};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};
use thiserror::Error;

use crate::protocol::tcp::{Header, CreateHeaderError, DecodeHeaderError};

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
    /// Drain `socket` data into `recv_buf`.
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
// Incoming
// ==========================================================================

impl TcpStreamHandler {
    /// Return message from `recv_buf`.
    pub fn incoming(&mut self) -> Result<Option<IncomingMessage>, IncomingError> {
        match Header::split_frame_from(&mut self.recv_buf) {
            Ok((header, payload)) => Ok(Some(IncomingMessage {
                data: payload,
                channel: header.channel,
                client_id: self.key,
            })),
            Err(DecodeHeaderError::BufferTooSmall) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IncomingError {
    #[error("failed to decode header: {0}")]
    DecodeHeader(#[from] DecodeHeaderError),
}

// ==========================================================================
// Write
// ==========================================================================

impl TcpStreamHandler {
    /// Fill `socket` from `send_buf` data.
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
// Outgoing
// ==========================================================================

impl TcpStreamHandler {
    /// Enqueue message into `send_buf`.
    pub fn outgoing(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), OutgoingError> {
        // Create & Buffer header.
        let header = Header::new(message.data.len(), message.channel)?;
        header.put_into(&mut self.send_buf);

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
pub enum OutgoingError {
    #[error("failed to create header: {0}")]
    CreateHeader(#[from] CreateHeaderError),

    #[error("failed to update interest: {0}")]
    UpdateInterest(#[from] io::Error),
}
