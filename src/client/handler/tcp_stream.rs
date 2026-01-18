//! Frame = header + payload.

use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::{Shutdown, TcpStream},
    sync::mpsc::{SendError, Sender},
};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};
use thiserror::Error;

use crate::protocol::tcp::{Header, CreateHeaderError, DecodeHeaderError};

use super::{Incoming, IncomingMessage, OutgoingMessage};

// ===================================================================================
// Handler
// ===================================================================================

pub struct TcpStreamHandler {
    // ---- Socket ----
    socket: TcpStream,
    current_interest: Event,

    // ---- Buffers ----
    recv_buf: BytesMut,
    send_buf: BytesMut,
    send_queue: VecDeque<Bytes>,
}

impl TcpStreamHandler {
    pub fn create(socket: TcpStream, poller: &Poller, key: usize) -> io::Result<Self> {
        // Set socket to non-blocking.
        socket.set_nonblocking(true)?;

        // Add socket to poller with read interest.
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&socket, current_interest, PollMode::Level) })?;

        Ok(Self {
            socket,
            current_interest,
            recv_buf: BytesMut::new(),
            send_buf: BytesMut::new(),
            send_queue: VecDeque::new(),
        })
    }

    pub fn destroy(&mut self, poller: &Poller) -> io::Result<()> {
        // Flush unsent messages.
        while !self.send_queue.is_empty() {
            self.send()?;
        }

        // Shutdown socket (notifies client).
        let _ = self.socket.shutdown(Shutdown::Both);

        // Remove socket from poller.
        poller.delete(&self.socket)?;

        Ok(())
    }
}

// ==========================================================================
// Queue outgoing message
// ==========================================================================

impl TcpStreamHandler {
    pub fn queue_outgoing_message(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), QueueOutgoingMessageError> {
        // Create header.
        let header = Header::new(message.data.len(), message.channel)?;

        // Buffer frame.
        header.put_into(&mut self.send_buf);
        self.send_buf.put(message.data);
        let frame = self.send_buf.split().freeze();

        // Queue frame.
        self.send_queue.push_front(frame);

        // Update poller to add write interest.
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
    #[error("failed to create frame header: {0}")]
    CreateFrameHeader(#[from] CreateHeaderError),

    #[error("failed to update poller interest: {0}")]
    UpdatePollerInterest(#[from] io::Error),
}

// ==========================================================================
// Handle socket event
// ==========================================================================

impl TcpStreamHandler {
    pub fn handle_socket_event(
        &mut self,
        poller: &Poller,
        event: Event,
        incomings: &mut Sender<Incoming>,
    ) -> Result<(), HandleEventError> {
        if event.readable {
            let connection_closed = self.recv().map_err(HandleEventError::ReadSocket)?;
            while let Some(message) = self.next_incoming_message()? {
                incomings.send(Incoming::Network(message))?;
            }
            if connection_closed {
                return Err(HandleEventError::ConnectionClosed);
            }
        }
        if event.writable {
            let all_sent = self.send().map_err(HandleEventError::WriteSocket)?;
            if all_sent {
                // Update poller to remove write interest.
                self.current_interest.writable = false;
                poller
                    .modify(&self.socket, self.current_interest)
                    .map_err(HandleEventError::UpdatePollerInterest)?;
            }
        }
        Ok(())
    }

    /// Drain `socket` into `recv_buf` until WouldBlock.
    ///
    /// `Ok(true)` if connection was closed.
    fn recv(&mut self) -> io::Result<bool> {
        let mut buf = [0; 4096];
        loop {
            match self.socket.read(&mut buf) {
                Ok(0) => return Ok(true),
                Ok(n) => self.recv_buf.put(&buf[..n]),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(e) => return Err(e),
            }
        }
    }

    fn next_incoming_message(&mut self) -> Result<Option<IncomingMessage>, DecodeHeaderError> {
        match Header::split_frame_from(&mut self.recv_buf) {
            Ok((header, payload)) => Ok(Some(IncomingMessage {
                data: payload,
                channel: header.channel,
            })),
            Err(DecodeHeaderError::BufferTooSmall) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Drain `send_queue` into `socket` until WouldBlock
    ///
    /// `Ok(true)` if all frames have been sent.
    fn send(&mut self) -> io::Result<bool> {
        while let Some(mut frame) = self.send_queue.pop_back() {
            match self.socket.write(&frame) {
                // Partial send: Remove sent bytes and push back remaining ones.
                Ok(n) if n < frame.len() => {
                    let _ = frame.split_to(n);
                    self.send_queue.push_back(frame);
                }

                // Complete send.
                Ok(_) => (),

                // Socket full: Push back bytes and return.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.send_queue.push_back(frame);
                    return Ok(false);
                }

                // Error.
                Err(e) => return Err(e),
            }
        }
        // Exiting loop => All frames have been sent.
        Ok(true)
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleEventError {
    #[error("client closed connection")]
    ConnectionClosed,

    #[error("failed to decode frame header: {0}")]
    DecodeFrameHeader(#[from] DecodeHeaderError),

    #[error("failed to send incoming message: {0}")]
    SendIncomingMessage(#[from] SendError<Incoming>),

    // ---- I/O ----
    #[error("failed to read from socket: {0}")]
    ReadSocket(io::Error),

    #[error("failed to write to socket: {0}")]
    WriteSocket(io::Error),

    #[error("failed to update poller interest: {0}")]
    UpdatePollerInterest(io::Error),
}
