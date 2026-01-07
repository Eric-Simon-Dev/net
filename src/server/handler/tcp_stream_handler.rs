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

use crate::protocol::tcp::{Header, HeaderCreateError, HeaderDecodeError};

use super::{IncomingMessage, OutgoingMessage};

pub struct TcpStreamHandler {
    // ---- Socket ----
    socket: TcpStream,
    current_interest: Event,

    // ---- Buffers ----
    read_buf: BytesMut,
    write_buf: BytesMut,
    frames_queue: VecDeque<Bytes>,
}

impl TcpStreamHandler {
    pub fn create(socket: TcpStream, poller: &Poller, key: usize) -> io::Result<Self> {
        // Set socket to non-blocking.
        socket.set_nonblocking(true)?;

        // Set readable interest.
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&socket, current_interest, PollMode::Level) })?;

        Ok(Self {
            socket,
            current_interest,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            frames_queue: VecDeque::new(),
        })
    }

    pub fn destroy(self, poller: &Poller) -> io::Result<()> {
        self.socket.shutdown(Shutdown::Both)?;
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

        // Buffer and queue frame.
        header.put_into(&mut self.write_buf);
        self.write_buf.put(message.data);
        let frame = self.write_buf.split().freeze();
        self.frames_queue.push_front(frame);

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
    #[error("failed to create frame header: {0}")]
    Header(#[from] HeaderCreateError),

    #[error("failed to update poller interest: {0}")]
    PollerInterest(#[from] io::Error),
}

// ==========================================================================
// Handle event
// ==========================================================================

impl TcpStreamHandler {
    pub fn handle_event(
        &mut self,
        poller: &Poller,
        event: Event,
        incoming: &mut Sender<IncomingMessage>,
    ) -> Result<(), HandleEventError> {
        if event.readable {
            self.drain_socket_into_read_buf()?;
            while let Some((header, payload)) = self.decode_next_frame_from_read_buf()? {
                incoming.send(IncomingMessage {
                    data: payload,
                    channel: header.channel,
                    client_key: event.key,
                })?;
            }
        }
        if event.writable {
            self.fill_socket_from_write_buf(poller)?;
        }
        Ok(())
    }

    fn drain_socket_into_read_buf(&mut self) -> Result<(), HandleEventError> {
        let mut buf = [0; 4096];
        loop {
            match self.socket.read(&mut buf) {
                Ok(0) => return Err(HandleEventError::ConnectionClosed),
                Ok(n) => self.read_buf.put(&buf[..n]),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn decode_next_frame_from_read_buf(
        &mut self,
    ) -> Result<Option<(Header, BytesMut)>, HandleEventError> {
        match Header::split_frame_from(&mut self.read_buf) {
            Ok(frame) => Ok(Some(frame)),
            Err(HeaderDecodeError::BufferTooSmall) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn fill_socket_from_write_buf(&mut self, poller: &Poller) -> io::Result<()> {
        while let Some(mut frame) = self.frames_queue.pop_back() {
            match self.socket.write(&frame) {
                // Partial write: Remove written bytes and push back into queue.
                Ok(n) if n < frame.len() => {
                    let _ = frame.split_to(n);
                    self.frames_queue.push_back(frame);
                }

                // Complete write.
                Ok(_) => (),

                // Socket full: Push back into queue and return without error.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.frames_queue.push_back(frame);
                    return Ok(());
                }

                // Fatal error.
                Err(e) => return Err(e),
            }
        }
        // Exiting loop => All queued frames have been sent.

        // Remove writable interest.
        self.current_interest.writable = false;
        poller.modify(&self.socket, self.current_interest)?;

        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleEventError {
    #[error("client closed connection")]
    ConnectionClosed,

    #[error("failed to decode frame header: {0}")]
    Header(#[from] HeaderDecodeError),

    #[error("failed to send message into channel: {0}")]
    Channel(#[from] SendError<IncomingMessage>),

    #[error("failed to read/write socket or update poller interest: {0}")]
    Io(#[from] io::Error),
}
