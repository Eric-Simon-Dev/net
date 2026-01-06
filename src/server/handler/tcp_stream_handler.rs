//! Frame = header + payload.

use std::{
    collections::VecDeque,
    fmt,
    io::{self, Read, Write},
    net::TcpStream,
    sync::mpsc::{SendError, Sender},
};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};

use crate::protocol::tcp::{Header, HeaderCreateError, HeaderDecodeError};

use super::IncomingMessage;

pub struct TcpStreamHandler {
    // ---- Socket ----
    pub socket: TcpStream,
    current_interest: Event,

    // ---- Buffers ----
    read_buf: BytesMut,
    write_buf: BytesMut,
    frames_queue: VecDeque<Bytes>,
}

impl TcpStreamHandler {
    pub fn new(tcp_stream: TcpStream, poller: &Poller, key: usize) -> io::Result<Self> {
        // Set socket to non-blocking.
        tcp_stream.set_nonblocking(true)?;

        // Set readable interest.
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&tcp_stream, current_interest, PollMode::Level) })?;

        Ok(Self {
            socket: tcp_stream,
            current_interest,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            frames_queue: VecDeque::new(),
        })
    }

    pub fn queue_message(
        &mut self,
        poller: &Poller,
        payload: &[u8],
        channel: u8,
    ) -> Result<(), QueueMessageError> {
        // Create header.
        let header = Header::new(payload.len(), channel)?;

        // Buffer and queue frame.
        header.put_into(&mut self.write_buf);
        self.write_buf.put(payload);
        let frame = self.write_buf.split().freeze();
        self.frames_queue.push_front(frame);

        // Set writable interest.
        if !self.current_interest.writable {
            self.current_interest.writable = true;
            poller.modify(&self.socket, self.current_interest)?;
        }

        Ok(())
    }

    pub fn handle_event(
        &mut self,
        poller: &Poller,
        event: Event,
        incoming: &mut Sender<IncomingMessage>,
    ) -> Result<(), HandleEventError> {
        if event.readable {
            self.drain_socket()?; // Remove on client disconnection
            while let Some((header, payload)) = self.parse_next_frame()? {
                incoming.send(IncomingMessage {
                    data: payload,
                    channel: header.channel,
                    client_key: event.key,
                })?;
            }
        }
        if event.writable {
            self.fill_socket(poller)?;
        }
        Ok(())
    }

    /// Drain socket into read buffer.
    fn drain_socket(&mut self) -> Result<(), HandleEventError> {
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

    /// Try Parsing next frame from read buffer.
    fn parse_next_frame(&mut self) -> Result<Option<(Header, BytesMut)>, HandleEventError> {
        match Header::split_frame_from(&mut self.read_buf) {
            Ok(frame) => Ok(Some(frame)),
            Err(HeaderDecodeError::BufferTooSmall) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Try draining frames queue until socket would block.
    fn fill_socket(&mut self, poller: &Poller) -> io::Result<()> {
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

// ---- Errors ----

#[derive(Debug)]
#[non_exhaustive]
pub enum QueueMessageError {
    InvalidHeader(HeaderCreateError),
    Io(std::io::Error),
}

impl From<HeaderCreateError> for QueueMessageError {
    fn from(e: HeaderCreateError) -> Self {
        QueueMessageError::InvalidHeader(e)
    }
}

impl From<std::io::Error> for QueueMessageError {
    fn from(e: std::io::Error) -> Self {
        QueueMessageError::Io(e)
    }
}

impl fmt::Display for QueueMessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueMessageError::InvalidHeader(e) => write!(f, "failed to create header: {}", e),
            QueueMessageError::Io(e) => write!(f, "I/O error while queueing message: {}", e),
        }
    }
}

impl std::error::Error for QueueMessageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            QueueMessageError::InvalidHeader(e) => Some(e),
            QueueMessageError::Io(e) => Some(e),
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum HandleEventError {
    ConnectionClosed,
    InvalidHeader(HeaderDecodeError),
    ChannelDisconnected,
    Io(std::io::Error),
}

impl From<std::io::Error> for HandleEventError {
    fn from(e: std::io::Error) -> Self {
        HandleEventError::Io(e)
    }
}

impl From<HeaderDecodeError> for HandleEventError {
    fn from(e: HeaderDecodeError) -> Self {
        HandleEventError::InvalidHeader(e)
    }
}

impl<T> From<SendError<T>> for HandleEventError {
    fn from(_e: SendError<T>) -> Self {
        HandleEventError::ChannelDisconnected
    }
}

impl fmt::Display for HandleEventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandleEventError::ConnectionClosed => write!(f, "client close connection"),
            HandleEventError::InvalidHeader(e) => write!(f, "invalid protocol header: {}", e),
            HandleEventError::ChannelDisconnected => {
                write!(f, "incoming message channel disconnected")
            }
            HandleEventError::Io(e) => write!(f, "I/O error while handling socket event: {}", e),
        }
    }
}

impl std::error::Error for HandleEventError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HandleEventError::InvalidHeader(e) => Some(e),
            HandleEventError::Io(e) => Some(e),
            _ => None,
        }
    }
}
