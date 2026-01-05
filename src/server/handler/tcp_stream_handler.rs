use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::TcpStream,
    sync::mpsc::{SendError, Sender},
};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};

use crate::protocol::tcp::{SIZE_FIELD_LEN, SizeError, read_size, write_size};

use super::IncomingMessage;

type Result<T> = std::result::Result<T, TcpStreamHandlerError>;

pub struct TcpStreamHandler {
    socket: TcpStream,
    key: usize,

    /// Current poller interest.
    current_interest: Event,

    read_buf: BytesMut,

    write_buf: BytesMut,

    /// Sendable payloads, already contain their size field.
    write_queue: VecDeque<Bytes>,
}

impl TcpStreamHandler {
    pub fn new(
        tcp_stream: TcpStream,
        poller: &Poller,
        key: usize,
    ) -> io::Result<Self> {
        tcp_stream.set_nonblocking(true)?;
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&tcp_stream, current_interest, PollMode::Level) })?;
        Ok(Self {
            socket: tcp_stream,
            key,
            current_interest,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            write_queue: VecDeque::new(),
        })
    }

    pub fn queue_message(&mut self, poller: &Poller, data: &[u8], channel: u8) -> Result<()> {
        // Append size.
        self.write_buf.resize(SIZE_FIELD_LEN, 0);
        write_size(&mut self.write_buf, data.len())?;

        // Append header.
        self.write_buf.put_u8(channel);

        // Append data.
        self.write_buf.put(data);

        // Extract payload and queue it.
        let payload = self.write_buf.split().freeze();
        self.write_queue.push_front(payload);

        // Update socket interest
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
    ) -> Result<()> {
        if event.readable {
            self.drain_socket()?;
            while let Some(message) = self.next_message()? {
                incoming.send(message)?;
            }
        }
        if event.writable {
            self.fill_socket(poller)?;
        }
        Ok(())
    }

    /// Drain socket into read buffer.
    fn drain_socket(&mut self) -> Result<()> {
        let mut buf = [0; 4096];
        loop {
            match self.socket.read(&mut buf) {
                // Client shutdown.
                Ok(0) => return Err(TcpStreamHandlerError::Closed),

                // Read.
                Ok(n) => self.read_buf.put(&buf[..n]),

                // Socket empty.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),

                // Fatal error.
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Parse read buffer for next message.
    fn next_message(&mut self) -> Result<Option<IncomingMessage>> {
        let size = match read_size(&self.read_buf) {
            Ok(size) => size,
            Err(SizeError::BufferTooSmall) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        if self.read_buf.len() >= size + SIZE_FIELD_LEN {
            // Remove size prefix.
            let _ = self.read_buf.split_to(SIZE_FIELD_LEN);

            // Extract payload.
            let mut payload = self.read_buf.split_to(size);

            // Parse payload into header (single byte) and data.
            let header = payload.split_to(1);
            let data = payload;

            // Parse header
            let channel = header[0];

            Ok(Some(IncomingMessage {
                data,
                channel,
                client_key: self.key,
            }))
        } else {
            Ok(None)
        }
    }

    /// Try draining write queue until it would block.
    fn fill_socket(&mut self, poller: &Poller) -> io::Result<()> {
        while let Some(mut payload) = self.write_queue.pop_back() {
            match self.socket.write(&payload) {
                // Partial write : Remove written bytes and push back into queue.
                Ok(n) if n < payload.len() => {
                    let _ = payload.split_to(n);
                    self.write_queue.push_back(payload);
                }

                // Complete write.
                Ok(_) => (),

                // Socket full.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.write_queue.push_back(payload);
                    return Ok(());
                }

                // Fatal error.
                Err(e) => return Err(e.into()),
            }
        }

        // Exiting loop => All queued writes have been sent.

        // Update poller interest.
        self.current_interest.writable = false;
        poller.modify(&self.socket, self.current_interest)?;

        Ok(())
    }
}

// ---- Error ----

#[derive(Debug)]
pub enum TcpStreamHandlerError {
    /// Underlying I/O error
    Io(io::Error),
    /// Framing/length field error
    Size(SizeError),
    /// Connection closed by peer
    Closed,
    /// Sending message to the channel failed (receiver dropped)
    SendError,
}

impl std::fmt::Display for TcpStreamHandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TcpStreamHandlerError::Io(e) => write!(f, "I/O error: {}", e),
            TcpStreamHandlerError::Size(e) => write!(f, "Size error: {}", e),
            TcpStreamHandlerError::Closed => write!(f, "connection closed"),
            TcpStreamHandlerError::SendError => write!(f, "incoming receiver dropped"),
        }
    }
}

impl std::error::Error for TcpStreamHandlerError {}

impl From<io::Error> for TcpStreamHandlerError {
    fn from(e: io::Error) -> Self {
        TcpStreamHandlerError::Io(e)
    }
}

impl From<SizeError> for TcpStreamHandlerError {
    fn from(e: SizeError) -> Self {
        TcpStreamHandlerError::Size(e)
    }
}

impl<T> From<SendError<T>> for TcpStreamHandlerError {
    fn from(_: SendError<T>) -> Self {
        TcpStreamHandlerError::SendError
    }
}
