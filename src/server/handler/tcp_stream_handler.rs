use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::TcpStream,
    sync::mpsc::Sender,
};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};

use crate::protocol::tcp::{Header, HeaderDecodingError};

use super::IncomingMessage;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub struct TcpStreamHandler {
    // ---- Socket ----
    socket: TcpStream,
    current_interest: Event,

    // ---- Buffers ----
    read_buf: BytesMut,
    write_buf: BytesMut,
    payloads_queue: VecDeque<Bytes>,
}

impl TcpStreamHandler {
    pub fn new(tcp_stream: TcpStream, poller: &Poller, key: usize) -> io::Result<Self> {
        // Set socket to non-blocking.
        tcp_stream.set_nonblocking(true)?;

        // Set interest to readable.
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&tcp_stream, current_interest, PollMode::Level) })?;

        Ok(Self {
            socket: tcp_stream,
            current_interest,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            payloads_queue: VecDeque::new(),
        })
    }

    pub fn queue_message(&mut self, poller: &Poller, data: &[u8], channel: u8) -> Result<()> {
        // Append header.
        let header = Header {
            length: data.len() as u32,
            channel,
        };
        header.put_into(&mut self.write_buf);

        // Append data.
        self.write_buf.put(data);

        // Extract payload and queue it.
        let payload = self.write_buf.split().freeze();
        self.payloads_queue.push_front(payload);

        // Update interest with writable.
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
            while let Some(message) = self.next_message(event.key)? {
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
                Ok(0) => return Err("close".into()),

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
    fn next_message(&mut self, key: usize) -> Result<Option<IncomingMessage>> {
        // Split frame or return Ok(none).
        let (header, payload) = match Header::split_frame_from(&mut self.read_buf) {
            Ok((header, data)) => (header, data),
            Err(HeaderDecodingError::BufferTooSmall) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        Ok(Some(IncomingMessage {
            data: payload,
            channel: header.channel,
            client_key: key,
        }))
    }

    /// Try draining write queue until it would block.
    fn fill_socket(&mut self, poller: &Poller) -> io::Result<()> {
        while let Some(mut payload) = self.payloads_queue.pop_back() {
            match self.socket.write(&payload) {
                // Partial write : Remove written bytes and push back into queue.
                Ok(n) if n < payload.len() => {
                    let _ = payload.split_to(n);
                    self.payloads_queue.push_back(payload);
                }

                // Complete write.
                Ok(_) => (),

                // Socket full.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.payloads_queue.push_back(payload);
                    return Ok(());
                }

                // Fatal error.
                Err(e) => return Err(e),
            }
        }

        // Exiting loop => All queued writes have been sent.

        // Update poller interest.
        self.current_interest.writable = false;
        poller.modify(&self.socket, self.current_interest)?;

        Ok(())
    }
}
