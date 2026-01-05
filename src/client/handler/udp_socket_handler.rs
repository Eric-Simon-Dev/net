use std::{collections::VecDeque, io, net::UdpSocket, sync::mpsc::Sender};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};

use crate::protocol::udp::MAX_SIZE;

use super::IncomingMessage;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub struct UdpSocketHandler {
    socket: UdpSocket,

    /// Current poller interest.
    current_interest: Event,

    read_buf: BytesMut,

    write_buf: BytesMut,

    write_queue: VecDeque<Bytes>,
}

impl UdpSocketHandler {
    pub fn new(udp_socket: UdpSocket, poller: &Poller, key: usize) -> io::Result<Self> {
        udp_socket.set_nonblocking(true)?;
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&udp_socket, current_interest, PollMode::Level) })?;
        Ok(Self {
            socket: udp_socket,
            current_interest,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            write_queue: VecDeque::new(),
        })
    }

    pub fn queue_message(&mut self, poller: &Poller, data: &[u8], channel: u8) -> Result<()> {
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
            while let Some(message) = self.next_message()? {
                incoming.send(message)?;
            }
        }
        if event.writable {
            self.fill_socket(poller)?;
        }
        Ok(())
    }

    fn next_message(&mut self) -> Result<Option<IncomingMessage>> {
        let mut buf = [0; MAX_SIZE];
        loop {
            // Receive packet.
            match self.socket.recv(&mut buf) {
                Ok(n) => self.read_buf.put(&buf[..n]),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) => return Err(e.into()),
            };

            // Extract payload.
            let mut payload = self.read_buf.split();

            // Parse payload into header (single byte) and data.
            let header = payload.split_to(1);
            let data = payload;

            // Parse header.
            let channel = header[0];

            return Ok(Some(IncomingMessage { data, channel }));
        }
    }

    /// Try draining write queue until it would block.
    fn fill_socket(&mut self, poller: &Poller) -> io::Result<()> {
        while let Some(payload) = self.write_queue.pop_back() {
            match self.socket.send(&payload) {
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
