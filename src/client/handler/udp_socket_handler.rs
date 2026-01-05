use std::{collections::VecDeque, io, net::UdpSocket, sync::mpsc::Sender};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};

use crate::protocol::udp::{Header, MAX_PACKET_SIZE};

use super::IncomingMessage;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub struct UdpSocketHandler {
    // ---- Socket ----
    socket: UdpSocket,
    current_interest: Event,

    // ---- Buffers ----
    read_buf: BytesMut,
    write_buf: BytesMut,
    payloads_queue: VecDeque<Bytes>,
}

impl UdpSocketHandler {
    pub fn new(udp_socket: UdpSocket, poller: &Poller, key: usize) -> io::Result<Self> {
        // Set socket to non-blocking.
        udp_socket.set_nonblocking(true)?;

        // Set interest to readable.
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&udp_socket, current_interest, PollMode::Level) })?;

        Ok(Self {
            socket: udp_socket,
            current_interest,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            payloads_queue: VecDeque::new(),
        })
    }

    pub fn queue_message(&mut self, poller: &Poller, data: &[u8], channel: u8) -> Result<()> {
        // Append header.
        let header = Header::Classic { channel, seq: 0 };
        header.put_into(&mut self.write_buf);

        // Append data.
        self.write_buf.put(data);

        // Extract payload and queue it.
        let payload = self.write_buf.split().freeze();
        self.payloads_queue.push_front(payload);

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
        let mut buf = [0; MAX_PACKET_SIZE];
        loop {
            // Receive packet.
            match self.socket.recv(&mut buf) {
                Ok(n) => self.read_buf.put(&buf[..n]),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) => return Err(e.into()),
            };

            // Extract payload.
            let mut payload = self.read_buf.split();

            // Split header or drop.
            let header = match Header::split_from(&mut payload) {
                Ok(header) => header,
                Err(_) => continue,
            };
            let data = payload;

            // React to header
            match header {
                Header::Classic { channel, seq: _ } => {
                    return Ok(Some(IncomingMessage { data, channel }));
                }
            }
        }
    }

    /// Try draining write queue until it would block.
    fn fill_socket(&mut self, poller: &Poller) -> io::Result<()> {
        while let Some(payload) = self.payloads_queue.pop_back() {
            match self.socket.send(&payload) {
                // Complete write.
                Ok(_) => (),

                // Socket full.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.payloads_queue.push_back(payload);
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
