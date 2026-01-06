//! Datagram = header + payload.

use std::{
    collections::VecDeque,
    fmt, io,
    net::{SocketAddr, UdpSocket},
    sync::mpsc::{SendError, Sender},
};

use bytes::{BufMut, Bytes, BytesMut};
use polling::{Event, PollMode, Poller};

use crate::protocol::udp::{Header, MAX_PACKET_SIZE};

use super::{ClientRegistry, IncomingMessage};

pub struct UdpSocketHandler {
    // ---- Socket ----
    socket: UdpSocket,
    current_interest: Event,

    // ---- Buffers ----
    read_buf: BytesMut,
    write_buf: BytesMut,
    datagrams_queue: VecDeque<AddressedDatagram>,
}

struct AddressedDatagram {
    datagram: Bytes,
    peer_addr: SocketAddr,
}

impl UdpSocketHandler {
    pub fn new(udp_socket: UdpSocket, poller: &Poller, key: usize) -> io::Result<Self> {
        // Set socket to non-blocking.
        udp_socket.set_nonblocking(true)?;

        // Set readable interest.
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&udp_socket, current_interest, PollMode::Level) })?;

        Ok(Self {
            socket: udp_socket,
            current_interest,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            datagrams_queue: VecDeque::new(),
        })
    }

    pub fn queue_message(
        &mut self,
        poller: &Poller,
        payload: &[u8],
        channel: u8,
        peer_addr: SocketAddr,
        peer_seq: &mut u64,
    ) -> io::Result<()> {
        // Create header.
        let header = Header::Classic {
            channel,
            seq: *peer_seq,
        };
        *peer_seq += 1;

        // Buffer and queue datagram.
        header.put_into(&mut self.write_buf);
        self.write_buf.put(payload);
        let datagram = self.write_buf.split().freeze();
        self.datagrams_queue.push_front(AddressedDatagram {
            datagram,
            peer_addr,
        });

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
        clients: &mut ClientRegistry,
    ) -> Result<(), HandleEventError> {
        if event.readable {
            while let Some((header, payload)) = self.next_parsed_datagram(clients)? {
                incoming.send(IncomingMessage {
                    data: payload,
                    channel: header.channel(),
                    client_key: event.key,
                })?;
            }
        }
        if event.writable {
            self.fill_socket(poller)?;
        }
        Ok(())
    }

    fn next_parsed_datagram(
        &mut self,
        clients: &mut ClientRegistry,
    ) -> io::Result<Option<(Header, BytesMut)>> {
        let mut buf = [0; MAX_PACKET_SIZE];
        loop {
            // Receive packet.
            let (n, peer_addr) = match self.socket.recv_from(&mut buf) {
                Ok(recv) => recv,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) => return Err(e.into()),
            };

            // Drop packet if client is unknown.
            let Some(client) = clients.get_mut_by_addr(peer_addr) else {
                continue;
            };

            // Buffer datagram.
            self.read_buf.put(&buf[..n]);
            let mut datagram = self.read_buf.split();

            // Parse header or drop.
            let header = match Header::split_from(&mut datagram) {
                Ok(header) => header,
                Err(_) => continue,
            };
            let payload = datagram;

            // Drop packet if not in client window.
            if !client.recv_seq_window.check_and_mark(header.seq()) {
                continue;
            }

            return Ok(Some((header, payload)));
        }
    }

    /// Try draining datagram queue until socket would block.
    fn fill_socket(&mut self, poller: &Poller) -> io::Result<()> {
        while let Some(AddressedDatagram {
            datagram,
            peer_addr,
        }) = self.datagrams_queue.pop_back()
        {
            match self.socket.send_to(&datagram, peer_addr) {
                // Complete write.
                Ok(_) => (),

                // Socket full: Push back into queue and return without error.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.datagrams_queue.push_back(AddressedDatagram {
                        datagram,
                        peer_addr,
                    });
                    return Ok(());
                }

                // Fatal error.
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

// ---- Errors ----

#[derive(Debug)]
pub enum HandleEventError {
    ChannelDisconnected,
    Io(io::Error),
}

impl From<io::Error> for HandleEventError {
    fn from(e: io::Error) -> Self {
        HandleEventError::Io(e)
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
            HandleEventError::Io(e) => write!(f, "IO error: {}", e),
            HandleEventError::ChannelDisconnected => write!(f, "Channel disconnected"),
        }
    }
}

impl std::error::Error for HandleEventError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HandleEventError::Io(e) => Some(e),
            _ => None,
        }
    }
}
