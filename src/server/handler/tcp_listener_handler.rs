use std::{
    collections::HashMap,
    io,
    net::{SocketAddr, TcpListener},
};

use polling::{Event, PollMode, Poller};
use slab::Slab;

use super::{Client, TcpStreamHandler};

pub struct TcpListenerHandler {
    socket: TcpListener,
}

impl TcpListenerHandler {
    pub fn new(tcp_listener: TcpListener, poller: &Poller, key: usize) -> io::Result<Self> {
        // Set socket to non-blocking.
        tcp_listener.set_nonblocking(true)?;

        // Set interest to readable.
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&tcp_listener, current_interest, PollMode::Level) })?;

        Ok(Self {
            socket: tcp_listener,
        })
    }

    pub fn handle_event(
        &mut self,
        poller: &Poller,
        clients: &mut Slab<Client>,
        addr_to_key: &mut HashMap<SocketAddr, usize>,
    ) -> io::Result<()> {
        self.drain_socket(poller, clients, addr_to_key)
    }

    fn drain_socket(
        &mut self,
        poller: &Poller,
        clients: &mut Slab<Client>,
        addr_to_key: &mut HashMap<SocketAddr, usize>,
    ) -> io::Result<()> {
        loop {
            match self.socket.accept() {
                Ok((tcp_stream, addr)) => {
                    // Create and insert client.
                    let entry = clients.vacant_entry();
                    let key = entry.key();
                    let tcp = TcpStreamHandler::new(tcp_stream, poller, key)?;
                    let client = Client { addr, seq: 0, tcp };
                    entry.insert(client);

                    // Update mapping.
                    addr_to_key.insert(addr, key);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }
}
