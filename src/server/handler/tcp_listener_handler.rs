use std::{
    collections::HashMap,
    io,
    net::{SocketAddr, TcpListener},
};

use polling::{Event, PollMode, Poller};
use slab::Slab;

use super::{TcpStreamHandler, Client};

pub struct TcpListenerHandler {
    socket: TcpListener,

    /// Current poller interest.
    ///
    /// Might be used when implementing maximum clients
    /// and remove read interest.
    _current_interest: Event,
}

impl TcpListenerHandler {
    pub fn new(tcp_listener: TcpListener, poller: &Poller, key: usize) -> io::Result<Self> {
        tcp_listener.set_nonblocking(true)?;
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&tcp_listener, current_interest, PollMode::Level) })?;
        Ok(Self {
            socket: tcp_listener,
            _current_interest: current_interest,
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
        addr_to_stream_key: &mut HashMap<SocketAddr, usize>,
    ) -> io::Result<()> {
        loop {
            match self.socket.accept() {
                Ok((tcp_stream, addr)) => {
                    let entry = clients.vacant_entry();
                    let tcp = TcpStreamHandler::new(tcp_stream, poller, entry.key())?;
                    let client = Client { addr, tcp };
                    addr_to_stream_key.insert(addr, entry.key());
                    entry.insert(client);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }
}
