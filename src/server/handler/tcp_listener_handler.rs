use std::{
    collections::HashMap,
    io,
    net::{SocketAddr, TcpListener, TcpStream},
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
        while let Some(connection) = self.next_connection()? {
            Self::add_client(poller, clients, addr_to_key, connection)?;
        }
        Ok(())
    }

    fn next_connection(&mut self) -> io::Result<Option<(TcpStream, SocketAddr)>> {
        match self.socket.accept() {
            Ok(connection) => Ok(Some(connection)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn add_client(
        poller: &Poller,
        clients: &mut Slab<Client>,
        addr_to_key: &mut HashMap<SocketAddr, usize>,
        (tcp_stream, addr): (TcpStream, SocketAddr),
    ) -> io::Result<()> {
        // Get client entry.
        let entry = clients.vacant_entry();
        let key = entry.key();

        // Create client.
        let client = Client {
            tcp: TcpStreamHandler::new(tcp_stream, poller, key)?,
            addr,
            seq: 0,
        };

        // Update states.
        entry.insert(client);
        addr_to_key.insert(addr, key);

        Ok(())
    }
}
