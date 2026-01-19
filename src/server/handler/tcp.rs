use std::{
    collections::VecDeque,
    io,
    net::{SocketAddr, TcpListener, TcpStream},
};

use polling::{Event, PollMode, Poller};
use thiserror::Error;

// ===================================================================================
// Handler
// ===================================================================================

pub struct TcpHandler {
    // ---- I/O ----
    socket: TcpListener,

    // ---- Buffers ----
    read_queue: VecDeque<(TcpStream, SocketAddr)>,
}

impl TcpHandler {
    pub fn create(socket: TcpListener, poller: &Poller, key: usize) -> io::Result<Self> {
        // ---- I/O Setup ----

        // - Set socket to non-blocking.
        // - Add socket to poller with read interest.

        socket.set_nonblocking(true)?;

        unsafe {
            poller.add_with_mode(&socket, Event::readable(key), PollMode::Level)?;
        }

        // ----

        Ok(Self {
            socket,
            read_queue: VecDeque::new(),
        })
    }

    pub fn destroy(&mut self, poller: &Poller) -> io::Result<()> {
        // ---- I/O Shutdown ----

        // - Remove socket from poller.

        poller.delete(&self.socket)?;

        // ----

        Ok(())
    }
}

// ==========================================================================
// Read
// ==========================================================================

impl TcpHandler {
    /// Buffer `socket` incoming connections.
    pub fn read(&mut self) -> Result<(), ReadError> {
        loop {
            match self.socket.accept() {
                Ok(connection) => self.read_queue.push_front(connection),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e.into()),
            };
        }
    }
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct ReadError(#[from] io::Error);

// ==========================================================================
// Next incoming
// ==========================================================================

impl TcpHandler {
    pub fn next_incoming_connection(&mut self) -> Option<(TcpStream, SocketAddr)> {
        self.read_queue.pop_back()
    }
}
