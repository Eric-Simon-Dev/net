use std::{
    io,
    net::{SocketAddr, TcpListener, TcpStream},
};

use polling::{Event, PollMode, Poller};
use thiserror::Error;

pub struct TcpHandler {
    socket: TcpListener,
}

impl TcpHandler {
    pub fn new(socket: TcpListener, poller: &Poller, key: usize) -> io::Result<Self> {
        // Set socket to non-blocking.
        socket.set_nonblocking(true)?;

        // Set readable interest.
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&socket, current_interest, PollMode::Level) })?;

        Ok(Self { socket })
    }
}

// ==========================================================================
// Next connection
// ==========================================================================

impl TcpHandler {
    pub fn next_connection(
        &mut self,
    ) -> Result<Option<(TcpStream, SocketAddr)>, NextConnectionError> {
        match self.socket.accept() {
            Ok(connection) => Ok(Some(connection)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum NextConnectionError {
    #[error("failed to accept incoming TCP connection: {0}")]
    Accept(#[from] io::Error),
}
