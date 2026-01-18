use std::{
    io,
    net::{SocketAddr, TcpListener, TcpStream},
};

use polling::{Event, PollMode, Poller};
use thiserror::Error;

// ===================================================================================
// Handler
// ===================================================================================

pub struct TcpHandler {
    socket: TcpListener,
}

impl TcpHandler {
    pub fn create(socket: TcpListener, poller: &Poller, key: usize) -> io::Result<Self> {
        // Set socket to non-blocking.
        socket.set_nonblocking(true)?;

        // Add socket to poller with read interest.
        (unsafe { poller.add_with_mode(&socket, Event::readable(key), PollMode::Level) })?;

        Ok(Self { socket })
    }

    pub fn destroy(&mut self, poller: &Poller) -> io::Result<()> {
        // Remove socket from poller.
        poller.delete(&self.socket)?;

        Ok(())
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
    #[error("failed to accept connection: {0}")]
    AcceptConnection(#[from] io::Error),
}
