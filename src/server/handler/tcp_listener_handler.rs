use std::{
    io,
    net::{SocketAddr, TcpListener, TcpStream},
};

use polling::{Event, PollMode, Poller};

pub struct TcpListenerHandler {
    socket: TcpListener,
}

impl TcpListenerHandler {
    pub fn new(tcp_listener: TcpListener, poller: &Poller, key: usize) -> io::Result<Self> {
        // Set socket to non-blocking.
        tcp_listener.set_nonblocking(true)?;

        // Set readable interest.
        let current_interest = Event::readable(key);
        (unsafe { poller.add_with_mode(&tcp_listener, current_interest, PollMode::Level) })?;

        Ok(Self {
            socket: tcp_listener,
        })
    }

    pub fn next_connection(&mut self) -> io::Result<Option<(TcpStream, SocketAddr)>> {
        match self.socket.accept() {
            Ok(connection) => Ok(Some(connection)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }
}
