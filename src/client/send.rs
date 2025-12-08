use std::net::UdpSocket;

use bytes::Bytes;
use crossbeam::channel::Receiver;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

/// Receive packets from manager. Send them to network.
///
/// ## Usage
///
/// Meant to be used in its own thread looping over `recv()`.
///
/// Block <=> Wait for a message to send.
///
/// ```ignore
/// loop {
///     match send.send() {
///         Ok(_) => continue,
///         Err(_) => break,
///     }
/// }
/// ```
///////////////////////////////////////////////////////////
//
// Invariants :
// - `socket` is IPv4.
//
pub struct Send {
    socket: UdpSocket,
    outgoing: Receiver<Bytes>,
}

impl Send {
    /// `Err(_)` <=> or :
    /// - `socket` unbound.
    /// - `socket` bound but not IPv4.
    pub fn new(socket: UdpSocket, outgoing: Receiver<Bytes>) -> Result<Self> {
        //------// Checks //------//

        let Ok(local_addr) = socket.local_addr() else {
            return Err("socket unbound".into());
        };
        if !local_addr.is_ipv4() {
            return Err("socket not IPv4".into());
        }

        //------//

        Ok(Self { socket, outgoing })
    }

    /// `Err(_)` <=> or :
    /// - Not connected to a server.
    /// - Outgoing has disconnected.
    /// - Socket fatal error while sending (rare).
    ///
    /// Blocking <=> Waiting for messages.
    pub fn send(&mut self) -> Result<()> {
        //------// Wait message //------//

        // Wait for a message from outgoing (fallible, blocking).

        let msg = self.outgoing.recv()?;

        //------// Send packet //------//

        // Send packet to client address (fallible).

        self.socket.send(&msg)?;

        //------//

        Ok(())
    }
}
