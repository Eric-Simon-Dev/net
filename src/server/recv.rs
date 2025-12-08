use std::net::{SocketAddr, UdpSocket};

use bytes::{Bytes, BytesMut};
use crossbeam::channel::Sender;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

const REALLOCATION_SIZE: usize = 2_usize.pow(16);

/// Receive packets from network. Send them to manager.
///
/// ## Usage
///
/// Meant to be used in its own thread looping over `recv()`.
///
/// Blocks <=> or :
/// - Wait for a packet to receive.
/// - Wait for incoming channel to have space.
///
/// ```ignore
/// loop {
///     match recv.recv() {
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
pub struct Recv {
    socket: UdpSocket,
    buf: BytesMut,
    max_msg_size: usize,
    incoming: Sender<(Bytes, SocketAddr)>,
}

impl Recv {
    /// `Err(_)` <=> or :
    /// - `socket` unbound.
    /// - `socket` bound but not IPv4.
    pub fn new(
        socket: UdpSocket,
        max_msg_size: usize,
        incoming: Sender<(Bytes, SocketAddr)>,
    ) -> Result<Self> {
        //------// Checks //------//

        let Ok(local_addr) = socket.local_addr() else {
            return Err("socket unbound".into());
        };
        if !local_addr.is_ipv4() {
            return Err("socket not IPv4".into());
        }

        //------//

        Ok(Self {
            socket,
            buf: BytesMut::zeroed(max_msg_size),
            max_msg_size,
            incoming,
        })
    }

    /// `Ok(_)` <=> A packet was transmitted.
    ///
    /// `Err(_)` <=> or :
    /// - Socket fatal error while listening.
    /// - Channel disconnected.
    ///
    /// Blocking <=> or :
    /// - Waiting for packet.
    /// - Waiting for channel to have space.
    pub fn recv(&mut self) -> Result<()> {
        //------// Wait packet //------//

        // Receive a packet from socket (fallible, blocking).
        //
        // Buffer packet.
        // Bytes out of buffer bounds are discarded.

        let (packet_len, client_addr) = self.socket.recv_from(&mut self.buf)?;

        //------// Send message //------//

        // Take message ownership from buffer.
        // This reduce buffer size.
        //
        // Eventually reallocate buffer capacity for next messages.
        // Resize buffer for next message.
        //
        // Send message through incoming (fallible, blocking).

        let packet = self.buf.split_to(packet_len).freeze();

        if self.buf.capacity() < self.max_msg_size {
            self.buf.reserve(REALLOCATION_SIZE);
        }
        self.buf.resize(self.max_msg_size, 0);

        self.incoming.send((packet, client_addr))?;

        //------//

        Ok(())
    }
}
