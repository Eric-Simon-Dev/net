use std::net::{SocketAddr, UdpSocket};

use bytes::{Bytes, BytesMut};
use crossbeam::channel::Sender;

use super::codec::{ADDR_SIZE, CodecAddrV4};

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

const REALLOCATION_SIZE: usize = 2_usize.pow(16);

/// Packet receiving structure.
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
///     match socket_recv.recv() {
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
    incoming: Sender<Bytes>,
}

impl Recv {
    /// `Err(_)` <=> or :
    /// - `socket` unbound.
    /// - `socket` bound but not IPv4.
    pub fn new(socket: UdpSocket, max_msg_size: usize, incoming: Sender<Bytes>) -> Result<Self> {
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

    /// `Ok(true)` <=> Packet was transmitted.
    ///
    /// `Ok(false)` <=> Packet was dropped because protocol was invalid.
    ///
    /// `Err(_)` <=> or :
    /// - Socket fatal error while listening.
    /// - Channel disconnected.
    ///
    /// Blocking <=> or :
    /// - Waiting for packet.
    /// - Waiting for channel to have space.
    pub fn recv(&mut self) -> Result<bool> {
        //------// Wait packet //------//

        // Receive a packet from socket (fallible, blocking).
        //
        // Buffer packet but leave bytes front for client address.
        // Bytes out of buffer bounds are discarded.

        let (packet_len, client_addr) = self.socket.recv_from(&mut self.buf[ADDR_SIZE..])?;
        let msg_len = packet_len + ADDR_SIZE;

        //------// Append client address //------//

        // Encode client address as bytes.
        //
        // Append it to the message.

        let SocketAddr::V4(client_addr) = client_addr else {
            unreachable!("cannot receive IPv6 client packet on IPv4 socket");
        };
        let client_addr = client_addr.encode();

        self.buf[..ADDR_SIZE].copy_from_slice(&client_addr);

        //------// Send message //------//

        // Take message ownership from buffer.
        // This reduce buffer size.
        //
        // Eventually reallocate buffer space for next messages.
        // Resize it for next message.
        //
        // Send message through incoming (fallible, blocking).

        let msg = self.buf.split_to(msg_len).freeze();

        if self.buf.capacity() < self.max_msg_size {
            self.buf.reserve(REALLOCATION_SIZE);
        }
        self.buf.resize(self.max_msg_size, 0);

        self.incoming.send(msg)?;

        //------//

        Ok(true)
    }
}
