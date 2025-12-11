//! Server-side.
//!
//! ## Usage
//!
//! ```ignore
//! let server = server::listen("0:4567")?;
//! ```
//!
//! Use `CLIENT_CAPACITY` to allocate space for clients.

mod handle;
mod recv;
mod send;

use std::{
    net::{ToSocketAddrs, UdpSocket},
    thread,
};

use bytes::BytesMut;
use crossbeam::channel::{Receiver, Sender, bounded};

use handle::Handler;
use recv::UdpPacketReceiver;
use send::UdpPacketSender;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub use handle::clients::CLIENT_CAPACITY;
const CHANNELS_CAPACITY: usize = 256;

/// ## Usage
///
/// ```ignore
/// // Send/Recv messages using `crossbeam` channels
/// // passing `bytes` pointers + client indices.
/// // Client indices are below `CLIENT_CAPACITY` (meant for direct indexing).
/// server.incoming();
/// server.outgoing();
/// ```
pub struct Server {
    incoming_message: Receiver<(BytesMut, usize)>,
    outgoing_message: Sender<(BytesMut, usize)>,
}

impl Server {
    pub fn incoming(&mut self) -> &mut Receiver<(BytesMut, usize)> {
        &mut self.incoming_message
    }

    pub fn outgoing(&mut self) -> &mut Sender<(BytesMut, usize)> {
        &mut self.outgoing_message
    }
}

/// Err(_) <=> or :
/// - Fail to bind UDP socket.
/// - Fail to clone UDP socket.
pub fn listen(addr: impl ToSocketAddrs) -> Result<Server> {
    //------// Socket //------//

    // Bind UDP socket (fallible).
    //
    // Clone it (fallible).

    let socket = UdpSocket::bind(addr)?;
    let recv_socket = socket.try_clone()?;
    let send_socket = socket.try_clone()?;

    //------// Channels //------//

    let incoming_packet = bounded(CHANNELS_CAPACITY);
    let outgoing_packet = bounded(CHANNELS_CAPACITY);
    let incoming_message = bounded(CHANNELS_CAPACITY);
    let outgoing_message = bounded(CHANNELS_CAPACITY);

    //------// Threads //------//

    let mut receiver = UdpPacketReceiver::new(recv_socket, incoming_packet.0);
    thread::spawn(move || {
        loop {
            match receiver.recv() {
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    });

    let mut sender = UdpPacketSender::new(send_socket, outgoing_packet.1);
    thread::spawn(move || {
        loop {
            match sender.send() {
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    });

    let mut handler = Handler::new(
        incoming_packet.1,
        outgoing_message.1,
        outgoing_packet.0,
        incoming_message.0,
    );
    thread::spawn(move || {
        loop {
            match handler.handle() {
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    });

    //------//

    Ok(Server {
        incoming_message: incoming_message.1,
        outgoing_message: outgoing_message.0,
    })
}
