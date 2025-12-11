//! Client-side.
//!
//! ## Usage
//!
//! ```ignore
//! let client = client::connect("0:1234", "256.0.0.4:4567")?;
//! ```

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

const CHANNELS_CAPACITY: usize = 64;

/// ## Usage
///
/// ```ignore
/// // Send/Recv messages using `crossbeam` channels
/// // passing `bytes` pointers.
/// client.incoming();
/// client.outgoing();
/// ```
pub struct Client {
    incoming_message: Receiver<BytesMut>,
    outgoing_message: Sender<BytesMut>,
}

impl Client {
    pub fn incoming(&mut self) -> &mut Receiver<BytesMut> {
        &mut self.incoming_message
    }

    pub fn outgoing(&mut self) -> &mut Sender<BytesMut> {
        &mut self.outgoing_message
    }
}

/// Err(_) <=> or :
/// - Fail to bind UDP socket.
/// - Fail to connect UDP socket to `server_addr`.
/// - Fail to clone UDP socket.
pub fn connect(addr: impl ToSocketAddrs, server_addr: impl ToSocketAddrs) -> Result<Client> {
    //------// Socket //------//

    // Bind UDP socket (fallible).
    //
    // Connect to `server_addr` (fallible).
    //
    // Clone socket (fallible).

    let socket = UdpSocket::bind(addr)?;
    socket.connect(server_addr)?;
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
        incoming_message.0,
        outgoing_packet.0,
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

    Ok(Client {
        incoming_message: incoming_message.1,
        outgoing_message: outgoing_message.0,
    })
}
