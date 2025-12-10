//! Server-side network logic.
//!
//! Provide `Client` structure and `CLIENT_CAPACITY` constant.

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
/// ### Initialization
///
/// ```ignore
/// // create
/// let mut server = Server::new("0:4567")?;
///
/// // connect
/// server.listen()?;
/// ```
///
/// ### Sending & Receiving
///
/// Based on 2 ergonomic crates : `crossbeam` and `bytes`.
///
/// ```ignore
/// // example
/// let msgs = server.incoming()?.try_iter().collect();
///
/// // other example
/// let timeout = Duration::from_millis(10);
/// server.outgoing()?.send_timeout(msg, timeout);
/// ```
pub struct Server {
    socket: UdpSocket,
    incoming_message: Option<Receiver<(BytesMut, usize)>>,
    outgoing_message: Option<Sender<(BytesMut, usize)>>,
}

impl Server {
    /// `None` <=> Self is not listening (may have encounter a problem).
    pub fn incoming(&mut self) -> Option<&mut Receiver<(BytesMut, usize)>> {
        self.incoming_message.as_mut()
    }

    /// `None` <=> Self is not listening (may have encounter a problem).
    pub fn outgoing(&mut self) -> Option<&mut Sender<(BytesMut, usize)>> {
        self.outgoing_message.as_mut()
    }

    /// Err(_) <=> Fail to bind UDP socket.
    pub fn new(addr: impl ToSocketAddrs) -> Result<Self> {
        //------// Socket //------//

        // Bind UDP socket (fallible).

        let socket = UdpSocket::bind(addr)?;

        //------//

        Ok(Self {
            socket,
            incoming_message: None,
            outgoing_message: None,
        })
    }

    /// `Err(_)` <=> Fail to clone socket.
    pub fn listen(&mut self) -> Result<()> {
        //------// Socket //------//

        // Clone socket (fallible).

        let recv_socket = self.socket.try_clone()?;
        let send_socket = self.socket.try_clone()?;

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

        self.incoming_message = Some(incoming_message.1);
        self.outgoing_message = Some(outgoing_message.0);

        Ok(())
    }
}
