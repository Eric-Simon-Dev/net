//! Server-side.
//!
//! ## Terminology
//!
//! Packet = Message

mod handle;
mod recv;
mod send;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs, UdpSocket},
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

pub struct Client {
    socket: UdpSocket,
    incoming_message: Option<Receiver<BytesMut>>,
    outgoing_message: Option<Sender<BytesMut>>,
}

impl Client {
    pub fn incoming_message(&mut self) -> Option<&mut Receiver<BytesMut>> {
        self.incoming_message.as_mut()
    }

    pub fn outgoing_message(&mut self) -> Option<&mut Sender<BytesMut>> {
        self.outgoing_message.as_mut()
    }

    /// Err(_) <=> Fail to bind socket.
    pub fn new(port: u16) -> Result<Self> {
        //------// Socket //------//

        // Bind UDP socket (fallible).

        let socket = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))?;

        //------//

        Ok(Self {
            socket,
            incoming_message: None,
            outgoing_message: None,
        })
    }

    /// `Err(_)` <=> or :
    /// - Fail to connect to `addr`.
    /// - Fail to clone socket.
    pub fn connect(&mut self, addr: impl ToSocketAddrs) -> Result<()> {
        //------// Connect //------//

        // Connect to `addr` (fallible).
        //
        // Clone socket once connected (fallible).

        self.socket.connect(addr)?;
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

        self.incoming_message = Some(incoming_message.1);
        self.outgoing_message = Some(outgoing_message.0);

        Ok(())
    }
}
