mod handle;
mod recv;
mod send;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    thread,
};

use bytes::BytesMut;
use crossbeam::channel::{Receiver, Sender, bounded};

use handle::Handler;
use recv::UdpPacketReceiver;
use send::UdpPacketSender;

pub use handle::clients::CLIENT_CAPACITY;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

const CHANNELS_CAPACITY: usize = 64;

pub struct Server {
    incoming_message: Receiver<(BytesMut, usize)>,
    outgoing_message: Sender<(BytesMut, usize)>,
}

impl Server {
    pub fn incoming_message(&mut self) -> &mut Receiver<(BytesMut, usize)> {
        &mut self.incoming_message
    }

    pub fn outgoing_message(&mut self) -> &mut Sender<(BytesMut, usize)> {
        &mut self.outgoing_message
    }

    /// Err(_) <=> Fail to bind or clone UDP socket.
    pub fn new(port: u16) -> Result<Self> {
        //------// Socket //------//

        // Bind and clone UDP socket (fallible).

        let socket = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))?;
        let recv_socket = socket.try_clone()?;
        let send_socket = socket;

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

        Ok(Self {
            incoming_message: incoming_message.1,
            outgoing_message: outgoing_message.0,
        })
    }
}
