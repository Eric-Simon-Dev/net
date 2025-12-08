mod handle;
mod recv;
mod send;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    thread,
};

use bytes::Bytes;
use crossbeam::channel::{Receiver, Sender, bounded};

use handle::Handle;
use recv::Recv;
use send::Send;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

const MAX_MSG_SIZE: usize = 1024;

pub struct Server {
    pub message_incoming: Receiver<(Bytes, usize)>,
    pub message_outgoing: Sender<(Bytes, usize)>,
}

impl Server {
    /// Err(_) <=> Fail to bind socket.
    pub fn new(port: u16) -> Result<Self> {
        //------// Shared resources //------//

        // socket
        let socket = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))?;
        let recv_socket = socket.try_clone()?;
        let send_socket = socket;

        // channels
        let packet_incoming = bounded(64);
        let packet_outgoing = bounded(64);
        let message_incoming = bounded(64);
        let message_outgoing = bounded(64);

        //------// Threads //------//

        // Create Recv & Send.
        //
        // Unwraps : Socket is bind to IPv4 localhost.

        let mut recv = Recv::new(recv_socket, MAX_MSG_SIZE, packet_incoming.0).unwrap();
        thread::spawn(move || {
            loop {
                match recv.recv() {
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });

        let mut send = Send::new(send_socket, packet_outgoing.1).unwrap();
        thread::spawn(move || {
            loop {
                match send.send() {
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });

        let mut handle = Handle::new(
            packet_incoming.1,
            packet_outgoing.0,
            message_incoming.0,
            message_outgoing.1,
        );
        thread::spawn(move || {
            loop {
                match handle.handle() {
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });

        //------//

        Ok(Self {
            message_incoming: message_incoming.1,
            message_outgoing: message_outgoing.0,
        })
    }
}
