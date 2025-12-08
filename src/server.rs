//! Server-side.
//!
//! ## Terminology
//!
//! Packet = Sent over network.
//!
//! Message = Packet + client info.

mod codec;
mod recv;
mod send;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    thread,
};

use bytes::Bytes;
use crossbeam::channel::{Receiver, Sender, bounded};

use recv::Recv;
use send::Send;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

const MAX_MSG_SIZE: usize = 1024;

pub struct Server {
    // threads
    pub recv: thread::JoinHandle<()>,
    pub send: thread::JoinHandle<()>,

    // channels
    pub incoming: Receiver<Bytes>,
    pub outgoing: Sender<Bytes>,
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
        let (incoming_s, incoming_r) = bounded(64);
        let (outgoing_s, outgoing_r) = bounded(64);

        //------// Threads //------//

        // Create Recv & Send.
        //
        // Unwraps : Socket is bind to IPv4 localhost.

        let mut recv = Recv::new(recv_socket, MAX_MSG_SIZE, incoming_s).unwrap();
        let recv = thread::spawn(move || {
            loop {
                match recv.recv() {
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });

        let mut send = Send::new(send_socket, outgoing_r).unwrap();
        let send = thread::spawn(move || {
            loop {
                match send.send() {
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });

        //------//

        Ok(Self {
            recv,
            send,
            incoming: incoming_r,
            outgoing: outgoing_s,
        })
    }
}
