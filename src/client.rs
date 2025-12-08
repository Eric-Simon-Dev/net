//! Server-side.
//!
//! ## Terminology
//!
//! Packet = Message

mod recv;
mod send;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs, UdpSocket},
    thread,
};

use bytes::Bytes;
use crossbeam::channel::{Receiver, Sender, bounded};

use recv::Recv;
use send::Send;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

const MAX_MSG_SIZE: usize = 1024;

pub struct Client {
    // socket
    socket: UdpSocket,

    // channels
    pub incoming: Option<Receiver<Bytes>>,
    pub outgoing: Option<Sender<Bytes>>,

    // threads
    recv: Option<thread::JoinHandle<()>>,
    send: Option<thread::JoinHandle<()>>,
}

impl Client {
    /// Err(_) <=> Fail to bind socket.
    pub fn new(port: u16) -> Result<Self> {
        //------// Bind socket //------//

        // Create a new socket bound to LOCALHOST with `port` (fallible).

        let socket = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))?;

        //------//

        Ok(Self {
            socket,
            recv: None,
            send: None,
            incoming: None,
            outgoing: None,
        })
    }

    /// `Err(_)` <=> or :
    /// - Fail to connect to serve `addr`.
    /// - Fail to clone socket.
    pub fn connect(&mut self, addr: impl ToSocketAddrs) -> Result<()> {
        //------// Connect //------//

        // Connect to `addr` (fallible).
        //
        // Clone socket once connected (fallible).

        self.socket.connect(addr)?;
        let recv_socket = self.socket.try_clone()?;
        let send_socket = self.socket.try_clone()?;

        //------// Shared resources //------//

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

        self.incoming = Some(incoming_r);
        self.outgoing = Some(outgoing_s);
        self.recv = Some(recv);
        self.send = Some(send);

        Ok(())
    }
}
