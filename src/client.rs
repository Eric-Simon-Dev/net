mod handler;
mod reactor;

use std::{
    io,
    net::{SocketAddr, TcpStream, UdpSocket},
    sync::{
        Arc,
        mpsc::{self, Receiver, Sender},
    },
};

use bytes::BytesMut;
use polling::{Events, Poller};

use handler::Handler;

pub fn connect(
    local_addr: SocketAddr,
    server_addr: SocketAddr,
) -> io::Result<(Sender<OutgoingMessage>, Receiver<IncomingMessage>, Waker)> {
    // Create IO = sockets + poller.
    let tcp_stream = TcpStream::connect(server_addr)?;
    let udp_socket = UdpSocket::bind(local_addr)?;
    udp_socket.connect(server_addr)?;
    let poller = Arc::new(Poller::new()?);
    let events = Events::new();

    // Create communication = channels + waker.
    let incoming = mpsc::channel();
    let outgoing = mpsc::channel();
    let waker = Waker(poller.clone());

    // Create handler.
    let handler = Handler::new(tcp_stream, udp_socket, &poller, incoming.0, outgoing.1)?;

    // Spawn reactor.
    reactor::spawn(poller, events, handler);

    Ok((outgoing.0, incoming.1, waker))
}

// ---- Messages ----

#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub data: BytesMut,
    pub channel: u8,
}

#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub data: BytesMut,
    pub channel: u8,
    pub guarantees: Guarantees,
}

// ---- Guarantees ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Guarantees {
    None,
    Delivery,
    DeliveryOrder,
}

// ---- Waker ----

pub struct Waker(Arc<Poller>);

impl Waker {
    pub fn wake(&self) -> io::Result<()> {
        self.0.notify()
    }
}
