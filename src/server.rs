mod handler;
mod reactor;

use std::{
    io,
    net::{SocketAddr, TcpListener, UdpSocket},
    sync::{
        Arc,
        mpsc::{self, Receiver, Sender},
    },
};

use bytes::BytesMut;
use polling::Poller;

use handler::Handler;

pub fn listen(
    local_addr: SocketAddr,
) -> io::Result<(Sender<OutgoingMessage>, Receiver<IncomingMessage>, Waker)> {
    // Create i/o.
    let tcp = TcpListener::bind(local_addr)?;
    let udp = UdpSocket::bind(local_addr)?;
    let poller = Arc::new(Poller::new()?);

    // Create communication.
    let incoming = mpsc::channel();
    let outgoing = mpsc::channel();
    let waker = Waker(poller.clone());

    // Create handler.
    let handler = Handler::new(tcp, udp, &poller, incoming.0, outgoing.1)?;

    reactor::start(poller, handler);

    Ok((outgoing.0, incoming.1, waker))
}

// ---- Messages ----

#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub data: BytesMut,
    pub channel: u8,
    pub client_key: usize,
}

#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub data: BytesMut,
    pub channel: u8,
    pub client_key: usize,
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
    pub fn process_available_operations(&self) -> io::Result<()> {
        self.0.notify()
    }
}
