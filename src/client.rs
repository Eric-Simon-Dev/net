mod handler;
mod reactor;

use std::{
    io,
    net::{TcpStream, ToSocketAddrs, UdpSocket},
    sync::{
        Arc,
        mpsc::{self, Receiver, Sender},
    },
};

use bytes::BytesMut;
use polling::Poller;

pub use crate::protocol::MAX_PAYLOAD_LENGTH;

use handler::Handler;

// ===================================================================================
// Connect
// ===================================================================================

pub fn connect(
    local_addr: impl ToSocketAddrs,
    server_addr: impl ToSocketAddrs,
) -> io::Result<(Sender<Outgoing>, Receiver<Incoming>, Waker)> {
    // Create i/o primitives.
    let tcp_stream = TcpStream::connect(&server_addr)?;
    let udp = UdpSocket::bind(&local_addr)?;
    udp.connect(&server_addr)?;
    let poller = Arc::new(Poller::new()?);

    // Create communication.
    let incoming = mpsc::channel();
    let outgoing = mpsc::channel();
    let waker = Waker(poller.clone());

    // Create handler.
    let handler = Handler::new(tcp_stream, udp, &poller, incoming.0, outgoing.1)?;

    reactor::start(poller, handler);

    Ok((outgoing.0, incoming.1, waker))
}

// ===================================================================================
// Communication
// ===================================================================================

// ---- Incoming ----

#[derive(Debug, Clone)]
pub enum Incoming {
    Message(IncomingMessage),
    Notification(Notification),
}

#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub data: BytesMut,
    pub channel: u8,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Notification {}

// ---- Outgoing ----

#[derive(Debug, Clone)]
pub enum Outgoing {
    Message(OutgoingMessage),
    Command(Command),
}

#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub data: BytesMut,
    pub channel: u8,
    pub guarantees: Guarantees,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Guarantees {
    None,
    Delivery,
    DeliveryOrder,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Command {
    Shutdown,
}

// ---- Waker ----

pub struct Waker(Arc<Poller>);

impl Waker {
    pub fn notify_reactor(&self) -> io::Result<()> {
        self.0.notify()
    }
}
