mod handler;
mod reactor;

use std::{
    io,
    net::{SocketAddr, TcpListener, ToSocketAddrs, UdpSocket},
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
// Listen
// ===================================================================================

pub fn listen(
    local_addr: impl ToSocketAddrs,
) -> io::Result<(Sender<Outgoing>, Receiver<Incoming>, Waker)> {
    // ---- Setup ----

    // Create:
    // - I/O primitives.
    // - Interface.
    // - Handler (setup I/O behavior and initialize state).

    let tcp = TcpListener::bind(&local_addr)?;
    let udp = UdpSocket::bind(&local_addr)?;
    let poller = Arc::new(Poller::new()?);

    let incomings = mpsc::channel();
    let outgoings = mpsc::channel();
    let waker = Waker(poller.clone());

    let handler = Handler::create(tcp, udp, &poller, incomings.0, outgoings.1)?;

    // ---- Run ----

    reactor::spawn(poller, handler);

    // ----

    Ok((outgoings.0, incomings.1, waker))
}

// ===================================================================================
// Interface
// ===================================================================================

type ClientId = usize;

// ---- Incoming ----

#[derive(Debug, Clone)]
pub enum Incoming {
    Network(IncomingMessage),
    Internal(Notification),
}

// -- Message --

#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub client_id: ClientId,
    pub data: BytesMut,
    pub channel: u8,
}

// -- Notification --

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Notification {
    Connection {
        client_id: ClientId,
        addr: SocketAddr,
    },
    Disconnection {
        client_id: ClientId,
    },
}

// ---- Outgoing ----

#[derive(Debug, Clone)]
pub enum Outgoing {
    Network(OutgoingMessage),
    Internal(Command),
}

// -- Message --

#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub client_id: ClientId,
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

// -- Command --

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Command {
    Shutdown,
}

// ---- Waker ----

pub struct Waker(Arc<Poller>);

impl Waker {
    pub fn wake_reactor(&self) -> io::Result<()> {
        self.0.notify()
    }
}
