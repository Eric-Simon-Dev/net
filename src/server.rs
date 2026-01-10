mod handler;
mod reactor;

use std::{
    io,
    net::{SocketAddr, TcpListener, ToSocketAddrs, UdpSocket},
    str::FromStr,
    sync::{
        Arc,
        mpsc::{self, Receiver, Sender},
    },
};

use bytes::{BufMut, BytesMut};
use polling::Poller;

use handler::Handler;

pub fn listen(
    local_addr: impl ToSocketAddrs,
) -> io::Result<(Sender<OutgoingMessage>, Receiver<IncomingMessage>, Waker)> {
    // Create i/o.
    let tcp = TcpListener::bind(&local_addr)?;
    let udp = UdpSocket::bind(&local_addr)?;
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

impl IncomingMessage {
    pub fn is_notification(&self) -> Option<Notification> {
        Notification::decode_from(self)
    }
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

// ---- Notification ----

const CLIENT_CONNECTED: usize = usize::MAX;
const CLIENT_DISCONNECTED: usize = usize::MAX - 1;

pub enum Notification {
    ClientConnected { addr: SocketAddr, key: usize },
    ClientDisconnected { key: usize },
}

impl Notification {
    pub fn encode_as_message(&self) -> IncomingMessage {
        let mut message = IncomingMessage {
            data: BytesMut::new(),
            channel: 0,
            client_key: 0,
        };
        match *self {
            Notification::ClientConnected { addr, key } => {
                // Encode variant.
                message.client_key = CLIENT_CONNECTED;

                // Endode `key`.
                message.data.put(&key.to_ne_bytes()[..]);

                // Encode `addr`.
                message.data.put(addr.to_string().as_bytes());
            }
            Notification::ClientDisconnected { key } => {
                // Encode variant.
                message.client_key = CLIENT_DISCONNECTED;

                // Endode `key`.
                message.data.put(&key.to_ne_bytes()[..]);
            }
        }
        message
    }

    pub fn decode_from(message: &IncomingMessage) -> Option<Self> {
        // Decode variant.
        match message.client_key {
            CLIENT_CONNECTED => {
                // Decode `key`.
                let key_bytes = message.data[..size_of::<usize>()].try_into().unwrap();
                let key = usize::from_ne_bytes(key_bytes);

                // Decode `addr`.
                let addr_bytes = &message.data[size_of::<usize>()..];
                let addr_str = std::str::from_utf8(addr_bytes).unwrap();
                let addr = SocketAddr::from_str(addr_str).unwrap();

                Some(Notification::ClientConnected { addr, key })
            }
            CLIENT_DISCONNECTED => {
                // Decode `key`.
                let key_bytes = message.data[..size_of::<usize>()].try_into().unwrap();
                let key = usize::from_ne_bytes(key_bytes);

                Some(Notification::ClientDisconnected { key })
            }
            _ => None,
        }
    }
}
