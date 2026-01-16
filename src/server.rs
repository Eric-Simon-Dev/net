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

pub use crate::protocol::MAX_PAYLOAD_LENGTH;

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

#[derive(Debug, Clone, Default)]
pub struct IncomingMessage {
    pub client_id: usize,
    pub data: BytesMut,
    pub channel: u8,
}

impl IncomingMessage {
    pub fn is_notification(&self) -> Option<Notification> {
        Notification::decode_from(self)
    }
}

#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub client_id: usize,
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
    pub fn process_available_operations(&self) -> io::Result<()> {
        self.0.notify()
    }
}

// ---- Notification ----

/// `Notification` is encoded into and transmitted via `IncomingMessage`.
///
/// `client_id` field is used to separate normal messages from notifications.
/// We set it to special constants never attributed to actual clients.
///
/// The notification data is then encoded into `data` field.
pub enum Notification {
    ClientConnected { addr: SocketAddr, client_id: usize },
    ClientDisconnected { client_id: usize },
}

// ---- Notification constants ----
const CLIENT_CONNECTED: usize = usize::MAX;
const CLIENT_DISCONNECTED: usize = usize::MAX - 1;

impl Notification {
    pub fn encode_as_message(&self) -> IncomingMessage {
        let mut message = IncomingMessage::default();
        match *self {
            Notification::ClientConnected { addr, client_id } => {
                // Encode variant.
                message.client_id = CLIENT_CONNECTED;

                // Endode `client_id`.
                message.data.put(&client_id.to_ne_bytes()[..]);

                // Encode `addr`.
                message.data.put(addr.to_string().as_bytes());
            }
            Notification::ClientDisconnected { client_id } => {
                // Encode variant.
                message.client_id = CLIENT_DISCONNECTED;

                // Endode `client_id`.
                message.data.put(&client_id.to_ne_bytes()[..]);
            }
        }
        message
    }

    pub fn decode_from(message: &IncomingMessage) -> Option<Self> {
        // Decode variant.
        match message.client_id {
            CLIENT_CONNECTED => {
                // Decode `client_id`.
                let client_id_bytes = message.data[..size_of::<usize>()].try_into().unwrap();
                let client_id = usize::from_ne_bytes(client_id_bytes);

                // Decode `addr`.
                let addr_bytes = &message.data[size_of::<usize>()..];
                let addr_str = std::str::from_utf8(addr_bytes).unwrap();
                let addr = SocketAddr::from_str(addr_str).unwrap();

                Some(Notification::ClientConnected { addr, client_id })
            }
            CLIENT_DISCONNECTED => {
                // Decode `client_id`.
                let client_id_bytes = message.data[..size_of::<usize>()].try_into().unwrap();
                let client_id = usize::from_ne_bytes(client_id_bytes);

                Some(Notification::ClientDisconnected { client_id })
            }
            _ => None,
        }
    }
}
