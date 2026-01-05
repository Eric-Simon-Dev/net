mod tcp_listener_handler;
mod tcp_stream_handler;
mod udp_socket_handler;

use std::{
    collections::HashMap,
    io,
    net::{SocketAddr, TcpListener, UdpSocket},
    sync::mpsc::{Receiver, Sender, TryRecvError},
    time::Duration,
};

use polling::{Event, Poller};
use slab::Slab;

use super::{Guarantees, IncomingMessage, OutgoingMessage};
use tcp_listener_handler::TcpListenerHandler;
use tcp_stream_handler::TcpStreamHandler;
use udp_socket_handler::UdpSocketHandler;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

// ---- Poller keys ----
// `usize::MAX` is reserved for internal use from the crate.
const TCP_LISTENER_KEY: usize = usize::MAX - 1;
const UDP_SOCKET_KEY: usize = usize::MAX - 2;

pub struct Handler {
    // ---- Clients ----
    clients: Slab<Client>,
    addr_to_key: HashMap<SocketAddr, usize>,

    // ---- Handlers ----
    tcp: TcpListenerHandler,
    udp: UdpSocketHandler,

    // ---- Communication ----
    incoming: Sender<IncomingMessage>,
    outgoing: Receiver<OutgoingMessage>,
}

struct Client {
    // ---- Data ----
    addr: SocketAddr,
    seq: u64,

    // ---- Handler ----
    tcp: TcpStreamHandler,
}

impl Handler {
    pub fn new(
        tcp_listener: TcpListener,
        udp_socket: UdpSocket,
        poller: &Poller,
        incoming: Sender<IncomingMessage>,
        outgoing: Receiver<OutgoingMessage>,
    ) -> io::Result<Self> {
        Ok(Handler {
            clients: Slab::new(),
            addr_to_key: HashMap::new(),
            tcp: TcpListenerHandler::new(tcp_listener, poller, TCP_LISTENER_KEY)?,
            udp: UdpSocketHandler::new(udp_socket, poller, UDP_SOCKET_KEY)?,
            incoming,
            outgoing,
        })
    }

    pub fn handle_socket_event(&mut self, poller: &Poller, event: Event) -> Result<()> {
        match event.key {
            TCP_LISTENER_KEY => {
                self.tcp
                    .handle_event(poller, &mut self.clients, &mut self.addr_to_key)?
            }
            UDP_SOCKET_KEY => {
                self.udp
                    .handle_event(poller, event, &mut self.incoming, &self.addr_to_key)?
            }
            key => self.clients[key]
                .tcp
                .handle_event(poller, event, &mut self.incoming)?,
        }
        Ok(())
    }

    pub fn check_outgoing_messages(&mut self, poller: &Poller) -> Result<()> {
        loop {
            match self.outgoing.try_recv() {
                Ok(message) => self.handle_outgoing_message(poller, message)?,
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => return Err(TryRecvError::Disconnected.into()),
            }
        }
    }

    fn handle_outgoing_message(&mut self, poller: &Poller, message: OutgoingMessage) -> Result<()> {
        match message.guarantees {
            Guarantees::None => {
                // Get client or drop message.
                let Some(client) = self.clients.get_mut(message.client_key) else {
                    return Ok(());
                };

                self.udp.queue_message(
                    poller,
                    &message.data,
                    message.channel,
                    client.addr,
                    client.seq,
                )?;
                client.seq += 1;
            }
            Guarantees::Delivery | Guarantees::DeliveryOrder => {
                // Get client or drop message.
                let Some(client) = self.clients.get_mut(message.client_key) else {
                    return Ok(());
                };

                client
                    .tcp
                    .queue_message(poller, &message.data, message.channel)?;
            }
        }

        Ok(())
    }

    pub fn check_timers(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn next_timeout(&mut self) -> Option<Duration> {
        None
    }
}
