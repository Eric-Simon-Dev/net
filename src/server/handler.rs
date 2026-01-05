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
    // ---- Handlers ----
    tcp: TcpListenerHandler,
    udp: UdpSocketHandler,
    streams: Slab<TcpStreamHandler>,
    addr_to_stream_key: HashMap<SocketAddr, usize>,

    // ---- Communication ----
    incoming: Sender<IncomingMessage>,
    outgoing: Receiver<OutgoingMessage>,
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
            tcp: TcpListenerHandler::new(tcp_listener, poller, TCP_LISTENER_KEY)?,
            udp: UdpSocketHandler::new(udp_socket, poller, UDP_SOCKET_KEY)?,
            streams: Slab::new(),
            addr_to_stream_key: HashMap::new(),
            incoming,
            outgoing,
        })
    }

    pub fn handle_socket_event(&mut self, poller: &Poller, event: Event) -> Result<()> {
        match event.key {
            TCP_LISTENER_KEY => {
                self.tcp
                    .handle_event(poller, &mut self.streams, &mut self.addr_to_stream_key)?
            }
            UDP_SOCKET_KEY => self.udp.handle_event(
                poller,
                event,
                &mut self.incoming,
                &self.addr_to_stream_key,
            )?,
            key => self.streams[key].handle_event(poller, event, &mut self.incoming)?,
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
                // Get peer address of drop message.
                let Some(peer_addr) = self
                    .streams
                    .get(message.client_key)
                    .map(|stream| stream.peer_addr())
                else {
                    return Ok(());
                };

                self.udp
                    .queue_message(poller, &message.data, message.channel, peer_addr)?;
            }
            Guarantees::Delivery | Guarantees::DeliveryOrder => {
                // Get stream or drop message.
                let Some(stream) = self.streams.get_mut(message.client_key) else {
                    return Ok(());
                };

                stream.queue_message(poller, &message.data, message.channel)?;
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
