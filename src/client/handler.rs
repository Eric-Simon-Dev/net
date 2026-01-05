mod tcp_stream_handler;
mod udp_socket_handler;

use std::{
    io,
    net::{TcpStream, UdpSocket},
    sync::mpsc::{Receiver, Sender, TryRecvError},
    time::Duration,
};

use polling::{Event, Poller};

use super::{Guarantees, IncomingMessage, OutgoingMessage};
use tcp_stream_handler::TcpStreamHandler;
use udp_socket_handler::UdpSocketHandler;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

// ---- Poller keys ----
// `usize::MAX` is reserved for internal use from the crate.
const TCP_STREAM_KEY: usize = usize::MAX - 1;
const UDP_SOCKET_KEY: usize = usize::MAX - 2;

pub struct Handler {
    // ---- Handlers ----
    tcp: TcpStreamHandler,
    udp: UdpSocketHandler,

    // ---- Communication ----
    incoming: Sender<IncomingMessage>,
    outgoing: Receiver<OutgoingMessage>,
}

impl Handler {
    pub fn new(
        tcp_stream: TcpStream,
        udp_socket: UdpSocket,
        poller: &Poller,
        incoming: Sender<IncomingMessage>,
        outgoing: Receiver<OutgoingMessage>,
    ) -> io::Result<Self> {
        Ok(Handler {
            tcp: TcpStreamHandler::new(tcp_stream, poller, TCP_STREAM_KEY)?,
            udp: UdpSocketHandler::new(udp_socket, poller, UDP_SOCKET_KEY)?,
            incoming,
            outgoing,
        })
    }

    pub fn handle_socket_event(&mut self, poller: &Poller, event: Event) -> Result<()> {
        match event.key {
            TCP_STREAM_KEY => self.tcp.handle_event(poller, event, &mut self.incoming)?,
            UDP_SOCKET_KEY => self.udp.handle_event(poller, event, &mut self.incoming)?,
            _ => unreachable!(),
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
                self.udp
                    .queue_message(poller, &message.data, message.channel)?;
            }
            Guarantees::Delivery | Guarantees::DeliveryOrder => {
                self.tcp
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
