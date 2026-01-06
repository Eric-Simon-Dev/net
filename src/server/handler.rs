mod tcp_listener_handler;
mod tcp_stream_handler;
mod udp_socket_handler;

use std::{
    fmt, io,
    net::{Shutdown, TcpListener, UdpSocket},
    sync::mpsc::{Receiver, Sender, TryRecvError},
    time::Duration,
};

use polling::{Event, Poller};
use slab::Slab;

use super::{Guarantees, IncomingMessage, OutgoingMessage};

use tcp_listener_handler::TcpListenerHandler;
use tcp_stream_handler::{
    HandleEventError as TcpStreamHandleError, QueueMessageError as TcpStreamQueueMessageError,
    TcpStreamHandler,
};
use udp_socket_handler::{HandleEventError as UdpHandleError, UdpSocketHandler};

// ---- Poller keys ----
// `usize::MAX` is reserved for internal use from the crate.
const TCP_LISTENER_KEY: usize = usize::MAX - 1;
const UDP_SOCKET_KEY: usize = usize::MAX - 2;

pub struct Handler {
    // ---- Handlers ----
    tcp: TcpListenerHandler,
    udp: UdpSocketHandler,
    streams: Slab<TcpStreamHandler>,

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
            incoming,
            outgoing,
        })
    }

    pub fn handle_socket_event(
        &mut self,
        poller: &Poller,
        event: Event,
    ) -> Result<(), HandleSocketEventError> {
        match event.key {
            TCP_LISTENER_KEY => {
                while let Some((tcp_stream, addr)) = self.tcp.handle_event()? {
                    let entry = self.streams.vacant_entry();
                    let stream_key = entry.key();
                    let tcp = TcpStreamHandler::new(tcp_stream, poller, stream_key)?;
                    entry.insert(tcp);
                    let udp_key = self.udp.clients.add_client(addr)?;
                    assert_eq!(stream_key, udp_key);
                }
                Ok(())
            }
            UDP_SOCKET_KEY => {
                self.udp.handle_event(poller, event, &mut self.incoming)?;
                Ok(())
            }
            key => match self.streams[key].handle_event(poller, event, &mut self.incoming) {
                Ok(_) => Ok(()),
                Err(TcpStreamHandleError::ConnectionClosed) => {
                    self.streams.remove(key);
                    self.udp.clients.remove_client(key);
                    Ok(())
                }
                Err(TcpStreamHandleError::InvalidHeader(_)) => {
                    let stream = self.streams.remove(key);
                    stream.socket.shutdown(Shutdown::Both)?;
                    self.udp.clients.remove_client(key);
                    Ok(())
                }
                Err(e) => Err(e.into()),
            },
        }
    }

    pub fn check_outgoing_messages(
        &mut self,
        poller: &Poller,
    ) -> Result<(), HandleOutgoingMessagesError> {
        loop {
            match self.outgoing.try_recv() {
                Ok(message) => self.handle_outgoing_message(poller, message)?,
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => {
                    return Err(HandleOutgoingMessagesError::ChannelDisconnected);
                }
            }
        }
    }

    fn handle_outgoing_message(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), HandleOutgoingMessagesError> {
        match message.guarantees {
            Guarantees::None => {
                self.udp.queue_message(
                    poller,
                    &message.data,
                    message.channel,
                    message.client_key,
                )?;
            }
            Guarantees::Delivery | Guarantees::DeliveryOrder => {
                self.streams[message.client_key].queue_message(
                    poller,
                    &message.data,
                    message.channel,
                )?;
            }
        }

        Ok(())
    }

    pub fn check_timers(&mut self) -> Result<(), HandleTimersError> {
        Ok(())
    }

    pub fn next_timeout(&mut self) -> Option<Duration> {
        None
    }
}

// ---- Errors ----

#[derive(Debug)]
#[non_exhaustive]
pub enum HandleSocketEventError {
    UdpHandle(UdpHandleError),
    TcpStreamHandle(TcpStreamHandleError),
    Io(std::io::Error),
}

impl From<std::io::Error> for HandleSocketEventError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<UdpHandleError> for HandleSocketEventError {
    fn from(e: UdpHandleError) -> Self {
        Self::UdpHandle(e)
    }
}

impl From<TcpStreamHandleError> for HandleSocketEventError {
    fn from(value: TcpStreamHandleError) -> Self {
        Self::TcpStreamHandle(value)
    }
}

impl fmt::Display for HandleSocketEventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TcpStreamHandle(e) => {
                write!(f, "error while handling TCP stream socket event: {e}")
            }
            Self::UdpHandle(e) => write!(f, "error while handling UDP socket event: {e}"),
            Self::Io(e) => write!(f, "I/O error while handling socket event: {e}"),
        }
    }
}

impl std::error::Error for HandleSocketEventError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TcpStreamHandle(e) => Some(e),
            Self::UdpHandle(e) => Some(e),
            Self::Io(e) => Some(e),
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum HandleOutgoingMessagesError {
    ChannelDisconnected,
    TcpQueueMessage(TcpStreamQueueMessageError),
    Io(std::io::Error),
}

impl From<std::io::Error> for HandleOutgoingMessagesError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<TcpStreamQueueMessageError> for HandleOutgoingMessagesError {
    fn from(e: TcpStreamQueueMessageError) -> Self {
        Self::TcpQueueMessage(e)
    }
}

impl fmt::Display for HandleOutgoingMessagesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TcpQueueMessage(e) => write!(f, "error while queuing tcp message: {e}"),
            Self::ChannelDisconnected => write!(f, "outgoing messages channel disconnected"),
            Self::Io(e) => write!(f, "I/O error while handling socket event: {e}"),
        }
    }
}

impl std::error::Error for HandleOutgoingMessagesError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TcpQueueMessage(e) => Some(e),
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum HandleTimersError {}

impl fmt::Display for HandleTimersError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "timer error")
    }
}

impl std::error::Error for HandleTimersError {}
