mod tcp_listener_handler;
mod tcp_stream_handler;
mod udp_socket_handler;

use std::{
    io,
    net::{Shutdown, TcpListener, UdpSocket},
    sync::mpsc::{Receiver, Sender, TryRecvError},
    time::Duration,
};

use polling::{Event, Poller};
use slab::Slab;
use thiserror::Error;

use super::{Guarantees, IncomingMessage, OutgoingMessage};

use tcp_listener_handler::TcpListenerHandler;
use tcp_stream_handler::{
    HandleEventError as TcpStreamHandleEventError, QueueMessageError as TcpStreamQueueMessageError,
    TcpStreamHandler,
};
use udp_socket_handler::{
    HandleEventError as UdpHandleEventError, QueueMessageError as UdpQueueMessageError,
    UdpSocketHandler,
};

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
    ) -> Result<(), HandleEventError> {
        match event.key {
            TCP_LISTENER_KEY => {
                while let Some((tcp_stream, addr)) = self.tcp.handle_event()? {
                    let entry = self.streams.vacant_entry();
                    let stream_key = entry.key();
                    let tcp = TcpStreamHandler::new(tcp_stream, poller, stream_key)?;
                    entry.insert(tcp);
                    let udp_key = self.udp.clients.add_client(addr);
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
                Err(TcpStreamHandleEventError::ConnectionClosed) => {
                    self.streams.remove(key);
                    self.udp.clients.remove_client(key);
                    Ok(())
                }
                Err(TcpStreamHandleEventError::FrameHeaderDecoding(_)) => {
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
                Err(e) => {
                    return Err(e.into());
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

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleEventError {
    #[error("UDP event handling failed: {0}")]
    UdpEventHandling(#[from] UdpHandleEventError),

    #[error("TCP stream event handling failed: {0}")]
    TcpStreamEventHandling(#[from] TcpStreamHandleEventError),

    /// Can be creation or destruction of TCP stream handlers.
    #[error("I/O error while handling event: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleOutgoingMessagesError {
    #[error("outgoing message receiving failed: {0}")]
    OutgoingMessageReceiving(#[from] TryRecvError),

    #[error("TCP stream message queueing failed: {0}")]
    TcpStreamMessageQueueing(#[from] TcpStreamQueueMessageError),

    #[error("UDP message queueing failed: {0}")]
    UdpMessageQueueing(#[from] UdpQueueMessageError),
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleTimersError {}
