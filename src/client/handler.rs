mod tcp_stream_handler;
mod udp_handler;

use std::{
    io,
    net::{TcpStream, UdpSocket},
    sync::mpsc::{Receiver, Sender, TryRecvError},
    time::Duration,
};

use polling::{Event, Events, Poller};
use thiserror::Error;

use super::{Guarantees, IncomingMessage, OutgoingMessage};

use tcp_stream_handler::{
    HandleEventError as TcpStreamHandleEventError,
    QueueOutgoingMessageError as TcpStreamQueueOutgoingMessageError, TcpStreamHandler,
};
use udp_handler::{
    HandleEventError as UdpHandleEventError,
    QueueOutgoingMessageError as UdpQueueOutgoingMessageError, UdpHandler,
};

// ---- Poller keys ----
// `usize::MAX` is reserved for internal use from the crate.
const TCP_STREAM_KEY: usize = usize::MAX - 1;
const UDP_KEY: usize = usize::MAX - 2;

pub struct Handler {
    // ---- Handlers ----
    tcp_stream: TcpStreamHandler,
    udp: UdpHandler,

    // ---- Communication ----
    incoming: Sender<IncomingMessage>,
    outgoing: Receiver<OutgoingMessage>,
}

impl Handler {
    pub fn new(
        tcp_stream: TcpStream,
        udp: UdpSocket,
        poller: &Poller,
        incoming: Sender<IncomingMessage>,
        outgoing: Receiver<OutgoingMessage>,
    ) -> io::Result<Self> {
        Ok(Handler {
            tcp_stream: TcpStreamHandler::new(tcp_stream, poller, TCP_STREAM_KEY)?,
            udp: UdpHandler::new(udp, poller, UDP_KEY)?,
            incoming,
            outgoing,
        })
    }
}

// ==========================================================================
// Handle available outgoing messages
// ==========================================================================

impl Handler {
    pub fn handle_available_outgoing_messages(
        &mut self,
        poller: &Poller,
    ) -> Result<(), HandleOutgoingMessagesError> {
        while let Some(message) = self.next_outgoing_message()? {
            self.handle_outgoing_message(poller, message)?;
        }
        Ok(())
    }

    fn next_outgoing_message(
        &mut self,
    ) -> Result<Option<OutgoingMessage>, HandleOutgoingMessagesError> {
        match self.outgoing.try_recv() {
            Ok(message) => Ok(Some(message)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_outgoing_message(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), HandleOutgoingMessagesError> {
        match message.guarantees {
            Guarantees::None => {
                self.udp.queue_outgoing_message(poller, message)?;
            }
            Guarantees::Delivery | Guarantees::DeliveryOrder => {
                self.tcp_stream.queue_outgoing_message(poller, message)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleOutgoingMessagesError {
    #[error("failed to receive message from channel: {0}")]
    Channel(#[from] TryRecvError),

    #[error("failed to queue message into TCP stream: {0}")]
    TcpStream(#[from] TcpStreamQueueOutgoingMessageError),

    #[error("failed to queue message into UDP: {0}")]
    Udp(#[from] UdpQueueOutgoingMessageError),
}

// ==========================================================================
// Handle events
// ==========================================================================

impl Handler {
    pub fn handle_events(
        &mut self,
        poller: &Poller,
        events: &Events,
    ) -> Result<(), HandleEventsError> {
        for event in events.iter() {
            match event.key {
                TCP_STREAM_KEY => self.handle_tcp_stream_event(poller, event)?,
                UDP_KEY => self.handle_udp_event(poller, event)?,
                _ => unreachable!("should not register other sockets"),
            }
        }
        Ok(())
    }

    fn handle_udp_event(&mut self, poller: &Poller, event: Event) -> Result<(), HandleEventsError> {
        self.udp.handle_event(poller, event, &mut self.incoming)?;
        Ok(())
    }

    fn handle_tcp_stream_event(
        &mut self,
        poller: &Poller,
        event: Event,
    ) -> Result<(), HandleEventsError> {
        self.tcp_stream.handle_event(poller, event, &mut self.incoming)?;
        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleEventsError {
    #[error("failed to handle UDP event: {0}")]
    Udp(#[from] UdpHandleEventError),

    #[error("failed to handle TCP stream event: {0}")]
    TcpStream(#[from] TcpStreamHandleEventError),
}

// ==========================================================================
// Handle expired timers
// ==========================================================================

impl Handler {
    pub fn handle_expired_timers(&mut self) -> Result<(), HandleTimersError> {
        Ok(())
    }

    pub fn next_timeout(&mut self) -> Option<Duration> {
        None
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleTimersError {}
