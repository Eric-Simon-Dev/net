mod tcp_stream;
mod udp;

use std::{
    io,
    net::{TcpStream, UdpSocket},
    sync::mpsc::{Receiver, Sender, TryRecvError},
    time::Duration,
};

use polling::{Event, Events, Poller};
use thiserror::Error;

use crate::protocol;

use super::{Command, Guarantees, Incoming, IncomingMessage, Outgoing, OutgoingMessage};

use tcp_stream::TcpStreamHandler;
use udp::UdpHandler;

// ---- Poller keys ----
// `usize::MAX` is reserved for internal use from the crate.
const TCP_STREAM_KEY: usize = usize::MAX - 1;
const UDP_KEY: usize = usize::MAX - 2;

// ===================================================================================
// Handler
// ===================================================================================

pub struct Handler {
    pub shutdown: bool,

    // ---- Handlers ----
    tcp_stream: TcpStreamHandler,
    udp: UdpHandler,

    // ---- Interface ----
    incomings: Sender<Incoming>,
    outgoings: Receiver<Outgoing>,
}

impl Handler {
    pub fn create(
        tcp_stream: TcpStream,
        udp: UdpSocket,
        poller: &Poller,
        incomings: Sender<Incoming>,
        outgoings: Receiver<Outgoing>,
    ) -> io::Result<Self> {
        Ok(Handler {
            shutdown: false,
            tcp_stream: TcpStreamHandler::create(tcp_stream, poller, TCP_STREAM_KEY)?,
            udp: UdpHandler::create(udp, poller, UDP_KEY)?,
            incomings,
            outgoings,
        })
    }

    /// Flush unsent messages before destruction.
    pub fn destroy(&mut self, poller: &Poller) -> Result<(), DestroyError> {
        // Destroy handlers.
        self.tcp_stream
            .destroy(poller)
            .map_err(DestroyError::DestroyTcpStreamHandler)?;
        self.udp
            .destroy(poller)
            .map_err(DestroyError::DestroyUdpHandler)?;

        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DestroyError {
    #[error("failed to destroy TCP stream handler: {0}")]
    DestroyTcpStreamHandler(io::Error),

    #[error("failed to destroy UDP handler: {0}")]
    DestroyUdpHandler(io::Error),
}

// ==========================================================================
// Handle outgoings
// ==========================================================================

impl Handler {
    pub fn handle_outgoings(&mut self, poller: &Poller) -> Result<(), HandleOutgoingsError> {
        while let Some(outgoing) = self.next_outgoing()? {
            match outgoing {
                Outgoing::Network(message) => {
                    self.handle_outgoing_message(poller, message)?;
                }
                Outgoing::Internal(command) => {
                    self.handle_command(command);
                }
            }
        }
        Ok(())
    }

    fn next_outgoing(&mut self) -> Result<Option<Outgoing>, HandleOutgoingsError> {
        match self.outgoings.try_recv() {
            Ok(outgoing) => Ok(Some(outgoing)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) if self.shutdown => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_outgoing_message(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), HandleOutgoingsError> {
        match (message.guarantees, message.data.len()) {
            (Guarantees::None, len) if len <= protocol::udp::MAX_PACKET_SIZE => {
                self.udp.queue_outgoing_message(poller, message)?;
            }
            _ => {
                self.tcp_stream.queue_outgoing_message(poller, message)?;
            }
        }
        Ok(())
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::Shutdown => self.shutdown = true,
        }
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleOutgoingsError {
    #[error("failed to receive message from channel: {0}")]
    Channel(#[from] TryRecvError),

    #[error("failed to queue message into TCP stream: {0}")]
    TcpStream(#[from] tcp_stream::QueueOutgoingMessageError),

    #[error("failed to queue message into UDP: {0}")]
    Udp(#[from] udp::QueueOutgoingMessageError),
}

// ==========================================================================
// Handle socket events
// ==========================================================================

impl Handler {
    pub fn handle_socket_events(
        &mut self,
        poller: &Poller,
        events: &Events,
    ) -> Result<(), HandleSocketEventsError> {
        for event in events.iter() {
            match event.key {
                TCP_STREAM_KEY => self.handle_tcp_stream_event(poller, event)?,
                UDP_KEY => self.handle_udp_event(poller, event)?,
                _ => unreachable!("should not register other sockets"),
            }
        }
        Ok(())
    }

    fn handle_udp_event(
        &mut self,
        poller: &Poller,
        event: Event,
    ) -> Result<(), HandleSocketEventsError> {
        self.udp.handle_event(poller, event, &mut self.incomings)?;
        Ok(())
    }

    fn handle_tcp_stream_event(
        &mut self,
        poller: &Poller,
        event: Event,
    ) -> Result<(), HandleSocketEventsError> {
        self.tcp_stream
            .handle_socket_event(poller, event, &mut self.incomings)?;
        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleSocketEventsError {
    #[error("failed to handle UDP event: {0}")]
    Udp(#[from] udp::HandleEventError),

    #[error("failed to handle TCP stream event: {0}")]
    TcpStream(#[from] tcp_stream::HandleEventError),
}

// ==========================================================================
// Next timeout & Handle timers
// ==========================================================================

impl Handler {
    pub fn handle_timers(&mut self) -> Result<(), HandleTimersError> {
        Ok(())
    }

    pub fn next_timeout(&mut self) -> Option<Duration> {
        None
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleTimersError {}
