mod tcp_handler;
mod tcp_stream_handler;
mod udp_handler;

use std::{
    io,
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    sync::mpsc::{Receiver, SendError, Sender, TryRecvError},
    time::Duration,
};

use polling::{Event, Events, Poller};
use slab::Slab;
use thiserror::Error;

use crate::protocol::udp;

use super::{
    Command, Guarantees, Incoming, IncomingMessage, Notification, Outgoing, OutgoingMessage,
};

use tcp_handler::{NextConnectionError as TcpNextConnectionError, TcpHandler};
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
const TCP_KEY: usize = usize::MAX - 1;
const UDP_KEY: usize = usize::MAX - 2;

pub struct Handler {
    pub shutdown: bool,

    // ---- Handlers ----
    tcp: TcpHandler,
    udp: UdpHandler,
    tcp_streams: Slab<TcpStreamHandler>,

    // ---- Communication ----
    incoming: Sender<Incoming>,
    outgoing: Receiver<Outgoing>,
}

impl Handler {
    pub fn create(
        tcp: TcpListener,
        udp: UdpSocket,
        poller: &Poller,
        incoming: Sender<Incoming>,
        outgoing: Receiver<Outgoing>,
    ) -> io::Result<Self> {
        Ok(Handler {
            shutdown: false,
            tcp: TcpHandler::create(tcp, poller, TCP_KEY)?,
            udp: UdpHandler::create(udp, poller, UDP_KEY)?,
            tcp_streams: Slab::new(),
            incoming,
            outgoing,
        })
    }

    pub fn destroy(&mut self, poller: &Poller) -> Result<(), DestructionError> {
        let tcp_stream_keys: Vec<_> = self.tcp_streams.iter().map(|(key, _)| key).collect();
        for key in tcp_stream_keys {
            self.remove_client(poller, key)?;
        }
        self.tcp.destroy(poller).map_err(DestructionError::Tcp)?;
        self.udp.destroy(poller).map_err(DestructionError::Udp)?;
        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DestructionError {
    #[error("failed to remove clients: {0}")]
    Clients(#[from] ClientManagementError),

    #[error("failed to destroy TCP: {0}")]
    Tcp(io::Error),

    #[error("failed to destroy UDP: {0}")]
    Udp(io::Error),
}

// ==========================================================================
// Client management
// ==========================================================================

impl Handler {
    fn is_client(&self, key: usize) -> bool {
        self.tcp_streams.contains(key)
    }

    /// client entry key and TCP sream key should always be equal (since used by the poller).
    /// Should be okay since client is always added to both slabs.
    fn add_client(
        &mut self,
        poller: &Poller,
        (tcp_stream, addr): (TcpStream, SocketAddr),
    ) -> Result<(), ClientManagementError> {
        // Add a new TCP stream handler.
        let entry = self.tcp_streams.vacant_entry();
        let tcp_stream = TcpStreamHandler::create(tcp_stream, poller, entry.key())?;
        entry.insert(tcp_stream);

        // Add a new client entry in UDP handler.
        let key = self.udp.clients.add(addr);

        // Send notification.
        self.incoming
            .send(Incoming::Internal(Notification::Connection {
                client_id: key,
                addr,
            }))?;

        Ok(())
    }

    fn remove_client(&mut self, poller: &Poller, key: usize) -> Result<(), ClientManagementError> {
        // Remove from TCP streams handlers.
        let tcp_stream = self.tcp_streams.remove(key);
        tcp_stream.destroy(poller)?;

        // Remove from UDP handler client registry.
        self.udp.clients.remove(key);

        // Send notification.
        self.incoming
            .send(Incoming::Internal(Notification::Disconnection {
                client_id: key,
            }))?;

        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientManagementError {
    #[error("failed to create/destroy TCP stream handler: {0}")]
    TcpStream(#[from] io::Error),

    #[error("failed to send notification message into channel: {0}")]
    Channel(#[from] SendError<Incoming>),
}

// ==========================================================================
// Handle outgoing
// ==========================================================================

impl Handler {
    pub fn handle_outgoing(&mut self, poller: &Poller) -> Result<(), HandleOutgoingError> {
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

    fn next_outgoing(&mut self) -> Result<Option<Outgoing>, HandleOutgoingError> {
        match self.outgoing.try_recv() {
            Ok(outgoing) => Ok(Some(outgoing)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Drop messages to unregistered clients.
    fn handle_outgoing_message(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), HandleOutgoingError> {
        if !self.is_client(message.client_id) {
            return Ok(());
        }

        match (message.guarantees, message.data.len()) {
            (Guarantees::None, len) if len <= udp::MAX_PACKET_SIZE => {
                self.udp.queue_outgoing_message(poller, message)?;
            }
            _ => {
                let tcp_stream = &mut self.tcp_streams[message.client_id];
                tcp_stream.queue_outgoing_message(poller, message)?;
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
pub enum HandleOutgoingError {
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
                TCP_KEY => self.handle_tcp_event(poller)?,
                UDP_KEY => self.handle_udp_event(poller, event)?,
                _tcp_stream_key => self.handle_tcp_stream_event(poller, event)?,
            }
        }
        Ok(())
    }

    fn handle_tcp_event(&mut self, poller: &Poller) -> Result<(), HandleEventsError> {
        while let Some(connection) = self.tcp.next_connection()? {
            self.add_client(poller, connection)?;
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
        if let Some(tcp_stream) = self.tcp_streams.get_mut(event.key) {
            match tcp_stream.handle_event(poller, event, &mut self.incoming) {
                Ok(_) => (),
                Err(
                    TcpStreamHandleEventError::ConnectionClosed
                    | TcpStreamHandleEventError::Header(_),
                ) => {
                    self.remove_client(poller, event.key)?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleEventsError {
    #[error("failed to handle TCP event: {0}")]
    Tcp(#[from] TcpNextConnectionError),

    #[error("failed to handle UDP event: {0}")]
    Udp(#[from] UdpHandleEventError),

    #[error("failed to handle TCP stream event: {0}")]
    TcpStream(#[from] TcpStreamHandleEventError),

    #[error("failed to add/remove client: {0}")]
    ClientManagement(#[from] ClientManagementError),
}

// ==========================================================================
// Next timeout & Handle expired timers
// ==========================================================================

impl Handler {
    pub fn next_timeout(&mut self) -> Option<Duration> {
        None
    }

    pub fn handle_expired_timers(&mut self) -> Result<(), HandleTimersError> {
        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleTimersError {}
