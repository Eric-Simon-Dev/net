mod tcp;
mod tcp_stream;
mod udp;

use std::{
    io,
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    sync::mpsc::{Receiver, SendError, Sender, TryRecvError},
    time::Duration,
};

use polling::{Event, Events, Poller};
use slab::Slab;
use thiserror::Error;

use crate::protocol;

use super::{
    Command, Guarantees, Incoming, IncomingMessage, Notification, Outgoing, OutgoingMessage,
};

use tcp::TcpHandler;
use tcp_stream::TcpStreamHandler;
use udp::UdpHandler;

// ---- Poller keys ----
// `usize::MAX` is reserved for internal use from the crate.
const TCP_KEY: usize = usize::MAX - 1;
const UDP_KEY: usize = usize::MAX - 2;

// ===================================================================================
// Handler
// ===================================================================================

pub struct Handler {
    pub shutdown: bool,

    // ---- Handlers ----
    tcp: TcpHandler,
    udp: UdpHandler,
    tcp_streams: Slab<TcpStreamHandler>,

    // ---- Interface ----
    incomings: Sender<Incoming>,
    outgoings: Receiver<Outgoing>,
}

impl Handler {
    pub fn create(
        tcp: TcpListener,
        udp: UdpSocket,
        poller: &Poller,
        incomings: Sender<Incoming>,
        outgoings: Receiver<Outgoing>,
    ) -> io::Result<Self> {
        Ok(Handler {
            shutdown: false,
            tcp: TcpHandler::create(tcp, poller, TCP_KEY)?,
            udp: UdpHandler::create(udp, poller, UDP_KEY)?,
            tcp_streams: Slab::new(),
            incomings,
            outgoings,
        })
    }

    pub fn destroy(&mut self, poller: &Poller) -> Result<(), DestroyError> {
        // Remove clients.
        let client_keys: Vec<usize> = self.tcp_streams.iter().map(|(key, _)| key).collect();
        for key in client_keys {
            self.remove_client(poller, key)?;
        }

        // Destroy handlers.
        self.tcp
            .destroy(poller)
            .map_err(DestroyError::DestroyTcpHandler)?;
        self.udp
            .destroy(poller)
            .map_err(DestroyError::DestroyUdpHandler)?;

        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DestroyError {
    #[error("failed to remove clients: {0}")]
    RemoveClients(#[from] ClientManagementError),

    #[error("failed to destroy TCP handler: {0}")]
    DestroyTcpHandler(io::Error),

    #[error("failed to destroy UDP handler: {0}")]
    DestroyUdpHandler(io::Error),
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
        let tcp_stream = TcpStreamHandler::create(poller, tcp_stream, entry.key())?;
        entry.insert(tcp_stream);

        // Add a new client entry in UDP handler.
        let key = self.udp.clients.add(addr);

        // Send notification.
        self.incomings
            .send(Incoming::Internal(Notification::Connection {
                client_id: key,
                addr,
            }))?;

        Ok(())
    }

    fn remove_client(&mut self, poller: &Poller, key: usize) -> Result<(), ClientManagementError> {
        // Remove from TCP streams handlers.
        let mut tcp_stream = self.tcp_streams.remove(key);
        tcp_stream.destroy(poller)?;

        // Remove from UDP handler client registry.
        self.udp.clients.remove(key);

        // Send notification.
        self.incomings
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
                TCP_KEY => self.handle_tcp_event(poller)?,
                UDP_KEY => self.handle_udp_event(poller, event)?,
                tcp_stream_key if self.tcp_streams.contains(tcp_stream_key) => {
                    self.handle_tcp_stream_event(poller, event)?
                }
                _ => (),
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleSocketEventsError {
    #[error("failed to handle TCP event: {0}")]
    HandleTcpEvent(#[from] HandleTcpEventError),

    #[error("failed to handle UDP event: {0}")]
    HandleUdpEvent(#[from] HandleUdpEventError),

    #[error("failed to handle TCP stream event: {0}")]
    HandleTcpStreamEvent(#[from] HandleTcpStreamEventError),
}

// ==========================================================================
// Handle TCP event
// ==========================================================================

impl Handler {
    fn handle_tcp_event(&mut self, poller: &Poller) -> Result<(), HandleTcpEventError> {
        // TCP event are only for readability.
        self.handle_tcp_readable(poller)
    }

    fn handle_tcp_readable(&mut self, poller: &Poller) -> Result<(), HandleTcpEventError> {
        self.tcp.read()?;
        while let Some(connection) = self.tcp.incoming() {
            self.add_client(poller, connection)?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleTcpEventError {
    #[error("failed to read: {0}")]
    Read(#[from] tcp::ReadError),

    #[error("failed to add client: {0}")]
    AddClient(#[from] ClientManagementError),
}

// ==========================================================================
// Handle TCP stream event
// ==========================================================================

impl Handler {
    fn handle_tcp_stream_event(
        &mut self,
        poller: &Poller,
        event: Event,
    ) -> Result<(), HandleTcpStreamEventError> {
        let mut connection_closed = false;
        if event.readable {
            self.handle_tcp_stream_readable(event.key, &mut connection_closed)?;
        }
        if event.writable {
            self.handle_tcp_stream_writable(poller, event.key, &mut connection_closed)?;
        }
        if connection_closed {
            self.remove_client(poller, event.key)?;
        }
        Ok(())
    }

    fn handle_tcp_stream_readable(
        &mut self,
        key: usize,
        connection_closed: &mut bool,
    ) -> Result<(), HandleTcpStreamEventError> {
        match self.tcp_streams[key].read() {
            Ok(_) => (),
            Err(tcp_stream::ReadError::ConnectionClosed) => *connection_closed = true,
            Err(error) => return Err(error.into()),
        }
        while let Some(message) = self.tcp_streams[key].incoming()? {
            self.incomings.send(Incoming::Network(message))?;
        }
        Ok(())
    }

    fn handle_tcp_stream_writable(
        &mut self,
        poller: &Poller,
        key: usize,
        connection_closed: &mut bool,
    ) -> Result<(), HandleTcpStreamEventError> {
        match self.tcp_streams[key].write(poller) {
            Ok(_) => (),
            Err(tcp_stream::WriteError::ConnectionClosed) => *connection_closed = true,
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleTcpStreamEventError {
    #[error("failed to read: {0}")]
    Read(#[from] tcp_stream::ReadError),

    #[error("failed to write: {0}")]
    Write(#[from] tcp_stream::WriteError),

    #[error("failed to get incoming message: {0}")]
    GetIncomingMessage(#[from] tcp_stream::IncomingError),

    #[error("failed to send incoming message: {0}")]
    SendIncomingMessage(#[from] SendError<Incoming>),

    #[error("failed to remove client: {0}")]
    RemoveClient(#[from] ClientManagementError),
}

// ==========================================================================
// Handle UDP event
// ==========================================================================

impl Handler {
    fn handle_udp_event(
        &mut self,
        poller: &Poller,
        event: Event,
    ) -> Result<(), HandleUdpEventError> {
        if event.readable {
            self.handle_udp_readable()?;
        }
        if event.writable {
            self.handle_udp_writable(poller)?;
        }
        Ok(())
    }

    fn handle_udp_readable(&mut self) -> Result<(), HandleUdpEventError> {
        self.udp.read()?;
        while let Some(message) = self.udp.incoming() {
            self.incomings.send(Incoming::Network(message))?;
        }
        Ok(())
    }

    fn handle_udp_writable(&mut self, poller: &Poller) -> Result<(), HandleUdpEventError> {
        self.udp.write(poller)?;
        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleUdpEventError {
    #[error("failed to read: {0}")]
    Read(#[from] udp::ReadError),

    #[error("failed to write: {0}")]
    Write(#[from] udp::WriteError),

    #[error("failed to send incoming message: {0}")]
    SendIncomingMessage(#[from] SendError<Incoming>),
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

    /// Drop messages to unregistered clients.
    fn handle_outgoing_message(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), HandleOutgoingsError> {
        if !self.is_client(message.client_id) {
            return Ok(());
        }

        match (message.guarantees, message.data.len()) {
            (Guarantees::None, len) if len <= protocol::udp::MAX_PACKET_SIZE => {
                self.udp.outgoing(poller, message)?;
            }
            _ => {
                let tcp_stream = &mut self.tcp_streams[message.client_id];
                tcp_stream.outgoing(poller, message)?;
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
    TcpStream(#[from] tcp_stream::OutgoingError),

    #[error("failed to queue message into UDP: {0}")]
    Udp(#[from] udp::OutgoingError),
}

// ==========================================================================
// Next timeout & Handle timers
// ==========================================================================

impl Handler {
    pub fn next_timeout(&mut self) -> Option<Duration> {
        None
    }

    pub fn handle_timers(&mut self) -> Result<(), HandleTimersError> {
        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HandleTimersError {}
