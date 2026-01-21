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

use super::*;

use tcp::TcpHandler;
use tcp_stream::TcpStreamHandler;
use udp::UdpHandler;

// ---- Poller keys ----
// `usize::MAX` is reserved for internal use by polling crate.
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

    pub fn destroy(&mut self, poller: &Poller) -> io::Result<()> {
        // Remove clients (destroy TCP stream handlers).
        for key in self.client_keys() {
            self.remove_client(poller, key)?;
        }

        // Destroy TCP & UDP handlers.
        self.tcp.destroy(poller)?;
        self.udp.destroy(poller)?;

        Ok(())
    }
}

// ==========================================================================
// Client management
// ==========================================================================

impl Handler {
    fn client_keys(&self) -> Vec<usize> {
        self.tcp_streams.iter().map(|(key, _)| key).collect()
    }

    fn contains_client(&self, key: usize) -> bool {
        self.tcp_streams.contains(key)
    }

    fn add_client(
        &mut self,
        poller: &Poller,
        tcp_stream: TcpStream,
        addr: SocketAddr,
    ) -> io::Result<usize> {
        // Add its TCP stream.
        let tcp_stream_entry = self.tcp_streams.vacant_entry();
        let tcp_stream_key = tcp_stream_entry.key();
        tcp_stream_entry.insert(TcpStreamHandler::create(
            poller,
            tcp_stream,
            tcp_stream_key,
        )?);

        // Add its state into UDP.
        let udp_client_state_key = self.udp.add_client_state(addr);

        debug_assert_eq!(tcp_stream_key, udp_client_state_key);

        Ok(udp_client_state_key)
    }

    fn remove_client(&mut self, poller: &Poller, key: usize) -> io::Result<()> {
        // Remove its TCP stream.
        self.tcp_streams.remove(key).destroy(poller)?;

        // Remove its state from UDP.
        self.udp.remove_client_state(key);

        Ok(())
    }
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
        while let Some((tcp_stream, addr)) = self.tcp.next_incoming_connection() {
            let client_id = self.add_client(poller, tcp_stream, addr)?;
            self.incoming
                .send(Incoming::Internal(Notification::Connection {
                    client_id,
                    addr,
                }))?;
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
    AddClient(#[from] io::Error),

    #[error("failed to send connection notification: {0}")]
    SendConnectionNotification(#[from] SendError<Incoming>),
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
            self.incoming
                .send(Incoming::Internal(Notification::Disconnection {
                    client_id: event.key,
                }))
                .map_err(HandleTcpStreamEventError::SendDisconnectionNotification)?;
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
        while let Some(message) = self.tcp_streams[key].next_incoming_message()? {
            self.incoming
                .send(Incoming::Network(message))
                .map_err(HandleTcpStreamEventError::SendIncomingMessage)?;
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

    #[error("failed to get next incoming message: {0}")]
    NextIncomingMessage(#[from] tcp_stream::NextIncomingMessageError),

    #[error("failed to send incoming message: {0}")]
    SendIncomingMessage(SendError<Incoming>),

    #[error("failed to remove client: {0}")]
    RemoveClient(#[from] io::Error),

    #[error("failed to send disconnection notification: {0}")]
    SendDisconnectionNotification(SendError<Incoming>),
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
        while let Some(message) = self.udp.next_incoming_message() {
            self.incoming.send(Incoming::Network(message))?;
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
            Err(TryRecvError::Disconnected) if self.shutdown => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_outgoing_message(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), HandleOutgoingError> {
        // Drop if client unregistered.
        if !self.contains_client(message.client_id) {
            return Ok(());
        }

        match (message.guarantees, message.data.len()) {
            // If no guarantees and small enough => UDP ...
            (Guarantees::None, len) if len <= protocol::udp::MAX_PACKET_SIZE => {
                self.udp.enqueue_outgoing_message(poller, message)?;
            }
            // ... else => TCP.
            _ => {
                self.tcp_streams[message.client_id].enqueue_outgoing_message(poller, message)?;
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
    #[error("failed to receive outgoing (message or command): {0}")]
    RecvOutgoing(#[from] TryRecvError),

    #[error("failed to enqueue outgoing message into TCP stream: {0}")]
    TcpStreamEnqueue(#[from] tcp_stream::EnqueueOutgoingMessageError),

    #[error("failed to enqueue outgoing message into UDP: {0}")]
    UdpEnqueue(#[from] udp::EnqueueOutgoingMessageError),
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
