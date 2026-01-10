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

use super::{Guarantees, IncomingMessage, Notification, OutgoingMessage};

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
    // ---- Handlers ----
    tcp: TcpHandler,
    udp: UdpHandler,
    tcp_streams: Slab<TcpStreamHandler>,

    // ---- Communication ----
    incoming: Sender<IncomingMessage>,
    outgoing: Receiver<OutgoingMessage>,
}

impl Handler {
    pub fn new(
        tcp: TcpListener,
        udp: UdpSocket,
        poller: &Poller,
        incoming: Sender<IncomingMessage>,
        outgoing: Receiver<OutgoingMessage>,
    ) -> io::Result<Self> {
        Ok(Handler {
            tcp: TcpHandler::new(tcp, poller, TCP_KEY)?,
            udp: UdpHandler::new(udp, poller, UDP_KEY)?,
            tcp_streams: Slab::new(),
            incoming,
            outgoing,
        })
    }
}

// ==========================================================================
// Client management
// ==========================================================================

impl Handler {
    fn is_client(&self, key: usize) -> bool {
        self.tcp_streams.contains(key)
    }

    fn add_client(
        &mut self,
        poller: &Poller,
        (tcp_stream, addr): (TcpStream, SocketAddr),
    ) -> Result<(), ClientManagementError> {
        // Add a new TCP stream handler.
        let entry = self.tcp_streams.vacant_entry();
        let tcp_stream_key = entry.key();
        let tcp_stream = TcpStreamHandler::create(tcp_stream, poller, tcp_stream_key)?;
        entry.insert(tcp_stream);

        // Add a new client entry in UDP handler.
        let udp_client_key = self.udp.clients.add(addr);

        debug_assert_eq!(tcp_stream_key, udp_client_key);

        // Send notification.
        let notification = Notification::ClientConnected {
            addr,
            key: tcp_stream_key,
        };
        self.incoming.send(notification.encode_as_message())?;

        Ok(())
    }

    fn remove_client(&mut self, poller: &Poller, key: usize) -> Result<(), ClientManagementError> {
        // Remove from TCP streams handlers.
        let tcp_stream = self.tcp_streams.remove(key);
        tcp_stream.destroy(poller)?;

        // Remove from UDP handler client registry.
        self.udp.clients.remove(key);

        // Send notification.
        let notification = Notification::ClientDisconnected { key };
        self.incoming.send(notification.encode_as_message())?;

        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientManagementError {
    #[error("failed to create/destroy TCP stream handler: {0}")]
    TcpStream(#[from] io::Error),

    #[error("failed to send notification message into channel: {0}")]
    Channel(#[from] SendError<IncomingMessage>),
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

    /// Drop messages to unregistered clients.
    fn next_outgoing_message(
        &mut self,
    ) -> Result<Option<OutgoingMessage>, HandleOutgoingMessagesError> {
        loop {
            let message = match self.outgoing.try_recv() {
                Ok(message) => message,
                Err(TryRecvError::Empty) => return Ok(None),
                Err(e) => return Err(e.into()),
            };

            // Drop if unregistered.
            if !self.is_client(message.client_key) {
                continue;
            }

            return Ok(Some(message));
        }
    }

    fn handle_outgoing_message(
        &mut self,
        poller: &Poller,
        message: OutgoingMessage,
    ) -> Result<(), HandleOutgoingMessagesError> {
        match (message.guarantees, message.data.len()) {
            (Guarantees::None, len) if len <= udp::MAX_PACKET_SIZE => {
                self.udp.queue_outgoing_message(poller, message)?;
            }
            _ => {
                let tcp_stream = &mut self.tcp_streams[message.client_key];
                tcp_stream.queue_outgoing_message(poller, message)?;
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
