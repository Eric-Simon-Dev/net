mod handle;
mod recv;
mod send;

use std::{
    io,
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
    thread,
};

use bytes::BytesMut;
use crossbeam::channel::{Receiver, Sender, bounded};

use super::{MAX_PACKET_SIZE, MAX_PAYLOAD_SIZE, PacketType};
use handle::Handler;
use recv::PacketReceiver;
use send::PacketSender;

const MAX_PACKET_IN_FLIGHT: usize = 1024;
const BUFFER_SIZE: usize = MAX_PACKET_SIZE * MAX_PACKET_IN_FLIGHT;
const RING_SIZE: usize = BUFFER_SIZE * 16;

////////////////////////////////////////////////////////////////////////////////
// Server
////////////////////////////////////////////////////////////////////////////////

pub struct Server {
    socket: UdpSocket,
}

//------// Constructor //------//

impl Server {
    /// Create a server bound to `addr`.
    pub fn new(addr: impl ToSocketAddrs) -> io::Result<Server> {
        Ok(Self {
            socket: UdpSocket::bind(addr)?,
        })
    }
}

//------// Methods //------//

impl Server {
    /// Start pumping messages.
    ///
    /// In case of an endpoint returning `Err(Disconnected)`,
    /// it means network stopped working.
    ///
    /// It is then safe to call again this method but old client ids became useless.
    pub fn listen(&mut self) -> io::Result<(Sender<Message>, Receiver<Message>)> {
        //------// Channels //------//

        // Create channels for inter-threads communication.
        // See module documentation for naming conventions.

        let cap = MAX_PACKET_IN_FLIGHT;
        let incoming_packet = bounded(cap);
        let outgoing_packet = bounded(cap);
        let incoming_message = bounded(cap);
        let outgoing_message = bounded(cap);

        //------// Threads //------//

        // Spawn threads to pump packets and messages.
        // See module documentation for explanations.

        self.spawn_receiver(incoming_packet.0)?;
        self.spawn_sender(outgoing_packet.1)?;
        self.spawn_handler(
            incoming_packet.1,
            outgoing_message.1,
            outgoing_packet.0,
            incoming_message.0,
        );

        //------//

        Ok((outgoing_message.0, incoming_message.1))
    }

    /// `Err(_)` <=> Fail to clone `self.socket`.
    fn spawn_receiver(&mut self, incoming_packet: Sender<Packet>) -> io::Result<()> {
        let mut receiver = PacketReceiver::new(self.socket.try_clone()?, incoming_packet);
        thread::spawn(move || {
            loop {
                match receiver.recv() {
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });
        Ok(())
    }

    /// `Err(_)` <=> Fail to clone `self.socket`.
    fn spawn_sender(&mut self, outgoing_packet: Receiver<Packet>) -> io::Result<()> {
        let mut sender = PacketSender::new(self.socket.try_clone()?, outgoing_packet);
        thread::spawn(move || {
            loop {
                match sender.send() {
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });
        Ok(())
    }

    fn spawn_handler(
        &mut self,
        incoming_packet: Receiver<Packet>,
        outgoing_message: Receiver<Message>,
        outgoing_packet: Sender<Packet>,
        incoming_message: Sender<Message>,
    ) {
        let mut handler = Handler::new(
            incoming_packet,
            outgoing_message,
            outgoing_packet,
            incoming_message,
        );
        thread::spawn(move || {
            loop {
                match handler.handle() {
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });
    }
}

////////////////////////////////////////////////////////////////////////////////
// Small structures
////////////////////////////////////////////////////////////////////////////////

//------// Message (public) //------//

#[derive(Debug, Clone)]
pub struct Message {
    pub data: BytesMut,
    pub client: ClientId,
    pub channel: u8,
    pub guarantees: Guarantees,
}

impl Message {
    pub const MAX_DATA_SIZE: usize = MAX_PAYLOAD_SIZE;
}

/// Unique per client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Guarantees {
    None,
}

//------// Packet (private) //------//

#[derive(Debug)]
struct Packet {
    data: BytesMut,
    addr: SocketAddr,
}
