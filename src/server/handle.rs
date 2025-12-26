pub mod clients;

use bytes::{BufMut, BytesMut};
use crossbeam::{
    channel::{Receiver, Sender},
    select,
};

use super::{
    BUFFER_SIZE, ClientId, Guarantees, MAX_PACKET_SIZE, Message, Packet, PacketType, RING_SIZE,
};
use clients::Clients;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

/// Central handler responsible for:
/// - managing connected clients,
/// - converting packets to messages and vice versa,
/// - routing data between network and application channels.
pub struct Handler {
    /// Client registry mapping socket addresses to client IDs.
    clients: Clients,

    /// Buffer for building outgoing packets.
    /// Mainly to adjust header size.
    outgoing_buffer: BytesMut,

    // ---- Channels ----
    incoming_packet: Receiver<Packet>,
    outgoing_message: Receiver<Message>,
    outgoing_packet: Sender<Packet>,
    incoming_message: Sender<Message>,
}

// ---- Constructor ----

impl Handler {
    pub fn new(
        incoming_packet: Receiver<Packet>,
        outgoing_message: Receiver<Message>,
        outgoing_packet: Sender<Packet>,
        incoming_message: Sender<Message>,
    ) -> Self {
        Self {
            clients: Clients::new(),
            outgoing_buffer: BytesMut::with_capacity(RING_SIZE),
            incoming_packet,
            outgoing_packet,
            incoming_message,
            outgoing_message,
        }
    }
}

// ---- Handling ----

impl Handler {
    /// Process a single event, blocking until either:
    /// - a packet is received from the network, or
    /// - a message is ready to be sent.
    ///
    /// # Errors
    /// Any channel disconnects.
    pub fn handle(&mut self) -> Result<()> {
        select! {
            recv(self.incoming_packet) -> packet => {
                self.handle_incoming_packet(packet?)?;
            }
            recv(self.outgoing_message) -> message => {
                self.handle_outgoing_message(message?)?;
            }
        }
        Ok(())
    }

    /// Convert an incoming `Packet` into a high-level `Message`.
    ///
    /// # Behavior
    /// - If client is unknown -> Try adding it or else drop the packet.
    /// - If packet type is unknown -> Drop the packet.
    ///
    /// # Errors
    /// One of the incoming channels disconnects.
    fn handle_incoming_packet(&mut self, Packet { mut data, addr }: Packet) -> Result<()> {
        // ---- Client handling ----

        let client = match self.clients.addr_to_id(addr) {
            Some(id) => id,
            None => match self.clients.add(addr) {
                Ok(id) => id,
                Err(_) => return Ok(()),
            },
        };

        // ---- Extract ----

        // Extract packet type (gives header format).
        // Drop packet if unknown.
        let packet_type_byte = data.split_to(1)[0];
        let Ok(packet_type) = PacketType::try_from(packet_type_byte) else {
            return Ok(());
        };

        // Extract header
        let (channel, guarantees) = match packet_type {
            PacketType::Test => {
                let header = data.split_to(1);
                let channel = header[0];
                (channel, Guarantees::None)
            }
        };

        // ---- Forward ----

        self.incoming_message.send(Message {
            data,
            client,
            channel,
            guarantees,
        })?;

        // ----

        Ok(())
    }

    /// Convert a high-level `Message` into a raw `Packet` and send it.
    ///
    /// # Behavior
    /// - Messages to unknown clients are dropped.
    fn handle_outgoing_message(
        &mut self,
        Message {
            data,
            client,
            channel,
            guarantees,
        }: Message,
    ) -> Result<()> {
        // ---- Client handling ----

        let Some(addr) = self.clients.id_to_addr(client) else {
            return Ok(());
        };

        // ---- Build ----

        // Build packet header based on guarantees
        match guarantees {
            Guarantees::None => {
                self.outgoing_buffer.put_u8(PacketType::Test.into());
                self.outgoing_buffer.put_u8(channel);
            }
        };

        // Append payload
        self.outgoing_buffer.put(data);

        // ---- Forward ----

        let data = self.outgoing_buffer.split();
        self.outgoing_packet.send(Packet { data, addr })?;

        // ---- Buffer maintenance ----

        if self.outgoing_buffer.capacity() < MAX_PACKET_SIZE {
            self.outgoing_buffer
                .reserve(BUFFER_SIZE - self.outgoing_buffer.capacity());
        }

        // ----

        Ok(())
    }
}
