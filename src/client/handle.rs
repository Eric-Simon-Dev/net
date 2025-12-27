use bytes::{BufMut, BytesMut};
use crossbeam::{
    channel::{Receiver, Sender},
    select,
};

use super::{BUFFER_SIZE, Guarantees, MAX_PACKET_SIZE, Message, Packet, PacketType, RING_SIZE};

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

/// Central handler responsible for:
/// - converting packets to messages and vice versa,
/// - routing data between network and application channels.
pub struct Handler {
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
    /// Any channel disconnect.
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
    /// If packet type is unknown -> Drop the packet.
    ///
    /// # Errors
    /// `incoming_message` disconnects.
    fn handle_incoming_packet(&mut self, mut pkt: Packet) -> Result<()> {
        // Extract packet type (gives header format). Drop packet if unknown.
        let packet_type_byte = pkt.data.split_to(1)[0];
        let Ok(packet_type) = PacketType::try_from(packet_type_byte) else {
            return Ok(());
        };

        // Extract header.
        let (channel, guarantees) = match packet_type {
            PacketType::Unreliable => {
                let header = pkt.data.split_to(1);
                let channel = header[0];
                (channel, Guarantees::None)
            }
        };

        // Forward.
        self.incoming_message.send(Message {
            data: pkt.data,
            channel,
            guarantees,
        })?;

        Ok(())
    }

    /// Convert a high-level `Message` into a raw `Packet` and send it.
    ///
    /// # Errors
    /// `outgoing_packet` disconnects.
    fn handle_outgoing_message(&mut self, msg: Message) -> Result<()> {
        // Build packet. This will consume buffer memory.
        let packet = self.build_packet(msg);

        // Send packet.
        self.outgoing_packet.send(packet)?;

        // Maintain buffer for next packet.
        if self.outgoing_buffer.capacity() < MAX_PACKET_SIZE {
            self.outgoing_buffer
                .reserve(BUFFER_SIZE - self.outgoing_buffer.capacity());
        }

        Ok(())
    }

    fn build_packet(&mut self, msg: Message) -> Packet {
        // Append header.
        match msg.guarantees {
            Guarantees::None => {
                self.outgoing_buffer.put_u8(PacketType::Unreliable.into());
                self.outgoing_buffer.put_u8(msg.channel);
            }
        };

        // Append payload.
        self.outgoing_buffer.put(msg.data);

        Packet {
            data: self.outgoing_buffer.split(),
        }
    }
}
