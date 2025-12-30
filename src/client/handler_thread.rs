use std::thread;

use bytes::{BufMut, BytesMut};
use crossbeam::{
    channel::{Receiver, Sender},
    select,
};

use super::{BUFFER_SIZE, Guarantees, MAX_PACKET_SIZE, Message, Packet, PacketType, RING_SIZE};

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub fn spawn(
    incoming_packets: Receiver<Packet>,
    outgoing_messages: Receiver<Message>,
    outgoing_packets: Sender<Packet>,
    incoming_messages: Sender<Message>,
) {
    thread::spawn(move || {
        let mut handler = Handler::new(
            incoming_packets,
            outgoing_messages,
            outgoing_packets,
            incoming_messages,
        );
        while handler.handle().is_ok() {
            continue;
        }
    });
}

/// Central handler responsible for:
/// - converting packets to messages and vice versa,
/// - routing data between network and application channels.
struct Handler {
    /// Buffer for building outgoing packets.
    /// Mainly to adjust header size.
    outgoing_buffer: BytesMut,

    // ---- Channels ----
    incoming_packets: Receiver<Packet>,
    outgoing_messages: Receiver<Message>,
    outgoing_packets: Sender<Packet>,
    incoming_messages: Sender<Message>,
}

// ---- Constructor ----

impl Handler {
    fn new(
        incoming_packets: Receiver<Packet>,
        outgoing_messages: Receiver<Message>,
        outgoing_packets: Sender<Packet>,
        incoming_messages: Sender<Message>,
    ) -> Self {
        Self {
            outgoing_buffer: BytesMut::with_capacity(RING_SIZE),
            incoming_packets,
            outgoing_packets,
            incoming_messages,
            outgoing_messages,
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
    fn handle(&mut self) -> Result<()> {
        select! {
            recv(self.incoming_packets) -> packet => {
                self.handle_incoming_packet(packet?)?;
            }
            recv(self.outgoing_messages) -> message => {
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
    /// `incoming_messages` disconnects.
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
        self.incoming_messages.send(Message {
            data: pkt.data,
            channel,
            guarantees,
        })?;

        Ok(())
    }

    /// Convert a high-level `Message` into a raw `Packet` and send it.
    ///
    /// # Errors
    /// `outgoing_packets` disconnects.
    fn handle_outgoing_message(&mut self, msg: Message) -> Result<()> {
        // Build packet. This will consume buffer memory.
        let packet = self.build_packet(msg);

        // Send packet.
        self.outgoing_packets.send(packet)?;

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
