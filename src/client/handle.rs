use bytes::{BufMut, BytesMut};
use crossbeam::{
    channel::{Receiver, Sender},
    select,
};

use super::{BUFFER_SIZE, Guarantees, MAX_PACKET_SIZE, Message, Packet, PacketType, RING_SIZE};

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub struct Handler {
    outgoing_buffer: BytesMut,

    //------// Channels //------//
    incoming_packet: Receiver<Packet>,
    outgoing_message: Receiver<Message>,
    outgoing_packet: Sender<Packet>,
    incoming_message: Sender<Message>,
}

//------// Constructor //------//

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

//------// Handling //------//

impl Handler {
    /// Block <=> Wait channels.
    ///
    /// `Err(_)` <=> Channel disconnect.
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

    /// `Err(_)` <=> Channel disconnect.
    ///
    /// Client unknown => Try adding client, else drop packet.
    /// Packet type unknown => Drop packet.
    fn handle_incoming_packet(&mut self, Packet { mut data }: Packet) -> Result<()> {
        //------// Conversion : Packet -> Message //------//

        // Extract packet type (to know header format).
        // Drop packet if unknown.
        //
        // Extract header, react to it and return (channel, guarantees).

        let packet_type_byte = data.split_to(1)[0];
        let Ok(packet_type) = PacketType::try_from(packet_type_byte) else {
            return Ok(());
        };

        let (channel, guarantees) = match packet_type {
            PacketType::Test => {
                let header = data.split_to(1);
                let channel = header[0];
                (channel, Guarantees::None)
            }
        };

        //------//

        self.incoming_message.send(Message {
            data,
            channel,
            guarantees,
        })?;

        Ok(())
    }

    /// `Err(_)` <=> Channel disconnect.
    ///
    /// Client unknown => Drop message.
    fn handle_outgoing_message(
        &mut self,
        Message {
            data,
            channel,
            guarantees,
        }: Message,
    ) -> Result<()> {
        //------// Conversion : Message -> Packet //------//

        // Append header based on guarantees.
        //
        // Append payload and send.

        match guarantees {
            Guarantees::None => {
                self.outgoing_buffer.put_u8(PacketType::Test.into());
                self.outgoing_buffer.put_u8(channel.into());
            }
        };

        self.outgoing_buffer.put(data);
        let data = self.outgoing_buffer.split().freeze();
        self.outgoing_packet.send(Packet { data })?;

        //------// Resize buffer //------//

        if self.outgoing_buffer.capacity() < MAX_PACKET_SIZE {
            self.outgoing_buffer
                .reserve(BUFFER_SIZE - self.outgoing_buffer.capacity());
        }

        //------//

        Ok(())
    }
}
