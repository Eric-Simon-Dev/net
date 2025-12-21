use bytes::{BufMut, BytesMut};
use crossbeam::{
    channel::{Receiver, Sender},
    select,
};

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

const REALLOCATION_CAPACITY: usize = 1_048_576; // = 2^20
const REALLOCATION_THRESHOLD: usize = 1024;

/// Interface between UDP packets and messages (incoming & outgoing).
///
/// Apply transport protocols.
///
/// ## Usage
///
/// Meant to be used in its own thread looping over `handle()`.
///
/// ```ignore
/// loop {
///     match handler.handle() {
///         Ok(_) => continue,
///         Err(_) => break,
///     }
/// }
/// ```
pub struct Handler {
    outgoing_buffer: BytesMut,

    //------// Channels //------//
    incoming_packet: Receiver<BytesMut>,
    outgoing_message: Receiver<(BytesMut, u8)>,
    outgoing_packet: Sender<BytesMut>,
    incoming_message: Sender<(BytesMut, u8)>,
}

impl Handler {
    pub fn new(
        incoming_packet: Receiver<BytesMut>,
        outgoing_message: Receiver<(BytesMut, u8)>,
        outgoing_packet: Sender<BytesMut>,
        incoming_message: Sender<(BytesMut, u8)>,
    ) -> Self {
        Self {
            outgoing_buffer: BytesMut::with_capacity(REALLOCATION_CAPACITY),
            incoming_packet,
            outgoing_message,
            incoming_message,
            outgoing_packet,
        }
    }

    /// Handle network protocols (blocking).
    ///
    /// Blocks <=> Wait channels for a packet to handle.
    ///
    /// `Err(_)` <=> Channel disconnection.
    pub fn handle(&mut self) -> Result<()> {
        select! {
            recv(self.incoming_packet) -> packet => {
                self.handle_incoming_packet(packet?)?;
            }
            recv(self.outgoing_message) -> message => {
                let (message, channel) = message?;
                self.handle_outgoing_message(message, channel)?;
            }
        }
        Ok(())
    }

    /// `Err(_)` <=> Channel disconnection.
    fn handle_incoming_packet(&mut self, mut packet: BytesMut) -> Result<()> {
        //------// Conversion : Packet -> Message //------//

        // Extract header then truncate it to get message.

        let channel = packet[0];
        let message = packet.split_off(1);

        //------//

        self.incoming_message.send((message, channel))?;

        Ok(())
    }

    /// `Err(_)` <=> Channel disconnection.
    fn handle_outgoing_message(&mut self, message: BytesMut, channel: u8) -> Result<()> {
        //------// Conversion : Message -> Packet //------//

        // Add header then copy message after.
        // Header = channel.
        //
        // Eventually reserve more capacity for buffer.

        self.outgoing_buffer.put_u8(channel);
        self.outgoing_buffer.put(message);
        let packet = self.outgoing_buffer.split();

        if self.outgoing_buffer.capacity() < REALLOCATION_THRESHOLD {
            self.outgoing_buffer.reserve(REALLOCATION_CAPACITY);
        }

        //------//

        self.outgoing_packet.send(packet)?;

        Ok(())
    }
}
