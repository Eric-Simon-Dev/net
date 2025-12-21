use bytes::BytesMut;
use crossbeam::{
    channel::{Receiver, Sender},
    select,
};

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

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
    //------// Channels //------//
    incoming_packet: Receiver<BytesMut>,
    outgoing_message: Receiver<BytesMut>,
    incoming_message: Sender<BytesMut>,
    outgoing_packet: Sender<BytesMut>,
}

impl Handler {
    pub fn new(
        incoming_packet: Receiver<BytesMut>,
        outgoing_message: Receiver<BytesMut>,
        incoming_message: Sender<BytesMut>,
        outgoing_packet: Sender<BytesMut>,
    ) -> Self {
        Self {
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
                self.handle_outgoing_message(message?)?;
            }
        }
        Ok(())
    }

    /// `Err(_)` <=> Channel disconnection.
    fn handle_incoming_packet(&mut self, packet: BytesMut) -> Result<()> {
        //------// Conversion : Packet -> Message //------//

        let message = packet;

        //------//

        self.incoming_message.send(message)?;

        Ok(())
    }

    /// `Err(_)` <=> Channel disconnection.
    fn handle_outgoing_message(&mut self, message: BytesMut) -> Result<()> {
        //------// Conversion : Message -> Packet //------//

        let packet = message;

        //------//

        self.outgoing_packet.send(packet)?;

        Ok(())
    }
}
