//! Items shared by client and server.
//! 
//! Should explain all procedures that server and client agree on.

pub mod tcp;
pub mod udp;

pub const MAX_PAYLOAD_LENGTH: usize = tcp::MAX_PAYLOAD_LENGTH as usize;
