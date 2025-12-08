use std::net::{Ipv4Addr, SocketAddrV4};

// The first 7 bytes of the message are needed by socket layer.
pub const ADDR_SIZE: usize = 6; // 6 for client addr

pub trait CodecAddrV4 {
    fn encode(&self) -> [u8; ADDR_SIZE];
    fn decode(bytes: [u8; ADDR_SIZE]) -> Self;
}

impl CodecAddrV4 for SocketAddrV4 {
    fn encode(&self) -> [u8; ADDR_SIZE] {
        let ip = self.ip().octets();
        let port = self.port().to_ne_bytes();
        [ip[0], ip[1], ip[2], ip[3], port[0], port[1]]
    }

    fn decode(bytes: [u8; ADDR_SIZE]) -> Self {
        let ip = Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
        let port = u16::from_ne_bytes([bytes[4], bytes[5]]);
        SocketAddrV4::new(ip, port)
    }
}
