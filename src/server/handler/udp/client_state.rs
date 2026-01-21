use std::net::SocketAddr;

use crate::protocol::udp::SlidingWindow;

pub struct ClientState {
    pub addr: SocketAddr,
    pub seq: u64,
    pub seq_window: SlidingWindow,
}

impl ClientState {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            seq: 0,
            seq_window: SlidingWindow::new(),
        }
    }
}
