use std::{
    io,
    net::{SocketAddr, TcpStream},
};

use polling::Poller;

use crate::protocol::udp::SlidingWindow;

use super::TcpStreamHandler;

pub struct Server {
    // ---- Handler ----
    pub tcp: TcpStreamHandler,

    // ---- Data ----
    pub send_seq: u64,
    pub recv_seq_window: SlidingWindow,
}

impl Server {
    pub fn new(tcp_stream: TcpStream, poller: &Poller, key: usize) -> io::Result<Self> {
        Ok(Self {
            tcp: TcpStreamHandler::new(tcp_stream, poller, key)?,
            send_seq: 0,
            recv_seq_window: SlidingWindow::new(),
        })
    }
}
