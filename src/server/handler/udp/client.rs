use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::{Index, IndexMut},
};

use slab::Slab;

use crate::protocol::udp::SlidingWindow;

pub struct Client {
    pub addr: SocketAddr,
    pub send_seq: u64,
    pub recv_seq_window: SlidingWindow,
}

pub struct ClientRegistry {
    clients: Slab<Client>,
    addr_to_key: HashMap<SocketAddr, usize>,
}

impl ClientRegistry {
    pub fn new() -> Self {
        Self {
            clients: Slab::new(),
            addr_to_key: HashMap::new(),
        }
    }

    pub fn add(&mut self, addr: SocketAddr) -> usize {
        // Create client.
        let client = Client {
            addr,
            send_seq: 0,
            recv_seq_window: SlidingWindow::new(),
        };

        // Update registry.
        let key = self.clients.insert(client);
        self.addr_to_key.insert(addr, key);

        key
    }

    pub fn remove(&mut self, key: usize) -> Client {
        // Update registry.
        let client = self.clients.remove(key);
        self.addr_to_key.remove(&client.addr);

        client
    }

    pub fn get_key(&self, addr: &SocketAddr) -> Option<usize> {
        self.addr_to_key.get(addr).copied()
    }
}

impl Index<usize> for ClientRegistry {
    type Output = Client;

    fn index(&self, index: usize) -> &Self::Output {
        &self.clients[index]
    }
}

impl IndexMut<usize> for ClientRegistry {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.clients.get_mut(index).expect("invalid client key")
    }
}
