use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::{Index, IndexMut},
};

use slab::Slab;

use crate::protocol::udp::SlidingWindow;

pub struct ClientState {
    pub addr: SocketAddr,
    pub send_seq: u64,
    pub recv_seq_window: SlidingWindow,
}

impl ClientState {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            send_seq: 0,
            recv_seq_window: SlidingWindow::new(),
        }
    }
}

// =================================================================
// Registry
// =================================================================

pub struct ClientStateRegistry {
    clients: Slab<ClientState>,
    addr_to_key: HashMap<SocketAddr, usize>,
}

impl ClientStateRegistry {
    pub fn new() -> Self {
        Self {
            clients: Slab::new(),
            addr_to_key: HashMap::new(),
        }
    }

    pub fn add(&mut self, addr: SocketAddr) -> usize {
        // Create.
        let client = ClientState::new(addr);

        // Register.
        let key = self.clients.insert(client);
        self.addr_to_key.insert(addr, key);

        key
    }

    pub fn remove(&mut self, key: usize) -> ClientState {
        // Remove.
        let client = self.clients.remove(key);
        self.addr_to_key.remove(&client.addr);

        client
    }

    pub fn get_key(&self, addr: &SocketAddr) -> Option<usize> {
        self.addr_to_key.get(addr).copied()
    }
}

impl Index<usize> for ClientStateRegistry {
    type Output = ClientState;

    fn index(&self, index: usize) -> &Self::Output {
        &self.clients[index]
    }
}

impl IndexMut<usize> for ClientStateRegistry {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.clients.get_mut(index).expect("invalid client key")
    }
}
