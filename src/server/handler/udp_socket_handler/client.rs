use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::{Index, IndexMut},
};

use slab::Slab;

use crate::protocol::udp::SlidingWindow;

pub struct ClientRegistry {
    clients: Slab<Client>,
    addr_to_key: HashMap<SocketAddr, usize>,
}

pub struct Client {
    pub addr: SocketAddr,
    pub send_seq: u64,
    pub recv_seq_window: SlidingWindow,
}

impl ClientRegistry {
    pub fn new() -> Self {
        Self {
            clients: Slab::new(),
            addr_to_key: HashMap::new(),
        }
    }

    pub fn add_client(&mut self, addr: SocketAddr) -> usize {
        let key = self.clients.insert(Client {
            addr,
            send_seq: 0,
            recv_seq_window: SlidingWindow::new(),
        });
        self.addr_to_key.insert(addr, key);
        key
    }

    pub fn remove_client(&mut self, key: usize) -> Option<Client> {
        let client = self.clients.remove(key);
        self.addr_to_key.remove(&client.addr);
        Some(client)
    }

    pub fn get_mut_by_key(&mut self, key: usize) -> Option<&mut Client> {
        self.clients.get_mut(key)
    }

    pub fn get_mut_by_addr(&mut self, addr: SocketAddr) -> Option<&mut Client> {
        let key = self.addr_to_key.get(&addr).copied()?;
        self.clients.get_mut(key)
    }
}

impl Index<usize> for ClientRegistry {
    type Output = Client;

    fn index(&self, key: usize) -> &Self::Output {
        &self.clients[key]
    }
}

impl IndexMut<usize> for ClientRegistry {
    fn index_mut(&mut self, key: usize) -> &mut Self::Output {
        self.clients
            .get_mut(key)
            .expect("client with given key does not exist")
    }
}
