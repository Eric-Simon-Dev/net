use std::net::SocketAddr;

use rustc_hash::FxHashMap;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub const CLIENT_CAPACITY: usize = 256;

/// Handle clients.
///
///////////////////////////////////////////////////////////////////////
//
// Invariants :
// - Hard to describe rigorously...
//
pub struct Clients {
    addr_to_index: FxHashMap<SocketAddr, usize>,
    available_client_indices: Vec<usize>,
    clients: Vec<Option<SocketAddr>>,
}

impl Clients {
    pub fn new() -> Self {
        Self {
            addr_to_index: FxHashMap::with_capacity_and_hasher(CLIENT_CAPACITY, Default::default()),
            available_client_indices: (0..CLIENT_CAPACITY).rev().collect(),
            clients: vec![None; CLIENT_CAPACITY],
        }
    }

    pub fn add(&mut self, client_addr: SocketAddr) -> Result<usize> {
        let Some(client_index) = self.available_client_indices.pop() else {
            return Err("client capacity reached".into());
        };
        self.addr_to_index.insert(client_addr, client_index);
        self.clients[client_index] = Some(client_addr);
        Ok(client_index)
    }

    pub fn _remove(&mut self, client_addr: SocketAddr) -> Result<()> {
        let Some(client_index) = self.addr_to_index.remove(&client_addr) else {
            return Err("unknown client".into());
        };
        self.clients[client_index] = None;
        self.available_client_indices.push(client_index);
        Ok(())
    }

    pub fn addr_to_index(&mut self, client_addr: SocketAddr) -> Option<usize> {
        self.addr_to_index.get(&client_addr).copied()
    }

    pub fn index_to_addr(&mut self, client_index: usize) -> Option<SocketAddr> {
        self.clients[client_index]
    }
}
