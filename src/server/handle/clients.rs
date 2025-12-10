use std::net::SocketAddr;

use rustc_hash::FxHashMap;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub const CLIENT_CAPACITY: usize = 256;

/// Handle clients.
///
//////////////////////////////////////////////////////////////////////////
//
// Invariants : Hard to describe rigorously...
// But indices must be coherent within structures.
//
// Ex: An index cannot be in `available_indices`
// and in `addr_to_index`.
//
// Good thing is to check if all structures are updated when mutating.
//
pub struct Clients {
    addr_to_index: FxHashMap<SocketAddr, usize>,
    index_to_addr: Vec<Option<SocketAddr>>, // No need to hash.
    available_indices: Vec<usize>,
}

impl Clients {
    pub fn new() -> Self {
        Self {
            addr_to_index: FxHashMap::with_capacity_and_hasher(CLIENT_CAPACITY, Default::default()),
            available_indices: (0..CLIENT_CAPACITY).rev().collect(),
            index_to_addr: vec![None; CLIENT_CAPACITY],
        }
    }

    /// `Err(_)` <=> or :
    /// - Client max capacity reached.
    /// - Client already registered.
    pub fn add(&mut self, client_addr: SocketAddr) -> Result<usize> {
        // Check client registration (fallible).
        //
        // Pop an available index (fallible).
        //
        // Add to mapping structures.

        if self.addr_to_index.contains_key(&client_addr) {
            return Err("client already registered".into());
        }

        let Some(client_index) = self.available_indices.pop() else {
            return Err("client max capacity reached".into());
        };

        self.addr_to_index.insert(client_addr, client_index);
        self.index_to_addr[client_index] = Some(client_addr);

        Ok(client_index)
    }

    /// `Err(_)` <=> Unknown client.
    pub fn _remove(&mut self, client_index: usize) -> Result<()> {
        // Fetch client address (fallible).
        //
        // Remove from mapping structures.
        //
        // Push client index to available indices.

        let Some(client_addr) = self.index_to_addr[client_index] else {
            return Err("unknown client".into());
        };

        self.index_to_addr[client_index] = None;
        self.addr_to_index.remove(&client_addr);

        self.available_indices.push(client_index);

        Ok(())
    }

    pub fn addr_to_index(&mut self, client_addr: SocketAddr) -> Option<usize> {
        self.addr_to_index.get(&client_addr).copied()
    }

    pub fn index_to_addr(&mut self, client_index: usize) -> Option<SocketAddr> {
        self.index_to_addr[client_index]
    }
}
