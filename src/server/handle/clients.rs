use std::net::SocketAddr;

use rustc_hash::FxHashMap;

use super::ClientId;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

pub struct Clients {
    addr_to_id: FxHashMap<SocketAddr, ClientId>,
    id_to_addr: FxHashMap<ClientId, SocketAddr>,
    next_id: usize,
}

//------// Constructor //------//

impl Clients {
    pub fn new() -> Self {
        Self {
            addr_to_id: FxHashMap::with_hasher(Default::default()),
            id_to_addr: FxHashMap::with_hasher(Default::default()),
            next_id: 0,
        }
    }
}

//------// Add & Remove //------//

impl Clients {
    /// `Err(_)` <=> Client already registered.
    pub fn add(&mut self, addr: SocketAddr) -> Result<ClientId> {
        //------// Check //------//

        if self.addr_to_id.contains_key(&addr) {
            return Err("already registered".into());
        }

        //------// Add //------//

        let id = self.generate_client_id();
        self.addr_to_id.insert(addr, id);
        self.id_to_addr.insert(id, addr);

        //------//

        Ok(id)
    }

    fn generate_client_id(&mut self) -> ClientId {
        self.next_id += 1;
        ClientId(self.next_id - 1)
    }

    /// `Err(_)` <=> Unknown client.
    pub fn _remove(&mut self, id: ClientId) -> Result<()> {
        //------// Check //------//

        if self.id_to_addr.contains_key(&id) {
            return Err("unknown".into());
        }

        //------// Remove //------//

        // UNWRAP : From earlier check.
        let addr = self.id_to_addr.remove(&id).unwrap();
        self.addr_to_id.remove(&addr);

        //------//

        Ok(())
    }
}

//------// Accessors //------//

impl Clients {
    pub fn addr_to_id(&mut self, addr: SocketAddr) -> Option<ClientId> {
        self.addr_to_id.get(&addr).copied()
    }

    pub fn id_to_addr(&mut self, id: ClientId) -> Option<SocketAddr> {
        self.id_to_addr.get(&id).copied()
    }
}
