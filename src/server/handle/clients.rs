use std::net::SocketAddr;

use rustc_hash::FxHashMap;

use super::ClientId;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

/// Maintains a bidirectional mapping between client socket addresses
/// and internally assigned client identifiers.
pub struct Clients {
    addr_to_id: FxHashMap<SocketAddr, ClientId>,
    id_to_addr: FxHashMap<ClientId, SocketAddr>,
    next_id: usize,
}

// ---- Constructor ----

impl Clients {
    pub fn new() -> Self {
        Self {
            addr_to_id: FxHashMap::with_hasher(Default::default()),
            id_to_addr: FxHashMap::with_hasher(Default::default()),
            next_id: 0,
        }
    }
}

// ---- Accessors ----

impl Clients {
    pub fn addr_to_id(&mut self, addr: SocketAddr) -> Option<ClientId> {
        self.addr_to_id.get(&addr).copied()
    }

    pub fn id_to_addr(&mut self, id: ClientId) -> Option<SocketAddr> {
        self.id_to_addr.get(&id).copied()
    }
}

// ---- Add & Remove ----

impl Clients {
    /// Register a new client address and assign it a unique `ClientId`.
    ///
    /// # Errors
    /// Client address is already registered.
    pub fn add(&mut self, addr: SocketAddr) -> Result<ClientId> {
        // Check for existing client
        if self.addr_to_id.contains_key(&addr) {
            return Err("already registered".into());
        }

        // Insert new client
        let id = self.generate_client_id();
        self.addr_to_id.insert(addr, id);
        self.id_to_addr.insert(id, addr);

        Ok(id)
    }

    /// Generate a new unique `ClientId`.
    fn generate_client_id(&mut self) -> ClientId {
        let id = self.next_id;
        self.next_id += 1;
        ClientId(id)
    }

    /// Remove a client by its identifier.
    ///
    /// # Errors
    /// Client ID unknown.
    pub fn remove(&mut self, id: ClientId) -> Result<()> {
        // Remove from `id_to_addr` and ensure the client exists.
        let addr = self
            .id_to_addr
            .remove(&id)
            .ok_or_else(|| Error::from("unknown"))?;

        // Remove from `addr_to_id`.
        self.addr_to_id.remove(&addr);

        Ok(())
    }
}
