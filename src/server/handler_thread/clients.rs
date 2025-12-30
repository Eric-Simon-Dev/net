use std::net::SocketAddr;

use rustc_hash::FxHashMap;

use super::ClientId;

/// Maintains a bidirectional mapping between client socket addresses
/// and internally assigned client identifiers.
pub struct Clients {
    // INVARIANT: Bidirectionality.
    // Registered addresses map to a unique ID and vice-versa.
    addr_to_id: FxHashMap<SocketAddr, ClientId>,
    id_to_addr: FxHashMap<ClientId, SocketAddr>,
    next_id: u64,
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

// ---- Mapping ----

impl Clients {
    pub fn addr_to_id(&mut self, addr: SocketAddr) -> Option<ClientId> {
        self.addr_to_id.get(&addr).copied()
    }

    pub fn id_to_addr(&mut self, id: ClientId) -> Option<SocketAddr> {
        self.id_to_addr.get(&id).copied()
    }
}

// ---- ID generation ----

impl Clients {
    /// Generate a new unique `ClientId`.
    ///
    /// # Notes
    /// If it weren't unique, it would broke the bidirectionality invariant.
    fn generate_client_id(&mut self) -> ClientId {
        let id = self.next_id;
        self.next_id += 1;
        ClientId(id)
    }
}

// ---- Add & Remove ----

impl Clients {
    /// Registers a new client address and assign it a unique `ClientId`.
    ///
    /// # Preconditions
    /// Client address not already registered.
    ///
    /// # Panics
    /// Client address registered.
    pub fn add(&mut self, addr: SocketAddr) -> ClientId {
        if self.addr_to_id.contains_key(&addr) {
            panic!("cannot add already registered client");
        }
        let id = self.generate_client_id();
        // UNWRAPS: Bidirectionality invariant + check.
        self.addr_to_id.insert(addr, id).unwrap();
        self.id_to_addr.insert(id, addr).unwrap();
        id
    }

    /// Removes a client by its identifier.
    ///
    /// # Preconditions
    /// Client ID is registered.
    ///
    /// # Panics
    /// Client ID not registered.
    pub fn remove(&mut self, id: ClientId) {
        if !self.id_to_addr.contains_key(&id) {
            panic!("cannot remove unregistered client");
        }
        // UNWRAPS: Bidirectionality invariant + check.
        let addr = self.id_to_addr.remove(&id).unwrap();
        self.addr_to_id.remove(&addr).unwrap();
    }
}
