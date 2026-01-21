#[derive(Debug)]
pub struct SlidingWindow {
    max_seq: u64,
    bitmap: u64,
}

impl SlidingWindow {
    pub fn new() -> Self {
        Self {
            max_seq: 0,
            bitmap: 0,
        }
    }

    /// Returns true if packet is new and not too old.
    pub fn accept(&mut self, seq: u64) -> bool {
        if seq <= self.max_seq {
            if !self.is_marked(seq) {
                self.mark(seq);
                true
            } else {
                false
            }
        } else {
            self.shift(seq);
            self.mark(seq);
            true
        }
    }

    fn shift(&mut self, seq: u64) {
        self.bitmap <<= seq - self.max_seq;
        self.max_seq = seq;
    }

    fn mark(&mut self, seq: u64) {
        let mask = 1 << self.max_seq - seq;
        self.bitmap |= mask;
    }

    fn is_marked(&self, seq: u64) -> bool {
        let mask = 1 << self.max_seq - seq;
        self.bitmap & mask != 0
    }
}
