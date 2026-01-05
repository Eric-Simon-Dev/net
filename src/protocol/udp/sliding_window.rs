/// Sliding window deduplicator for packet sequence numbers.
///
/// Accepts out-of-order packets within a fixed window
/// and drops duplicates or packets that are too old.
#[derive(Debug)]
pub struct SlidingWindow {
    max_seq: u64,
    bitmap: u64,
}

impl SlidingWindow {
    /// Size of the sliding window
    /// `seq` under `max_seq` - 64 are too old.
    pub const WINDOW_SIZE: u64 = 64;

    pub fn new() -> Self {
        Self {
            max_seq: 0,
            bitmap: 0,
        }
    }

    /// Checks whether a sequence number should be accepted.
    ///
    /// Returns `true` if the packet is new and should be processed.
    /// Returns `false` if the packet is a duplicate or too old.
    pub fn check_and_mark(&mut self, seq: u64) -> bool {
        // First packet ever received
        if self.bitmap == 0 {
            self.max_seq = seq;
            self.bitmap = 1;
            return true;
        }

        if seq > self.max_seq {
            let shift = seq - self.max_seq;

            if shift >= Self::WINDOW_SIZE {
                // Packet is far ahead: reset window
                self.bitmap = 1;
            } else {
                self.bitmap <<= shift;
                self.bitmap |= 1;
            }

            self.max_seq = seq;
            return true;
        }

        let diff = self.max_seq - seq;

        if diff >= Self::WINDOW_SIZE {
            // Packet is too old
            return false;
        }

        let mask = 1u64 << diff;

        if self.bitmap & mask != 0 {
            // Duplicate packet
            false
        } else {
            // New packet inside the window
            self.bitmap |= mask;
            true
        }
    }

    /// Returns the highest sequence number seen so far.
    pub fn max_seq(&self) -> u64 {
        self.max_seq
    }
}
