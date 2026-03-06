use super::Entry;
use blake3::Hasher;
use core::fmt;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// A cryptographic hash identifying a single `Entry`.
///
/// This is a deterministic 256-bit digest (32 bytes) computed from the
/// canonical encoding of an entry.
///
/// Equal logical entries MUST produce identical `EntryHash` values
/// across processes and machines.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct EntryHash([u8; 32]);

impl EntryHash {
    /// Computes the hash of a single entry.
    ///
    /// The entry must provide a canonical encoding via `stable_encode`.
    pub fn compute<E: Entry>(entry: &E) -> Self {
        let mut hasher = Hasher::new();

        // Domain separation to avoid cross-type collisions
        hasher.update(b"entry/v1");

        let mut buf = Vec::new();
        entry.stable_encode(&mut buf);

        hasher.update(&(buf.len() as u64).to_le_bytes());
        hasher.update(&buf);

        Self(hasher.finalize().into())
    }

    /// Returns the raw 32-byte hash.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for EntryHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EntryHash({})", hex::encode(self.0))
    }
}

/// A hash representing the current contents of the log.
///
/// Maintained as a running XOR of per-entry content hashes so that all
/// updates are O(1):

/// Order is intentionally excluded- the log is always ordered by
/// deadline, so only the entry contents needs to match.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct LogHash([u8; 32]);

impl LogHash {
    /// Returns the hash of an empty log.
    pub fn new() -> Self {
        LogHash([0u8; 32])
    }

    /// XOR the hash of `entry` into this accumulator
    pub fn toggle<E: Entry>(&mut self, entry: &E) {
        let h = EntryHash::compute(entry);
        for (a, b) in self.0.iter_mut().zip(h.as_bytes()) {
            *a ^= b;
        }
    }

    /// Computes the hash of a collection of entries by XOR-accumulation.
    /// Equivalent to calling `toggle` for each entry on a fresh `LogHash::new()`.
    pub fn compute<E: Entry>(entries: &[E]) -> Self {
        let mut h = LogHash::new();
        for entry in entries {
            h.toggle(entry);
        }
        h
    }

    /// Returns the raw 32-byte hash.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for LogHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LogHash({})", hex::encode(self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{messages::RequestId, storage::Snapshot, util::NodeId};
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};

    // Minimal Entry stub – only stable_encode matters for hashing.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    struct E(u64);

    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    struct ESnap;
    impl Snapshot<E> for ESnap {
        fn create(_: &[E]) -> Self {
            ESnap
        }
        fn merge(&mut self, _: Self) {}
        fn use_snapshots() -> bool {
            false
        }
    }

    impl Entry for E {
        type Snapshot = ESnap;
        fn stable_encode(&self, out: &mut Vec<u8>) {
            out.extend_from_slice(&self.0.to_le_bytes());
        }
        fn get_deadline(&self) -> u64 {
            0
        }
        fn set_deadline(&mut self, _: u64) {}
        fn get_request_id(&self) -> RequestId {
            Default::default()
        }
        fn set_request_id(&mut self, _: RequestId) {}
        fn get_nezha_proxy_id(&self) -> NodeId {
            0
        }
        fn set_nezha_proxy_id(&mut self, _: NodeId) {}
    }

    // Toggle an entry in then out again resets back to empty-log hash.
    #[test]
    fn toggle_is_own_inverse() {
        let mut h = LogHash::new();
        let e = E(42);
        h.toggle(&e);
        h.toggle(&e);
        assert_eq!(h, LogHash::new());
    }

    // compute() and repeated toggle() on the same entries produce the same result.
    #[test]
    fn compute_matches_incremental() {
        let entries = vec![E(1), E(2), E(3)];
        let batch = LogHash::compute(&entries);

        let mut incremental = LogHash::new();
        for e in &entries {
            incremental.toggle(e);
        }
        assert_eq!(batch, incremental);
    }

    // Replacing an entry: toggle old out, toggle new in.
    #[test]
    fn replace_entry() {
        let mut h = LogHash::new();
        h.toggle(&E(1));
        h.toggle(&E(2));

        // Replace E(1) with E(99)
        h.toggle(&E(1)); // remove old
        h.toggle(&E(99)); // add new

        let mut expected = LogHash::new();
        expected.toggle(&E(2));
        expected.toggle(&E(99));

        assert_eq!(h, expected);
    }

    // Two different sets of entries should produce different hashes.
    #[test]
    fn different_entries_differ() {
        assert_ne!(LogHash::compute(&[E(1)]), LogHash::compute(&[E(2)]));
    }
}
