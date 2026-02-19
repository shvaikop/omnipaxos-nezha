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

/// A cryptographic hash identifying an ordered sequence of entries.
///
/// This represents the hash of a log segment or entire log.
/// Order and length are part of the hash computation.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogHash([u8; 32]);

impl LogHash {
    /// Computes the hash of an ordered sequence of entries.
    ///
    /// The resulting hash:
    /// - Depends on order
    /// - Depends on length
    /// - Changes if any entry changes
    pub fn compute<E: Entry>(entries: &[E]) -> Self {
        let mut hasher = Hasher::new();

        // Domain separation
        hasher.update(b"log/v1");

        // Include length to avoid ambiguity
        hasher.update(&(entries.len() as u64).to_le_bytes());

        let mut buf = Vec::new();

        for entry in entries {
            buf.clear();
            entry.stable_encode(&mut buf);

            hasher.update(&(buf.len() as u64).to_le_bytes());
            hasher.update(&buf);
        }

        Self(hasher.finalize().into())
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
