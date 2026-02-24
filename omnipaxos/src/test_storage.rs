#![cfg(test)]

use crate::{
    ballot_leader_election::Ballot,
    storage::{Entry, LogHash, StopSign, Storage, StorageOp, StorageResult},
};

/// Simple in-memory storage implementation intended for unit tests.
pub(crate) struct TestStorage<T: Entry> {
    log: Vec<T>,
    trimmed_idx: usize,
    promise: Option<Ballot>,
    accepted_round: Option<Ballot>,
    decided_idx: usize,
    compacted_idx: usize,
    stopsign: Option<StopSign>,
    snapshot: Option<T::Snapshot>,
    sync_point: usize,
}

impl<T: Entry> Default for TestStorage<T> {
    fn default() -> Self {
        Self {
            log: Vec::new(),
            trimmed_idx: 0,
            promise: None,
            accepted_round: None,
            decided_idx: 0,
            compacted_idx: 0,
            stopsign: None,
            snapshot: None,
            sync_point: 0,
        }
    }
}

impl<T: Entry> TestStorage<T> {
    pub(crate) fn with_entries(entries: Vec<T>) -> Self {
        Self {
            log: entries,
            ..Default::default()
        }
    }

    fn relative_index(&self, idx: usize) -> StorageResult<usize> {
        idx.checked_sub(self.trimmed_idx)
            .ok_or_else(|| "index before trimmed region".into())
            .and_then(|rel| {
                if rel <= self.log.len() {
                    Ok(rel)
                } else {
                    Err("index beyond log".into())
                }
            })
    }
}

impl<T: Entry> Storage<T> for TestStorage<T> {
    fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()> {
        for op in ops {
            match op {
                StorageOp::AppendEntry(entry) => self.append_entry(entry)?,
                StorageOp::AppendEntries(entries) => self.append_entries(entries)?,
                StorageOp::AppendOnPrefix(idx, entries) => self.append_on_prefix(idx, entries)?,
                StorageOp::SetPromise(bal) => self.set_promise(bal)?,
                StorageOp::SetDecidedIndex(idx) => self.set_decided_idx(idx)?,
                StorageOp::SetAcceptedRound(bal) => self.set_accepted_round(bal)?,
                StorageOp::SetCompactedIdx(idx) => self.set_compacted_idx(idx)?,
                StorageOp::Trim(idx) => self.trim(idx)?,
                StorageOp::SetStopsign(ss) => self.set_stopsign(ss)?,
                StorageOp::SetSnapshot(snap) => self.set_snapshot(snap)?,
                StorageOp::SetSyncPoint(sp) => self.set_sync_point(sp)?,
                StorageOp::UpdateDeadline(idx, deadline) => self.update_deadline(idx, deadline)?,
                StorageOp::ReplaceEntry(idx, entry) => {
                    let _ = self.replace_entry(idx, entry)?;
                }
            }
        }
        Ok(())
    }

    fn append_entry(&mut self, entry: T) -> StorageResult<()> {
        self.log.push(entry);
        Ok(())
    }

    fn append_entries(&mut self, mut entries: Vec<T>) -> StorageResult<()> {
        self.log.append(&mut entries);
        Ok(())
    }

    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()> {
        let rel = self.relative_index(from_idx)?;
        self.log.truncate(rel);
        self.append_entries(entries)
    }

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.promise = Some(n_prom);
        Ok(())
    }

    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        self.decided_idx = ld;
        Ok(())
    }

    fn get_decided_idx(&self) -> StorageResult<usize> {
        Ok(self.decided_idx)
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.accepted_round = Some(na);
        Ok(())
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.accepted_round)
    }

    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        if to <= from {
            return Ok(vec![]);
        }
        let start = self.relative_index(from)?;
        let end = self.relative_index(to)?.min(self.log.len());
        if start > end {
            return Ok(vec![]);
        }
        Ok(self.log[start..end].to_vec())
    }

    fn get_log_len(&self) -> StorageResult<usize> {
        Ok(self.log.len())
    }

    fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        let start = self.relative_index(from)?.min(self.log.len());
        Ok(self.log[start..].to_vec())
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.promise)
    }

    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        self.stopsign = s;
        Ok(())
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        Ok(self.stopsign.clone())
    }

    fn trim(&mut self, idx: usize) -> StorageResult<()> {
        if idx > self.trimmed_idx {
            let to_trim = (idx - self.trimmed_idx).min(self.log.len());
            self.log.drain(0..to_trim);
            self.trimmed_idx = idx;
        }
        Ok(())
    }

    fn set_compacted_idx(&mut self, idx: usize) -> StorageResult<()> {
        self.compacted_idx = idx;
        Ok(())
    }

    fn get_compacted_idx(&self) -> StorageResult<usize> {
        Ok(self.compacted_idx)
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        self.snapshot = snapshot;
        Ok(())
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        Ok(self.snapshot.clone())
    }

    fn set_sync_point(&mut self, sync_point: usize) -> StorageResult<()> {
        self.sync_point = sync_point;
        Ok(())
    }

    fn get_sync_point(&self) -> StorageResult<usize> {
        Ok(self.sync_point)
    }

    fn update_deadline(&mut self, idx: usize, deadline: u64) -> StorageResult<()> {
        let rel = self.relative_index(idx)?;
        if let Some(entry) = self.log.get_mut(rel) {
            entry.set_deadline(deadline);
            Ok(())
        } else {
            Err("deadline index out of bounds".into())
        }
    }

    fn get_hash(&self, to: usize) -> StorageResult<LogHash> {
        if to < self.trimmed_idx {
            return Err("hash index before trim".into());
        }
        let rel_to = (to - self.trimmed_idx).min(self.log.len());
        Ok(LogHash::compute(&self.log[..rel_to]))
    }

    fn replace_entry(&mut self, idx: usize, new_entry: T) -> StorageResult<T> {
        let rel = self.relative_index(idx)?;
        self.log
            .get_mut(rel)
            .map(|entry| std::mem::replace(entry, new_entry))
            .ok_or_else(|| "replace index out of bounds".into())
    }
}
