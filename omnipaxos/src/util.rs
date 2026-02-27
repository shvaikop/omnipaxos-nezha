use super::{
    ballot_leader_election::Ballot,
    messages::sequence_paxos::Promise,
    storage::{Entry, SnapshotType, StopSign},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt::Debug, marker::PhantomData};

/// Struct used to help another server synchronize their log with the current state of our own log.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LogSync<T>
where
    T: Entry,
{
    /// The decided snapshot.
    pub decided_snapshot: Option<SnapshotType<T>>,
    /// The log suffix.
    pub suffix: Vec<T>,
    /// The index of the log where the entries from `suffix` should be applied at (also the compacted idx of `decided_snapshot` if it exists).
    pub sync_idx: usize,
    /// The accepted StopSign.
    pub stopsign: Option<StopSign>,
}

#[derive(Debug, Clone, Default)]
/// Promise without the log update
pub(crate) struct PromiseMetaData {
    pub n_accepted: Ballot,
    pub accepted_idx: usize,
    pub decided_idx: usize,
    pub pid: NodeId,
}

impl PartialOrd for PromiseMetaData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ordering = if self.n_accepted == other.n_accepted
            && self.accepted_idx == other.accepted_idx
            && self.pid == other.pid
        {
            Ordering::Equal
        } else if self.n_accepted > other.n_accepted
            || (self.n_accepted == other.n_accepted && self.accepted_idx > other.accepted_idx)
        {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        Some(ordering)
    }
}

impl PartialEq for PromiseMetaData {
    fn eq(&self, other: &Self) -> bool {
        self.n_accepted == other.n_accepted
            && self.accepted_idx == other.accepted_idx
            && self.pid == other.pid
    }
}

#[derive(Debug, Clone)]
/// The promise state of a node.
enum PromiseState {
    /// Not promised to any leader
    NotPromised,
    /// Promised to my ballot
    Promised(PromiseMetaData),
    /// Promised to a leader who's ballot is greater than mine
    PromisedHigher,
}

#[derive(Debug, Clone)]
pub(crate) struct LeaderState<T>
where
    T: Entry,
{
    pub n_leader: Ballot,
    promises_meta: Vec<PromiseState>,
    // the sequence number of accepts for each follower where AcceptSync has sequence number = 1
    follower_seq_nums: Vec<SequenceNumber>,
    pub accepted_indexes: Vec<usize>,
    max_promise_meta: PromiseMetaData,
    max_promise_sync: Option<LogSync<T>>,
    latest_accept_meta: Vec<Option<(Ballot, usize)>>, //  index in outgoing
    pub max_pid: usize,
    // The number of promises needed in the prepare phase to become synced and
    // the number of accepteds needed in the accept phase to decide an entry.
    pub quorum: Quorum,

    // Nezha fast-path optimization related
    sorted_accepted_indexes: Vec<usize>,
}

impl<T> LeaderState<T>
where
    T: Entry,
{
    pub fn with(n_leader: Ballot, max_pid: usize, quorum: Quorum) -> Self {
        Self {
            n_leader,
            promises_meta: vec![PromiseState::NotPromised; max_pid],
            follower_seq_nums: vec![SequenceNumber::default(); max_pid],
            accepted_indexes: vec![0; max_pid],
            max_promise_meta: PromiseMetaData::default(),
            max_promise_sync: None,
            latest_accept_meta: vec![None; max_pid],
            max_pid,
            quorum,
            sorted_accepted_indexes: vec![0; max_pid],
        }
    }

    fn pid_to_idx(pid: NodeId) -> usize {
        (pid - 1) as usize
    }

    // Resets `pid`'s accept sequence to indicate they are in the next session of accepts
    pub fn increment_seq_num_session(&mut self, pid: NodeId) {
        let idx = Self::pid_to_idx(pid);
        self.follower_seq_nums[idx].session += 1;
        self.follower_seq_nums[idx].counter = 0;
    }

    pub fn next_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        let idx = Self::pid_to_idx(pid);
        self.follower_seq_nums[idx].counter += 1;
        self.follower_seq_nums[idx]
    }

    pub fn get_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        self.follower_seq_nums[Self::pid_to_idx(pid)]
    }

    pub fn set_promise(&mut self, prom: Promise<T>, from: NodeId, check_max_prom: bool) -> bool {
        let promise_meta = PromiseMetaData {
            n_accepted: prom.n_accepted,
            accepted_idx: prom.accepted_idx,
            decided_idx: prom.decided_idx,
            pid: from,
        };
        if check_max_prom && promise_meta > self.max_promise_meta {
            self.max_promise_meta = promise_meta.clone();
            self.max_promise_sync = prom.log_sync;
        }
        self.promises_meta[Self::pid_to_idx(from)] = PromiseState::Promised(promise_meta);
        let num_promised = self
            .promises_meta
            .iter()
            .filter(|p| matches!(p, PromiseState::Promised(_)))
            .count();
        self.quorum.is_prepare_quorum(num_promised)
    }

    pub fn reset_promise(&mut self, pid: NodeId) {
        self.promises_meta[Self::pid_to_idx(pid)] = PromiseState::NotPromised;
    }

    /// Node `pid` seen with ballot greater than my ballot
    pub fn lost_promise(&mut self, pid: NodeId) {
        self.promises_meta[Self::pid_to_idx(pid)] = PromiseState::PromisedHigher;
    }

    pub fn take_max_promise_sync(&mut self) -> Option<LogSync<T>> {
        std::mem::take(&mut self.max_promise_sync)
    }

    pub fn get_max_promise_meta(&self) -> &PromiseMetaData {
        &self.max_promise_meta
    }

    pub fn get_max_decided_idx(&self) -> usize {
        self.promises_meta
            .iter()
            .filter_map(|p| match p {
                PromiseState::Promised(m) => Some(m.decided_idx),
                _ => None,
            })
            .max()
            .unwrap_or_default()
    }

    pub fn get_promise_meta(&self, pid: NodeId) -> &PromiseMetaData {
        match &self.promises_meta[Self::pid_to_idx(pid)] {
            PromiseState::Promised(metadata) => metadata,
            _ => panic!("No Metadata found for promised follower"),
        }
    }

    pub fn get_min_all_accepted_idx(&self) -> &usize {
        self.accepted_indexes
            .iter()
            .min()
            .expect("Should be all initialised to 0!")
    }

    pub fn reset_latest_accept_meta(&mut self) {
        self.latest_accept_meta = vec![None; self.max_pid];
    }

    pub fn get_promised_followers(&self) -> Vec<NodeId> {
        self.promises_meta
            .iter()
            .enumerate()
            .filter_map(|(idx, x)| match x {
                PromiseState::Promised(_) if idx != Self::pid_to_idx(self.n_leader.pid) => {
                    Some((idx + 1) as NodeId)
                }
                _ => None,
            })
            .collect()
    }

    /// The pids of peers which have not promised a higher ballot than mine.
    pub(crate) fn get_preparable_peers(&self, peers: &[NodeId]) -> Vec<NodeId> {
        peers
            .iter()
            .filter_map(|pid| {
                let idx = Self::pid_to_idx(*pid);
                match self.promises_meta.get(idx).unwrap() {
                    PromiseState::NotPromised => Some(*pid),
                    _ => None,
                }
            })
            .collect()
    }

    pub fn set_latest_accept_meta(&mut self, pid: NodeId, idx: Option<usize>) {
        let meta = idx.map(|x| (self.n_leader, x));
        self.latest_accept_meta[Self::pid_to_idx(pid)] = meta;
    }

    pub fn set_accepted_idx(&mut self, pid: NodeId, idx: usize) {
        let node_idx = Self::pid_to_idx(pid);
        let old_accepted_idx = self.accepted_indexes[node_idx];

        // If sent sync_idx is smaller or equal to current then do nothing
        if idx < old_accepted_idx {
            return;
        }

        // Update unsorted structure
        self.accepted_indexes[node_idx] = idx;

        // Update the sorted vector, used in `get_commit_idx` to avoid recomputing each time
        // Remove old value from sorted vector
        if let Ok(pos) = self
            .sorted_accepted_indexes
            .binary_search(&old_accepted_idx)
        {
            self.sorted_accepted_indexes.remove(pos);
        } else {
            // This should never happen if invariants are correct
            debug_assert!(false, "Old sync_idx not found in sorted vector");
        }

        // Insert new value into sorted position
        let insert_pos = self
            .sorted_accepted_indexes
            .binary_search(&idx)
            .unwrap_or_else(|pos| pos);

        self.sorted_accepted_indexes.insert(insert_pos, idx);
    }

    pub fn get_latest_accept_meta(&self, pid: NodeId) -> Option<(Ballot, usize)> {
        self.latest_accept_meta
            .get(Self::pid_to_idx(pid))
            .unwrap()
            .as_ref()
            .copied()
    }

    pub fn get_decided_idx(&self, pid: NodeId) -> Option<usize> {
        match self.promises_meta.get(Self::pid_to_idx(pid)).unwrap() {
            PromiseState::Promised(metadata) => Some(metadata.decided_idx),
            _ => None,
        }
    }

    pub fn get_accepted_idx(&self, pid: NodeId) -> usize {
        *self.accepted_indexes.get(Self::pid_to_idx(pid)).unwrap()
    }

    pub fn is_chosen(&self, idx: usize) -> bool {
        let num_accepted = self
            .accepted_indexes
            .iter()
            .filter(|la| **la >= idx)
            .count();
        self.quorum.is_accept_quorum(num_accepted)
    }

    pub fn get_commit_idx(&self) -> usize {
        // Required quorum size, assuming the leader has the furthest sync_idx already
        let quorum_size = self.quorum.get_write_quorum_size().saturating_sub(1);
        // The index of the max sync_point (if synced_indexes_values was sorted) which a quorum returned
        let nth_smallest_idx = self.sorted_accepted_indexes.len() - quorum_size;

        // `sorted_accepted_indexes` must be cloned because `select_nth_unstable` reorders in place
        let mut sorted_synced_indexes_cloned = self.sorted_accepted_indexes.clone();
        let (_, commit_idx, _) = sorted_synced_indexes_cloned.select_nth_unstable(nth_smallest_idx);
        *commit_idx
    }
}

/// The entry read in the log.
#[derive(Debug, Clone)]
pub enum LogEntry<T>
where
    T: Entry,
{
    /// The entry is decided.
    Decided(T),
    /// The entry is NOT decided. Might be removed from the log at a later time.
    Undecided(T),
    /// The entry has been trimmed.
    Trimmed(TrimmedIndex),
    /// The entry has been snapshotted.
    Snapshotted(SnapshottedEntry<T>),
    /// This Sequence Paxos instance has been stopped for reconfiguration. The accompanying bool
    /// indicates whether the reconfiguration has been decided or not. If it is `true`, then the OmniPaxos instance for the new configuration can be started.
    StopSign(StopSign, bool),
}

impl<T: PartialEq + Entry> PartialEq for LogEntry<T>
where
    <T as Entry>::Snapshot: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LogEntry::Decided(v1), LogEntry::Decided(v2)) => v1 == v2,
            (LogEntry::Undecided(v1), LogEntry::Undecided(v2)) => v1 == v2,
            (LogEntry::Trimmed(idx1), LogEntry::Trimmed(idx2)) => idx1 == idx2,
            (LogEntry::Snapshotted(s1), LogEntry::Snapshotted(s2)) => s1 == s2,
            (LogEntry::StopSign(ss1, b1), LogEntry::StopSign(ss2, b2)) => ss1 == ss2 && b1 == b2,
            _ => false,
        }
    }
}

/// Convenience struct for checking if a certain index exists, is compacted or is a StopSign.
#[derive(Debug, Clone)]
pub(crate) enum IndexEntry {
    Entry,
    Compacted,
    StopSign(StopSign),
}

#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct SnapshottedEntry<T>
where
    T: Entry,
{
    pub trimmed_idx: TrimmedIndex,
    pub snapshot: T::Snapshot,
    _p: PhantomData<T>,
}

impl<T> SnapshottedEntry<T>
where
    T: Entry,
{
    pub(crate) fn with(trimmed_idx: usize, snapshot: T::Snapshot) -> Self {
        Self {
            trimmed_idx,
            snapshot,
            _p: PhantomData,
        }
    }
}

impl<T: Entry> PartialEq for SnapshottedEntry<T>
where
    <T as Entry>::Snapshot: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.trimmed_idx == other.trimmed_idx && self.snapshot == other.snapshot
    }
}

pub(crate) mod defaults {
    pub(crate) const BUFFER_SIZE: usize = 100000;
    pub(crate) const BLE_BUFFER_SIZE: usize = 100;
    pub(crate) const ELECTION_TIMEOUT: u64 = 1;
    pub(crate) const RESEND_MESSAGE_TIMEOUT: u64 = 100;
    pub(crate) const FLUSH_BATCH_TIMEOUT: u64 = 200;
}

#[allow(missing_docs)]
pub type TrimmedIndex = usize;

/// ID for an OmniPaxos node
pub type NodeId = u64;
/// ID for an OmniPaxos configuration (i.e., the set of servers in an OmniPaxos cluster)
pub type ConfigurationId = u32;

/// Error message to display when there was an error reading to the storage implementation.
pub const READ_ERROR_MSG: &str = "Error reading from storage.";
/// Error message to display when there was an error writing to the storage implementation.
pub const WRITE_ERROR_MSG: &str = "Error writing to storage.";

/// Used for checking the ordering of message sequences in the accept phase
#[derive(PartialEq, Eq)]
pub(crate) enum MessageStatus {
    /// Expected message sequence progression
    Expected,
    /// Identified a message sequence break
    DroppedPreceding,
    /// An already identified message sequence break
    Outdated,
}

/// Keeps track of the ordering of messages in the accept phase
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SequenceNumber {
    /// Meant to refer to a TCP session
    pub session: u64,
    /// The sequence number with respect to a session
    pub counter: u64,
}

impl SequenceNumber {
    /// Compares this sequence number with the sequence number of an incoming message.
    pub(crate) fn check_msg_status(&self, msg_seq_num: SequenceNumber) -> MessageStatus {
        if msg_seq_num.session == self.session && msg_seq_num.counter == self.counter + 1 {
            MessageStatus::Expected
        } else if msg_seq_num <= *self {
            MessageStatus::Outdated
        } else {
            MessageStatus::DroppedPreceding
        }
    }
}

pub(crate) struct LogicalClock {
    time: u64,
    timeout: u64,
}

impl LogicalClock {
    pub fn with(timeout: u64) -> Self {
        Self { time: 0, timeout }
    }

    pub fn tick_and_check_timeout(&mut self) -> bool {
        self.time += 1;
        if self.time == self.timeout {
            self.time = 0;
            true
        } else {
            false
        }
    }
}

/// Flexible quorums can be used to increase/decrease the read and write quorum sizes,
/// for different latency vs fault tolerance tradeoffs.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), derive(Deserialize))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FlexibleQuorum {
    /// The number of nodes a leader needs to consult to get an up-to-date view of the log.
    pub read_quorum_size: usize,
    /// The number of acknowledgments a leader needs to commit an entry to the log
    pub write_quorum_size: usize,
}

/// The type of quorum used by the OmniPaxos cluster.
#[derive(Copy, Clone, Debug)]
pub(crate) enum Quorum {
    /// Both the read quorum and the write quorums are a majority of nodes
    Majority(usize),
    /// The read and write quorum sizes are defined by a `FlexibleQuorum`
    Flexible(FlexibleQuorum),
}

impl Quorum {
    pub(crate) fn with(flexible_quorum_config: Option<FlexibleQuorum>, num_nodes: usize) -> Self {
        match flexible_quorum_config {
            Some(FlexibleQuorum {
                read_quorum_size,
                write_quorum_size,
            }) => Quorum::Flexible(FlexibleQuorum {
                read_quorum_size,
                write_quorum_size,
            }),
            None => Quorum::Majority(num_nodes / 2 + 1),
        }
    }

    pub(crate) fn is_prepare_quorum(&self, num_nodes: usize) -> bool {
        match self {
            Quorum::Majority(majority) => num_nodes >= *majority,
            Quorum::Flexible(flex_quorum) => num_nodes >= flex_quorum.read_quorum_size,
        }
    }

    pub(crate) fn is_accept_quorum(&self, num_nodes: usize) -> bool {
        match self {
            Quorum::Majority(majority) => num_nodes >= *majority,
            Quorum::Flexible(flex_quorum) => num_nodes >= flex_quorum.write_quorum_size,
        }
    }

    pub(crate) fn is_super_quorum(&self, num_nodes: usize) -> bool {
        let f = match self {
            Quorum::Majority(majority) => majority - 1,
            Quorum::Flexible(flex_quorum) => flex_quorum.write_quorum_size - 1,
        };
        let super_quorum_size = (f + 1) / 2 + f + 1; // ceil(f/2) + f + 1
        num_nodes >= super_quorum_size
    }

    #[allow(dead_code)]
    pub(crate) fn get_read_quorum_size(&self) -> usize {
        match self {
            Quorum::Majority(majority) => *majority,
            Quorum::Flexible(flex_quorum) => flex_quorum.read_quorum_size,
        }
    }

    pub(crate) fn get_write_quorum_size(&self) -> usize {
        match self {
            Quorum::Majority(majority) => *majority,
            Quorum::Flexible(flex_quorum) => flex_quorum.write_quorum_size,
        }
    }
}

/// The entries flushed due to an append operation
pub(crate) struct AcceptedMetaData<T: Entry> {
    pub accepted_idx: usize,
    #[cfg(not(feature = "unicache"))]
    pub entries: Vec<T>,
    #[cfg(feature = "unicache")]
    pub entries: Vec<T::EncodeResult>,
}

#[cfg(not(feature = "unicache"))]
#[cfg(test)]
mod tests {
    use super::*; // Import functions and types from this module
    use crate::storage::NoSnapshot;
    #[test]
    fn preparable_peers_test() {
        type Value = ();

        impl Entry for Value {
            type Snapshot = NoSnapshot;

            fn get_deadline(&self) -> u64 {
                0
            }

            fn set_deadline(&mut self, _deadline: u64) {
                // no deadlines used in this test entry
            }

            fn get_request_id(&self) -> uuid::Uuid {
                uuid::Uuid::nil()
            }

            fn set_request_id(&mut self, _request_id: uuid::Uuid) {
                // no request ids used in this test entry
            }

            /// This test_only Value impl always hashes to 0 for simplicity
            fn stable_encode(&self, out: &mut Vec<u8>) {
                out.push(0)
            }
        }

        let nodes = vec![6, 7, 8];
        let quorum = Quorum::Majority(2);
        let max_pid = 8;
        let leader_state =
            LeaderState::<Value>::with(Ballot::with(1, 1, 1, max_pid), max_pid as usize, quorum);
        let prep_peers = leader_state.get_preparable_peers(&nodes);
        assert_eq!(prep_peers, nodes);

        let nodes = vec![7, 1, 100, 4, 6];
        let quorum = Quorum::Majority(3);
        let max_pid = 100;
        let leader_state =
            LeaderState::<Value>::with(Ballot::with(1, 1, 1, max_pid), max_pid as usize, quorum);
        let prep_peers = leader_state.get_preparable_peers(&nodes);
        assert_eq!(prep_peers, nodes);
    }

    fn leader_state_with_quorum(quorum: Quorum, max_pid: usize) -> LeaderState<()> {
        LeaderState::<()>::with(Ballot::with(1, 1, 1, max_pid as NodeId), max_pid, quorum)
    }

    #[test]
    fn follower_sync_point_is_monotonic() {
        let mut leader_state = leader_state_with_quorum(Quorum::Majority(3), 5);
        leader_state.set_accepted_idx(2, 10);
        leader_state.set_accepted_idx(2, 7);
        leader_state.set_accepted_idx(2, 12);

        // 5-node cluster => majority write quorum is 3 => need 2 followers; missing followers count as 0.
        assert_eq!(leader_state.get_commit_idx(), 0);
        leader_state.set_accepted_idx(3, 11);
        assert_eq!(leader_state.get_commit_idx(), 11);
    }

    #[test]
    fn commit_idx_uses_kth_largest_follower_sync_for_majority() {
        let mut leader_state = leader_state_with_quorum(Quorum::Majority(3), 5);
        leader_state.set_accepted_idx(2, 10);
        leader_state.set_accepted_idx(3, 7);
        leader_state.set_accepted_idx(4, 3);

        // Need 2 followers in addition to leader. 2nd largest follower sync is 7.
        assert_eq!(leader_state.get_commit_idx(), 7);
    }

    #[test]
    fn commit_idx_uses_write_quorum_for_flexible_quorum() {
        let quorum = Quorum::Flexible(FlexibleQuorum {
            read_quorum_size: 4,
            write_quorum_size: 2,
        });
        let mut leader_state = leader_state_with_quorum(quorum, 5);
        leader_state.set_accepted_idx(2, 4);
        leader_state.set_accepted_idx(3, 9);

        // Write quorum is 2 total => only 1 follower is needed; commit should be max follower sync.
        assert_eq!(leader_state.get_commit_idx(), 9);
    }

    #[test]
    fn commit_idx_treats_missing_followers_as_zero() {
        let mut leader_state = leader_state_with_quorum(Quorum::Majority(3), 5);
        leader_state.set_accepted_idx(2, 8);

        // Need 2 followers, but only one has reported. Missing followers are padded as 0.
        assert_eq!(leader_state.get_commit_idx(), 0);
    }

    #[test]
    fn commit_idx_majority_with_one_leader_and_eight_followers() {
        // 9 nodes total => majority write quorum = 5 => need 4 followers in addition to leader.
        let mut leader_state = leader_state_with_quorum(Quorum::Majority(5), 9);

        leader_state.set_accepted_idx(2, 12);
        leader_state.set_accepted_idx(3, 8);
        leader_state.set_accepted_idx(4, 11);
        leader_state.set_accepted_idx(5, 5);
        leader_state.set_accepted_idx(6, 9);
        leader_state.set_accepted_idx(7, 3);
        leader_state.set_accepted_idx(8, 1);
        leader_state.set_accepted_idx(9, 10);

        // Sorted desc: [12,11,10,9,8,5,3,1], 4th largest = 9
        assert_eq!(leader_state.get_commit_idx(), 9);
    }

    #[test]
    fn commit_idx_majority_with_partial_reports_in_large_cluster() {
        // 9 nodes total => majority write quorum = 5 => need 4 followers.
        let mut leader_state = leader_state_with_quorum(Quorum::Majority(5), 9);

        leader_state.set_accepted_idx(2, 6);
        leader_state.set_accepted_idx(3, 6);
        leader_state.set_accepted_idx(4, 6);
        // Only 3 followers reported; one more follower is effectively padded as 0.
        assert_eq!(leader_state.get_commit_idx(), 0);

        leader_state.set_accepted_idx(5, 4);
        // Now 4 followers reported => 4th largest among followers is 4.
        assert_eq!(leader_state.get_commit_idx(), 4);
    }

    #[test]
    fn commit_idx_large_cluster_ignores_decreasing_updates_per_follower() {
        // 9 nodes total => majority write quorum = 5 => need 4 followers.
        let mut leader_state = leader_state_with_quorum(Quorum::Majority(5), 9);

        leader_state.set_accepted_idx(2, 15);
        leader_state.set_accepted_idx(3, 14);
        leader_state.set_accepted_idx(4, 13);
        leader_state.set_accepted_idx(5, 12);
        assert_eq!(leader_state.get_commit_idx(), 12);

        // Decreasing update for follower 5 should be ignored.
        leader_state.set_accepted_idx(5, 2);
        assert_eq!(leader_state.get_commit_idx(), 12);
    }

    /// Helper: given a quorum, the expected super-quorum threshold, and a
    /// few values below/at/above that threshold, assert correctness.
    fn assert_super_quorum(quorum: Quorum, expected_threshold: usize, max_nodes: usize) {
        for n in 0..expected_threshold {
            assert!(
                !quorum.is_super_quorum(n),
                "expected is_super_quorum({n}) = false for threshold {expected_threshold}"
            );
        }
        for n in expected_threshold..=max_nodes {
            assert!(
                quorum.is_super_quorum(n),
                "expected is_super_quorum({n}) = true for threshold {expected_threshold}"
            );
        }
    }

    #[test]
    fn is_super_quorum_majority() {
        // (majority, expected super-quorum size, total nodes to check up to)
        // super_quorum = ceil(f/2) + f + 1 where f = majority - 1
        let cases: Vec<(usize, usize, usize)> = vec![
            (2, 3, 3), // 3 nodes:  f=1, super=3
            (3, 4, 5), // 5 nodes:  f=2, super=4
            (4, 6, 7), // 7 nodes:  f=3, super=6
            (5, 7, 9), // 9 nodes:  f=4, super=7
        ];
        for (majority, threshold, total) in cases {
            assert_super_quorum(Quorum::Majority(majority), threshold, total);
        }
    }

    #[test]
    fn is_super_quorum_flexible() {
        // (write_quorum_size, expected super-quorum threshold, max_nodes to check)
        let cases: Vec<(usize, usize, usize)> = vec![
            (2, 3, 5), // f=1, (1+1)/2+1+1 = 3
            (4, 6, 7), // f=3, (3+1)/2+3+1 = 6
        ];
        for (wq, threshold, max_n) in cases {
            let quorum = Quorum::Flexible(FlexibleQuorum {
                read_quorum_size: 1, // not relevant for super quorum
                write_quorum_size: wq,
            });
            assert_super_quorum(quorum, threshold, max_n);
        }
    }
}
