use super::{ballot_leader_election::Ballot, messages::sequence_paxos::*, util::LeaderState};
#[cfg(feature = "logging")]
use crate::utils::logger::create_logger;
use crate::{
    clock::Clock,
    messages::{Message, Timestamp},
    storage::{
        internal_storage::{InternalStorage, InternalStorageConfig},
        Entry, Snapshot, StopSign, Storage,
    },
    util::{
        FlexibleQuorum, LogSync, NodeId, Quorum, SequenceNumber, READ_ERROR_MSG, WRITE_ERROR_MSG,
    },
    ClusterConfig, CompactionErr, OmniPaxosConfig, ProposeErr,
};
use crate::{clock::ClockConfig, util::LogEntry};
#[cfg(feature = "logging")]
use slog::{debug, info, trace, warn, Logger};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, HashMap},
    fmt::Debug,
    vec,
};

pub mod follower;
pub mod leader;

/// Statistics for Nezha fast path and slow path commits.
#[derive(Clone, Debug, Default)]
pub struct NezhaStats {
    /// Number of requests committed via the fast path
    pub fast_path_commits: u64,
    /// Number of requests committed via the slow path
    pub slow_path_commits: u64,
}

/// Information about observed half-RTT samples coming from CommitStatus messages
#[derive(Clone, Copy, Debug, Default)]
pub struct HalfRttStats {
    /// Mean (microseconds) across the collected half-RTT samples
    pub mean: f64,
    /// Median across the collected half-RTT samples
    pub median: f64,
    /// Sample standard deviation (microseconds) across the collected half-RTT samples
    pub std_dev: f64,
}

const HALF_RTT_STATS_INTERVAL_US: Timestamp = 5_000_000;

/// a Sequence Paxos replica. Maintains local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
/// If snapshots are not desired to be used, use `()` for the type parameter `S`.
pub(crate) struct SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    pub(crate) internal_storage: InternalStorage<B, T>,
    pid: NodeId,
    peers: Vec<NodeId>, // excluding self pid
    state: (Role, Phase),
    buffered_proposals: Vec<T>,
    buffered_stopsign: Option<StopSign>,
    outgoing: Vec<Message<T>>,
    leader_state: LeaderState<T>,
    latest_accepted_meta: Option<(Ballot, usize)>,
    // Keeps track of sequence of accepts from leader where AcceptSync = 1
    current_seq_num: SequenceNumber,
    cached_promise_message: Option<Promise<T>>,
    // Nezha attributes
    clock: Clock,
    last_released_deadline: u64, // TODO: use correct type for clock simulator
    late_buffer: BTreeMap<(u64, RequestId), PrepareWithDeadline<T>>,
    early_buffer: BinaryHeap<Reverse<PrepareWithDeadline<T>>>,
    reply_set: HashMap<RequestId, (HashMap<NodeId, NezhaReply>, Option<(NodeId, usize)>)>, // Map<RequestId, (Map<NodeId, NezhaReply>, Optional (Leader NodeId, commit_idx) that sent FastReply)>
    committed: HashMap<RequestId, bool>,
    committed_idx: usize,
    nezha_stats: NezhaStats,
    half_rtt_samples: Vec<f64>,
    last_half_rtt_report: Timestamp,
    latest_half_rtt_stats: Option<HalfRttStats>,
    #[cfg(feature = "logging")]
    logger: Logger,
}

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /*** User functions ***/
    /// Creates a Sequence Paxos replica.
    pub(crate) fn with(config: SequencePaxosConfig, storage: B) -> Self {
        let pid = config.pid;
        let peers = config.peers;
        let num_nodes = &peers.len() + 1;
        let quorum = Quorum::with(config.flexible_quorum, num_nodes);
        let max_peer_pid = peers.iter().max().unwrap();
        let max_pid = *std::cmp::max(max_peer_pid, &pid) as usize;
        let mut outgoing = Vec::with_capacity(config.buffer_size);
        let (state, leader) = match storage
            .get_promise()
            .expect("storage error while trying to read promise")
        {
            // if we recover a promise from storage then we must do failure recovery
            Some(b) => {
                let state = (Role::Follower, Phase::Recover);
                for peer_pid in &peers {
                    let prepreq = PrepareReq { n: b };
                    outgoing.push(Message::SequencePaxos(PaxosMessage {
                        from: pid,
                        to: *peer_pid,
                        msg: PaxosMsg::PrepareReq(prepreq),
                    }));
                }
                (state, b)
            }
            None => ((Role::Follower, Phase::None), Ballot::default()),
        };
        let internal_storage_config = InternalStorageConfig {
            batch_size: config.batch_size,
        };
        let mut stats_clock = Clock::new(0,0,0);
        let last_half_rtt_report = stats_clock.now_us();
        let clock_config = ClockConfig {
            clock_uncertainty: config.clock_uncertainty,
            clock_drift: config.clock_drift,
            clock_sync_interval: config.clock_sync_interval,
        };
        let mut paxos = SequencePaxos {
            internal_storage: InternalStorage::with(
                storage,
                internal_storage_config,
                #[cfg(feature = "unicache")]
                pid,
            ),
            pid,
            peers,
            state,
            buffered_proposals: vec![],
            buffered_stopsign: None,
            outgoing,
            leader_state: LeaderState::<T>::with(leader, max_pid, quorum),
            latest_accepted_meta: None,
            current_seq_num: SequenceNumber::default(),
            cached_promise_message: None,
            clock: Clock::with(clock_config),
            early_buffer: BinaryHeap::new(),
            last_released_deadline: 0,
            late_buffer: BTreeMap::new(),
            reply_set: HashMap::new(),
            committed: HashMap::new(),
            committed_idx: 0,
            nezha_stats: NezhaStats::default(),
            half_rtt_samples: Vec::new(),
            last_half_rtt_report,
            latest_half_rtt_stats: None,
            #[cfg(feature = "logging")]
            logger: {
                if let Some(logger) = config.custom_logger {
                    logger
                } else {
                    let s = config
                        .logger_file_path
                        .unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
                    create_logger(s.as_str(), slog::Level::Trace)
                }
            },
        };
        paxos
            .internal_storage
            .set_promise(leader)
            .expect(WRITE_ERROR_MSG);
        #[cfg(feature = "logging")]
        {
            info!(paxos.logger, "Paxos component pid: {} created!", pid);
            if let Quorum::Flexible(flex_quorum) = quorum {
                if flex_quorum.read_quorum_size > num_nodes - flex_quorum.write_quorum_size + 1 {
                    warn!(
                        paxos.logger,
                        "Unnecessary overlaps in read and write quorums. Read and Write quorums only need to be overlapping by one node i.e., read_quorum_size + write_quorum_size = num_nodes + 1");
                }
            }
        }
        paxos
    }

    pub(crate) fn get_state(&self) -> &(Role, Phase) {
        &self.state
    }

    pub(crate) fn get_nezha_stats(&self) -> NezhaStats {
        self.nezha_stats.clone()
    }

    pub(crate) fn get_promise(&self) -> Ballot {
        self.internal_storage.get_promise()
    }

    fn record_half_rtt_sample(&mut self, half_rtt_us: f64) {
        self.half_rtt_samples.push(half_rtt_us);
        let now = self.clock.now_us();
        self.refresh_half_rtt_stats_if_due(now);
    }

    fn refresh_half_rtt_stats_if_due(&mut self, now: Timestamp) {
        if now.saturating_sub(self.last_half_rtt_report) < HALF_RTT_STATS_INTERVAL_US {
            return;
        }

        self.latest_half_rtt_stats = HalfRttStats::compute_stats(&self.half_rtt_samples);
        self.last_half_rtt_report = now;

        #[cfg(feature = "logging")]
        if let Some(stats) = self.latest_half_rtt_stats {
            info!(
                self.logger,
                "Half-RTT stats updated";
                "mean_us" => stats.mean,
                "median_us" => stats.median,
                "std_dev_us" => stats.std_dev,
                "samples" => self.half_rtt_samples.len()
            );
        }
    }

    /// Initiates the trim process.
    /// # Arguments
    /// * `trim_idx` - Deletes all entries up to [`trim_idx`], if the [`trim_idx`] is `None` then the minimum index accepted by **ALL** servers will be used as the [`trim_idx`].
    pub(crate) fn trim(&mut self, trim_idx: Option<usize>) -> Result<(), CompactionErr> {
        match self.state {
            (Role::Leader, _) => {
                let min_all_accepted_idx = self.leader_state.get_min_all_accepted_idx();
                let trimmed_idx = match trim_idx {
                    Some(idx) if idx <= *min_all_accepted_idx => idx,
                    None => {
                        #[cfg(feature = "logging")]
                        trace!(
                            self.logger,
                            "No trim index provided, using min_las_idx: {:?}",
                            min_all_accepted_idx
                        );
                        *min_all_accepted_idx
                    }
                    _ => {
                        return Err(CompactionErr::NotAllDecided(*min_all_accepted_idx));
                    }
                };
                let result = self.internal_storage.try_trim(trimmed_idx);
                if result.is_ok() {
                    for pid in &self.peers {
                        let msg = PaxosMsg::Compaction(Compaction::Trim(trimmed_idx));
                        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                            from: self.pid,
                            to: *pid,
                            msg,
                        }));
                    }
                }
                result.map_err(|e| {
                    *e.downcast()
                        .expect("storage error while trying to trim log")
                })
            }
            _ => Err(CompactionErr::NotCurrentLeader(self.get_current_leader())),
        }
    }

    /// Trim the log and create a snapshot. ** Note: only up to the `decided_idx` can be snapshotted **
    /// # Arguments
    /// `idx` - Snapshots all entries with index < [`idx`], if the [`idx`] is None then the decided index will be used.
    /// `local_only` - If `true`, only this server snapshots the log. If `false` all servers performs the snapshot.
    pub(crate) fn snapshot(
        &mut self,
        idx: Option<usize>,
        local_only: bool,
    ) -> Result<(), CompactionErr> {
        let result = self.internal_storage.try_snapshot(idx);
        if !local_only && result.is_ok() {
            // since it is decided, it is ok even for a follower to send this
            for pid in &self.peers {
                let msg = PaxosMsg::Compaction(Compaction::Snapshot(idx));
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: *pid,
                    msg,
                }));
            }
        }
        result.map_err(|e| {
            *e.downcast()
                .expect("storage error while trying to snapshot log")
        })
    }

    /// Return the decided index.
    pub(crate) fn get_decided_idx(&self) -> usize {
        self.internal_storage.get_decided_idx()
    }

    /// Return the committed index
    pub(crate) fn get_committed_idx(&self) -> usize {
        // if the committed index is 0 or less than decided idx, return the decided idx since it's more correct
        if self.committed_idx == 0 || self.committed_idx < self.internal_storage.get_decided_idx() {
            self.internal_storage.get_decided_idx()
        } else {
            self.committed_idx
        }
    }

    /// Sets the committed index to `idx` if `idx` is greater than the current committed index.
    /// This should only be called by the leader when it determines that a new commit point has been reached.
    pub(crate) fn set_committed_idx(&mut self, idx: usize) {
        self.committed_idx = self.committed_idx.max(idx);
    }

    /// Return trim index from storage.
    pub(crate) fn get_compacted_idx(&self) -> usize {
        self.internal_storage.get_compacted_idx()
    }

    pub(crate) fn read_committed_suffix(&self, from_idx: usize) -> Option<Vec<LogEntry<T>>> {
        let committed_idx = self.get_committed_idx();
        if from_idx >= committed_idx {
            #[cfg(feature = "logging")]
            slog::error!(
                self.logger,
                "read_committed_suffix returned None";
                "from_idx" => from_idx,
                "committed_idx" => committed_idx,
                "reason" => "from_idx_out_of_bounds"
            );
            return None;
        }

        let committed_suffix = self
            .internal_storage
            .read(from_idx..committed_idx)
            .expect("storage error while trying to read committed log suffix");

        if committed_suffix.is_none() {
            #[cfg(feature = "logging")]
            slog::error!(
                self.logger,
                "read_committed_suffix returned None";
                "from_idx" => from_idx,
                "committed_idx" => committed_idx,
                "reason" => "storage_read_returned_none"
            );
        }

        committed_suffix
    }

    fn handle_compaction(&mut self, c: Compaction) {
        // try trimming and snapshotting forwarded compaction. Errors are ignored as that the data will still be kept.
        match c {
            Compaction::Trim(idx) => {
                let _ = self.internal_storage.try_trim(idx);
            }
            Compaction::Snapshot(idx) => {
                let _ = self.snapshot(idx, true);
            }
        }
    }

    /// Detects if a Prepare, Promise, AcceptStopSign, Decide of a Stopsign, or PrepareReq message
    /// has been sent but not been received. If so resends them. Note: We can't detect if a
    /// StopSign's Decide message has been received so we always resend to be safe.
    pub(crate) fn resend_message_timeout(&mut self) {
        match self.state.0 {
            Role::Leader => self.resend_messages_leader(),
            Role::Follower => self.resend_messages_follower(),
        }
    }

    /// Flushes any batched log entries and sends their corresponding Accept or Accepted messages.
    pub(crate) fn flush_batch_timeout(&mut self) {
        match self.state {
            (Role::Leader, Phase::Accept) => self.flush_batch_leader(),
            (Role::Follower, Phase::Accept) => self.flush_batch_follower(),
            _ => (),
        }
    }

    /// Moves the outgoing messages from this replica into the buffer. The messages should then be sent via the network implementation.
    /// If `buffer` is empty, it gets swapped with the internal message buffer. Otherwise, messages are appended to the buffer. This prevents messages from getting discarded.
    /// the buffer.
    pub(crate) fn take_outgoing_msgs(&mut self, buffer: &mut Vec<Message<T>>) {
        if buffer.is_empty() {
            std::mem::swap(buffer, &mut self.outgoing);
        } else {
            // User has unsent messages in their buffer, must extend their buffer.
            buffer.append(&mut self.outgoing);
        }
        self.leader_state.reset_latest_accept_meta();
        self.latest_accepted_meta = None;
    }

    /// Handle an incoming message.
    pub(crate) fn handle(&mut self, m: PaxosMessage<T>) {
        match m.msg {
            PaxosMsg::PrepareReq(prepreq) => self.handle_preparereq(prepreq, m.from),
            PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
            PaxosMsg::Promise(prom) => match &self.state {
                (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                _ => {}
            },
            PaxosMsg::AcceptSync(acc_sync) => self.handle_acceptsync(acc_sync, m.from),
            PaxosMsg::AcceptDecide(acc) => self.handle_acceptdecide(acc),
            PaxosMsg::NotAccepted(not_acc) => self.handle_notaccepted(not_acc, m.from),
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::Decide(d) => self.handle_decide(d),
            PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
            PaxosMsg::Compaction(c) => self.handle_compaction(c),
            PaxosMsg::AcceptStopSign(acc_ss) => self.handle_accept_stopsign(acc_ss),
            PaxosMsg::ForwardStopSign(f_ss) => self.handle_forwarded_stopsign(f_ss),
            PaxosMsg::PrepareWithDeadline(prep) => self.handle_prepare_with_deadline(prep),
            PaxosMsg::FastReply(freply) => self.handle_fast_reply(freply, m.from),
            PaxosMsg::SlowReply(_sreply) => self.handle_slow_reply(_sreply, m.from),
            PaxosMsg::LogModifications(lm) => self.handle_log_modifications(lm),
            PaxosMsg::LogStatus(ls) => self.handle_log_status(ls, m.from),
            PaxosMsg::CommitStatus(cs) => self.handle_commit_status(cs),
        }
    }

    /// Returns whether this Sequence Paxos has been reconfigured
    pub(crate) fn is_reconfigured(&self) -> Option<StopSign> {
        match self.internal_storage.get_stopsign() {
            Some(ss) if self.internal_storage.stopsign_is_decided() => Some(ss),
            _ => None,
        }
    }

    /// Returns whether this Sequence Paxos instance is stopped, i.e. if it has been reconfigured.
    fn accepted_reconfiguration(&self) -> bool {
        self.internal_storage.get_stopsign().is_some()
    }

    /// Append an entry to the replicated log.
    pub(crate) fn append(&mut self, entry: T) -> Result<(), ProposeErr<T>> {
        if self.accepted_reconfiguration() {
            Err(ProposeErr::PendingReconfigEntry(entry))
        } else {
            self.propose_entry(entry);
            Ok(())
        }
    }

    pub(crate) fn handle_prepare_with_deadline(&mut self, prep: PrepareWithDeadline<T>) {
        // TODO:
        //   if Leader
        //     if Prepare: put in late buffer
        //     if Accept: put either in early or late buffer
        //  if Follower
        //    if Prepare: put in late buffer + forward to leader to put in late buffer (initate slow path)
        //    if Accept: put in early or late buffer
        if prep.entry.get_deadline() > self.last_released_deadline {
            #[cfg(feature = "logging")]
            trace!(self.logger, "PrepareWithDeadline buffered in early_buffer"; "request_id" => ?prep.entry.get_request_id(), "deadline" => prep.entry.get_deadline());
            self.early_buffer.push(Reverse(prep));
        } else {
            #[cfg(feature = "logging")]
            trace!(self.logger, "PrepareWithDeadline buffered in late_buffer"; "request_id" => ?prep.entry.get_request_id(), "deadline" => prep.entry.get_deadline());
            let deadline = prep.entry.get_deadline();
            let request_id = prep.entry.get_request_id();
            self.late_buffer.insert((deadline, request_id), prep);
        }
    }

    pub(crate) fn process_early_buffer(&mut self) {
        // If not in Accept phase, don't process early buffer
        if self.state.1 != Phase::Accept {
            return;
        }
        if !self.late_buffer.is_empty() && self.state.0 == Role::Leader {
            // Transfer requests from the late buffer to the early buffer
            for (i, ((_deadline, _request_id), mut prep)) in std::mem::take(&mut self.late_buffer)
                .into_iter()
                .enumerate()
            {
                // Modify the deadline to be eligible for early buffer
                // Each deadline will be different by 1us (not necessary, avoids having the same deadlines in the log)
                prep.entry
                    .set_deadline(self.last_released_deadline + 1 + i as u64);
                self.early_buffer.push(Reverse(prep));
            }
        }
        while let Some(Reverse(prep)) = self.early_buffer.peek().cloned() {
            if prep.entry.get_deadline() < self.clock.now_us() + self.clock.uncertainty_us() {
                self.early_buffer.pop();
                self.last_released_deadline = prep.entry.get_deadline();

                let is_leader = self.state.0 == Role::Leader;

                // Only leader increments accepted_idx when appending entry
                let inserted_index = self
                    .internal_storage
                    .append_entries_without_batching(vec![prep.entry.clone()], is_leader)
                    .expect(WRITE_ERROR_MSG);

                let is_leader = self.state.0 == Role::Leader;
                let freply = FastReply {
                    request_id: prep.entry.get_request_id(),
                    log_hash: self.internal_storage.get_hash().expect(READ_ERROR_MSG),
                    n: self.internal_storage.get_promise(),
                    is_leader: is_leader,
                    log_idx: if is_leader {
                        Some(inserted_index)
                    } else {
                        None
                    },
                };

                #[cfg(feature = "logging")]
                debug!(self.logger, "Processed entry from early_buffer"; "request_id" => ?prep.entry.get_request_id(), "inserted_index" => inserted_index, "is_leader" => self.state.0 == Role::Leader);

                if is_leader {
                    self.broadcast_log_modifications();
                }

                // If this server was the original receiver of this entry, add its FastReply to reply_set since it will be the one keeping track
                // of replies for this request
                if prep.from == self.pid {
                    self.handle_fast_reply(freply, self.pid);
                } else {
                    // Otherwise, add FastReply to outgoing buffer to be sent to original receiver of this entry
                    self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                        from: self.pid,
                        to: prep.from,
                        msg: PaxosMsg::FastReply(freply),
                    }));
                }
            } else {
                break;
            }
        }
    }

    pub(crate) fn handle_fast_reply(&mut self, freply: FastReply, from: NodeId) {
        // If phase is not Accept, or reply is from a previous ballot, or if we have already received a reply from this node for this request, ignore
        if self.state.1 != Phase::Accept
            || freply.n < self.internal_storage.get_promise()
            || self
                .reply_set
                .get(&freply.request_id)
                .is_some_and(|(replies, _)| replies.contains_key(&from))
        {
            #[cfg(feature = "logging")]
            trace!(self.logger, "Ignoring FastReply"; "from" => from, "request_id" => ?freply.request_id, "ballot" => ?freply.n);
            return;
        }

        let request_id = freply.request_id;
        let entry = self
            .reply_set
            .entry(request_id)
            .or_insert_with(|| (HashMap::new(), None));
        if freply.is_leader {
            // inserting both the leader's pid and the commit point that the leader sent in the FastReply
            let committed_idx = freply.log_idx.unwrap_or(0);
            entry.1 = Some((from, committed_idx));
        }
        entry.0.insert(from, NezhaReply::Fast(freply));
        #[cfg(feature = "logging")]
        trace!(self.logger, "FastReply recorded"; "from" => from, "request_id" => ?request_id, "is_leader" => entry.1.is_some());

        let is_committed = self.check_committed(request_id);
        if is_committed {
            #[cfg(feature = "logging")]
            debug!(self.logger, "Request committed via fast path"; "request_id" => ?request_id);
            self.committed.insert(request_id, true);
            self.nezha_stats.fast_path_commits += 1;

            // find the commit idx for that request id in reply_set and then remove request_id from reply_set
            if let Some((_replies, leader_meta)) = self.reply_set.remove(&request_id) {
                if let Some((_leader_id, commit_point)) = leader_meta {
                    self.set_committed_idx(commit_point);
                }
            }
        }
    }

    pub(crate) fn handle_slow_reply(&mut self, sreply: SlowReply, from: NodeId) {
        if self.state.1 != Phase::Accept
            || self
                .reply_set
                .get(&sreply.request_id)
                .is_some_and(|(replies, _)| replies.contains_key(&from))
        {
            #[cfg(feature = "logging")]
            trace!(self.logger, "Ignoring SlowReply"; "from" => from, "request_id" => ?sreply.request_id, "ballot" => ?sreply.n);
            return;
        }

        let request_id = sreply.request_id;
        let entry = self
            .reply_set
            .entry(request_id)
            .or_insert_with(|| (HashMap::new(), None));

        entry.0.insert(from, NezhaReply::Slow(sreply));
        #[cfg(feature = "logging")]
        trace!(self.logger, "SlowReply recorded"; "from" => from, "request_id" => ?request_id);

        let is_committed = self.check_committed(request_id);
        if is_committed {
            #[cfg(feature = "logging")]
            debug!(self.logger, "Request committed via slow path"; "request_id" => ?request_id);
            self.committed.insert(request_id, true);
            self.nezha_stats.slow_path_commits += 1;

            // find the log_idx for that request_id in reply_set and set it as the committed idx
            if let Some((_replies, leader_meta)) = self.reply_set.get(&request_id) {
                if let Some((_leader_id, commit_point)) = *leader_meta {
                    self.set_committed_idx(commit_point);
                }
            }
            // TODO: figure out a way to remove it from reply_set without it being reinserted back
        }
    }

    fn check_committed(&self, request_id: RequestId) -> bool {
        // Get replies mapping for this request id
        let (replies, leader_meta_opt) = match self.reply_set.get(&request_id) {
            Some((replies, leader_pid_opt)) => (replies, leader_pid_opt),
            None => return false,
        };

        // Get leader's reply if it exists (it is always a FastReply), otherwise return false as leader's reply is necessary to determine if request is committed
        let leader_pid = match leader_meta_opt {
            Some((pid, _commit_idx)) => *pid,
            None => return false,
        };
        let leader_reply = match replies.get(&leader_pid) {
            Some(NezhaReply::Fast(f)) => f,
            _ => return false,
        };

        // Count the number of fast and slow replies
        let mut slow_reply_num = 0;
        let mut fast_reply_num = 0;
        for reply in replies.values() {
            match reply {
                NezhaReply::Slow(_) => {
                    // Slow reply counts as a fast reply since follower's log is guaranteed to be up to date with the leader
                    slow_reply_num += 1;
                    fast_reply_num += 1;
                }
                // Fast reply only counts if it has the same log hash as the leader
                NezhaReply::Fast(f) if f.log_hash == leader_reply.log_hash => {
                    fast_reply_num += 1;
                }
                _ => {}
            }
        }

        // Request is committed if it has either a super quorum of fast replies or an accept quorum of slow replies
        let committed = self.leader_state.quorum.is_super_quorum(fast_reply_num)
            || self.leader_state.quorum.is_accept_quorum(slow_reply_num);

        #[cfg(feature = "logging")]
        if committed {
            debug!(
                self.logger,
                "Request {:?} is committed with {} fast replies and {} slow replies",
                request_id,
                fast_reply_num,
                slow_reply_num
            );
        }

        committed
    }

    /// Propose a reconfiguration. Returns an error if already stopped or `new_config` is invalid.
    /// `new_config` defines the cluster-wide configuration settings for the next cluster.
    /// `metadata` is optional data to commit alongside the reconfiguration.
    pub(crate) fn reconfigure(
        &mut self,
        new_config: ClusterConfig,
        metadata: Option<Vec<u8>>,
    ) -> Result<(), ProposeErr<T>> {
        if self.accepted_reconfiguration() {
            return Err(ProposeErr::PendingReconfigConfig(new_config, metadata));
        }
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "Accepting reconfiguration {:?}", new_config.nodes
        );
        let ss = StopSign::with(new_config, metadata);
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_stopsign = Some(ss),
            (Role::Leader, Phase::Accept) => self.accept_stopsign_leader(ss),
            _ => self.forward_stopsign(ss),
        }
        Ok(())
    }

    fn get_current_leader(&self) -> NodeId {
        self.get_promise().pid
    }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub(crate) fn reconnected(&mut self, pid: NodeId) {
        if pid == self.pid {
            return;
        } else if pid == self.get_current_leader() {
            self.state = (Role::Follower, Phase::Recover);
        }
        let prepreq = PrepareReq {
            n: self.get_promise(),
        };
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to: pid,
            msg: PaxosMsg::PrepareReq(prepreq),
        }));
    }

    fn propose_entry(&mut self, mut entry: T) {
        // TODO: Currently using a constant 100 microsecond addition to deadline. For bonus task, calculate max of OWDs values from receivers and augment with standard deviation (see paper)
        entry.set_deadline(self.clock.now_us() + 100);
        entry.set_request_id(RequestId::new_v4());
        entry.set_nezha_proxy_id(self.pid);

        match self.state {
            // While undergoing leader change, fall back to normal OmniPaxos paths
            (Role::Leader, Phase::Prepare) => self.buffered_proposals.push(entry),
            (Role::Follower, Phase::Prepare) => self.forward_proposals(vec![entry]),

            // Otherwise, follow Nezha path- broadcast PrepareWithDeadline to all peers and process it locally
            _ => {
                let prep = PrepareWithDeadline {
                    from: self.pid,
                    entry: entry.clone(),
                    sent: self.clock.now_us(),
                };

                for peer_pid in &self.peers {
                    self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                        from: self.pid,
                        to: *peer_pid,
                        msg: PaxosMsg::PrepareWithDeadline(prep.clone()),
                    }));
                }
                self.handle_prepare_with_deadline(prep)
            }
        }
    }

    pub(crate) fn get_leader_state(&self) -> &LeaderState<T> {
        &self.leader_state
    }

    pub(crate) fn forward_proposals(&mut self, mut entries: Vec<T>) {
        let leader = self.get_current_leader();
        if leader > 0 && self.pid != leader {
            let pf = PaxosMsg::ProposalForward(entries);
            let msg = Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: leader,
                msg: pf,
            });
            self.outgoing.push(msg);
        } else {
            self.buffered_proposals.append(&mut entries);
        }
    }

    pub(crate) fn forward_stopsign(&mut self, ss: StopSign) {
        let leader = self.get_current_leader();
        if leader > 0 && self.pid != leader {
            #[cfg(feature = "logging")]
            trace!(self.logger, "Forwarding StopSign to Leader {:?}", leader);
            let fs = PaxosMsg::ForwardStopSign(ss);
            let msg = Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: leader,
                msg: fs,
            });
            self.outgoing.push(msg);
        } else if self.buffered_stopsign.as_mut().is_none() {
            self.buffered_stopsign = Some(ss);
        }
    }
    /// Returns `LogSync`, a struct to help other servers synchronize their log to correspond to the
    /// current state of our own log. The `common_prefix_idx` marks where in the log the other server
    /// needs to be sync from.
    fn create_log_sync(
        &self,
        common_prefix_idx: usize,
        other_logs_decided_idx: usize,
    ) -> LogSync<T> {
        let decided_idx = self.internal_storage.get_decided_idx();
        let (decided_snapshot, suffix, sync_idx) =
            if T::Snapshot::use_snapshots() && decided_idx > common_prefix_idx {
                // Note: We snapshot from the other log's decided index and not the common prefix because
                // snapshots currently only work on decided entries.
                let (delta_snapshot, compacted_idx) = self
                    .internal_storage
                    .create_diff_snapshot(other_logs_decided_idx)
                    .expect(READ_ERROR_MSG);
                let suffix = self
                    .internal_storage
                    .get_suffix(decided_idx)
                    .expect(READ_ERROR_MSG);
                (delta_snapshot, suffix, compacted_idx)
            } else {
                let suffix = self
                    .internal_storage
                    .get_suffix(common_prefix_idx)
                    .expect(READ_ERROR_MSG);
                (None, suffix, common_prefix_idx)
            };
        LogSync {
            decided_snapshot,
            suffix,
            sync_idx,
            stopsign: self.internal_storage.get_stopsign(),
        }
    }
}

impl HalfRttStats {
    fn compute_stats(samples: &[f64]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }

        // Calculate the mean for the samples
        let len = samples.len();
        let sum: f64 = samples.iter().copied().sum();
        let mean = sum / len as f64;

        // Compute the median
        let mut sorted = samples.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = if len % 2 == 0 {
            (sorted[len / 2 - 1] + sorted[len / 2]) / 2.0
        } else {
            sorted[len / 2]
        };

        // Calculate the standard deviation
        let mut variance = 0.0;
        if len > 1 {
            variance = samples
                .iter()
                .map(|sample| {
                    let diff = sample - mean;
                    diff * diff
                })
                .sum::<f64>()
                / (len as f64 - 1.0);
        }
        let std_dev = variance.sqrt();

        Some(Self {
            mean,
            median,
            std_dev,
        })
    }
}

#[derive(PartialEq, Debug)]
pub(crate) enum Phase {
    Prepare,
    Accept,
    Recover,
    None,
}

#[derive(PartialEq, Debug)]
pub(crate) enum Role {
    Follower,
    Leader,
}

/// Configuration for `SequencePaxos`.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other servers in the configuration.
/// * `flexible_quorum` : Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `batch_size`: The size of the buffer for log batching. The default is 1, which means no batching.
/// * `clock_uncertainty`: Maximum clock uncertainty in microseconds upon re-synchronization.
/// * `clock_drift`: Clock drift in microseconds per second.
/// * `clock_sync_interval`: Clock synchronization interval in microseconds. The default is 1_000_000 (1 second).
/// * `logger_file_path`: The path where the default logger logs events.
#[derive(Clone, Debug)]
pub(crate) struct SequencePaxosConfig {
    pid: NodeId,
    peers: Vec<NodeId>,
    buffer_size: usize,
    pub(crate) batch_size: usize,
    flexible_quorum: Option<FlexibleQuorum>,
    clock_uncertainty: u64,
    clock_drift: i64,
    clock_sync_interval: u64,
    #[cfg(feature = "logging")]
    logger_file_path: Option<String>,
    #[cfg(feature = "logging")]
    custom_logger: Option<Logger>,
}

impl From<OmniPaxosConfig> for SequencePaxosConfig {
    fn from(config: OmniPaxosConfig) -> Self {
        let pid = config.server_config.pid;
        let peers = config
            .cluster_config
            .nodes
            .into_iter()
            .filter(|x| *x != pid)
            .collect();
        SequencePaxosConfig {
            pid,
            peers,
            flexible_quorum: config.cluster_config.flexible_quorum,
            buffer_size: config.server_config.buffer_size,
            batch_size: config.server_config.batch_size,
            clock_uncertainty: config.server_config.clock_uncertainty,
            clock_drift: config.server_config.clock_drift,
            clock_sync_interval: config.server_config.clock_sync_interval,
            #[cfg(feature = "logging")]
            logger_file_path: config.server_config.logger_file_path,
            #[cfg(feature = "logging")]
            custom_logger: config.server_config.custom_logger,
        }
    }
}

#[cfg(all(test, not(feature = "unicache")))]
mod tests {
    use super::*;
    use crate::ballot_leader_election::Ballot;
    use crate::messages::sequence_paxos::{FastReply, NezhaReply, PrepareWithDeadline, SlowReply};
    use crate::messages::RequestId;
    use crate::storage::{Entry, LogHash, Snapshot};
    use crate::test_storage::TestStorage;
    use crate::util::WRITE_ERROR_MSG;
    use crate::{ClusterConfig, OmniPaxosConfig, ServerConfig};
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    // ── Test helpers ──

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEntry {
        value: u64,
        request_id: RequestId,
        deadline: u64,
        nezha_proxy_id: NodeId,
    }

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct TestSnapshot;

    impl Snapshot<TestEntry> for TestSnapshot {
        fn create(_: &[TestEntry]) -> Self {
            Self
        }
        fn merge(&mut self, _: Self) {}
        fn use_snapshots() -> bool {
            false
        }
    }

    impl Entry for TestEntry {
        type Snapshot = TestSnapshot;

        fn stable_encode(&self, out: &mut Vec<u8>) {
            out.extend_from_slice(&self.value.to_le_bytes());
            out.extend_from_slice(self.request_id.as_bytes());
            out.extend_from_slice(&self.deadline.to_le_bytes());
        }

        fn get_deadline(&self) -> u64 {
            self.deadline
        }

        fn set_deadline(&mut self, deadline: u64) {
            self.deadline = deadline;
        }

        fn get_request_id(&self) -> RequestId {
            self.request_id
        }

        fn set_request_id(&mut self, request_id: RequestId) {
            self.request_id = request_id;
        }

        fn get_nezha_proxy_id(&self) -> NodeId {
            self.nezha_proxy_id
        }

        fn set_nezha_proxy_id(&mut self, node_id: NodeId) {
            self.nezha_proxy_id = node_id;
        }
    }

    impl TestEntry {
        fn new(value: u64, request_id: RequestId, deadline: u64) -> Self {
            Self {
                value,
                request_id,
                deadline,
                nezha_proxy_id: 0,
            }
        }
    }

    /// Create a SequencePaxos instance with the given nodes and this node's pid.
    /// Sets the instance into (role, phase) state and writes a promise for the given ballot.
    fn create_paxos(
        pid: u64,
        nodes: Vec<u64>,
        role: Role,
        phase: Phase,
    ) -> SequencePaxos<TestEntry, TestStorage<TestEntry>> {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes,
            flexible_quorum: None,
        };
        let server_config = ServerConfig {
            pid,
            ..ServerConfig::default()
        };
        let omni_config = OmniPaxosConfig {
            cluster_config,
            server_config,
        };
        let storage = TestStorage::default();
        let mut paxos = SequencePaxos::with(omni_config.into(), storage);
        paxos.state = (role, phase);
        paxos
    }

    /// Create a paxos node in Accept phase with a written promise.
    fn create_accept_paxos(
        pid: u64,
        is_leader: bool,
        nodes: Vec<u64>,
    ) -> SequencePaxos<TestEntry, TestStorage<TestEntry>> {
        let role = if is_leader {
            Role::Leader
        } else {
            Role::Follower
        };
        let mut paxos = create_paxos(pid, nodes, role, Phase::Accept);
        let ballot = Ballot::with(1, 1, 1, 5);
        paxos
            .internal_storage
            .set_promise(ballot)
            .expect(WRITE_ERROR_MSG);
        paxos
    }

    #[test]
    fn prepare_with_deadline_goes_to_early_buffer_when_deadline_above_last_released() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3]);
        let rid = Uuid::new_v4();
        let entry = TestEntry::new(42, rid, 100);
        let prep = PrepareWithDeadline {
            from: 2,
            entry,
            sent: 0,
        };

        paxos.handle_prepare_with_deadline(prep);

        assert_eq!(paxos.early_buffer.len(), 1);
        assert!(paxos.late_buffer.is_empty());
    }

    #[test]
    fn prepare_with_deadline_goes_to_late_buffer_when_deadline_at_or_below_last_released() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3]);
        paxos.last_released_deadline = 50;

        let rid = Uuid::new_v4();
        let entry = TestEntry::new(42, rid, 30); // deadline < last_released
        let prep = PrepareWithDeadline {
            from: 2,
            entry,
            sent: 0,
        };

        paxos.handle_prepare_with_deadline(prep);

        assert!(paxos.early_buffer.is_empty());
        assert_eq!(paxos.late_buffer.len(), 1);
        assert!(paxos.late_buffer.contains_key(&(30, rid)));
    }

    #[test]
    fn prepare_with_deadline_equal_to_last_released_goes_to_late_buffer() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3]);
        paxos.last_released_deadline = 50;

        let rid = Uuid::new_v4();
        let entry = TestEntry::new(42, rid, 50); // deadline == last_released
        let prep = PrepareWithDeadline {
            from: 2,
            entry,
            sent: 0,
        };

        paxos.handle_prepare_with_deadline(prep);

        assert!(paxos.early_buffer.is_empty());
        assert_eq!(paxos.late_buffer.len(), 1);
    }

    #[test]
    fn early_buffer_orders_by_deadline_ascending() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3]);

        let rid1 = Uuid::new_v4();
        let rid2 = Uuid::new_v4();
        let rid3 = Uuid::new_v4();
        let prep1 = PrepareWithDeadline {
            from: 2,
            entry: TestEntry::new(1, rid1, 300),
            sent: 0,
        };
        let prep2 = PrepareWithDeadline {
            from: 2,
            entry: TestEntry::new(2, rid2, 100),
            sent: 0,
        };
        let prep3 = PrepareWithDeadline {
            from: 2,
            entry: TestEntry::new(3, rid3, 200),
            sent: 0,
        };

        paxos.handle_prepare_with_deadline(prep1);
        paxos.handle_prepare_with_deadline(prep2);
        paxos.handle_prepare_with_deadline(prep3);

        assert_eq!(paxos.early_buffer.len(), 3);
        // BinaryHeap<Reverse<...>> should give smallest deadline first
        let top = paxos.early_buffer.peek().unwrap().0.entry.get_deadline();
        assert_eq!(top, 100);
    }

    #[test]
    fn process_early_buffer_does_nothing_when_not_in_accept_phase() {
        let mut paxos = create_paxos(1, vec![1, 2, 3], Role::Follower, Phase::Prepare);
        let prep = PrepareWithDeadline {
            from: 2,
            entry: TestEntry::new(1, Uuid::new_v4(), 0),
            sent: 0,
        };
        paxos.early_buffer.push(Reverse(prep));

        paxos.process_early_buffer();

        // Nothing should be consumed
        assert_eq!(paxos.early_buffer.len(), 1);
    }

    #[test]
    #[ignore]
    fn process_early_buffer_moves_late_buffer_entries_to_early_buffer_for_leader() {
        let mut paxos = create_accept_paxos(1, true, vec![1, 2, 3]);
        paxos.last_released_deadline = 10;

        let rid1 = Uuid::new_v4();
        let rid2 = Uuid::new_v4();
        paxos.late_buffer.insert(
            (5, rid1),
            PrepareWithDeadline {
                from: 2,
                entry: TestEntry::new(1, rid1, 5),
                sent: 0,
            },
        );
        paxos.late_buffer.insert(
            (6, rid2),
            PrepareWithDeadline {
                from: 3,
                entry: TestEntry::new(2, rid2, 6),
                sent: 0,
            },
        );

        paxos.process_early_buffer();

        assert!(paxos.late_buffer.is_empty());
        assert_eq!(paxos.early_buffer.len(), 2);

        let moved_deadlines: Vec<u64> = paxos
            .early_buffer
            .iter()
            .map(|Reverse(prep)| prep.entry.get_deadline())
            .collect();
        assert!(moved_deadlines.iter().all(|deadline| *deadline >= 10));
        assert!(moved_deadlines.iter().any(|deadline| *deadline == 10));
        assert!(moved_deadlines.iter().any(|deadline| *deadline == 11));
    }

    #[test]
    fn process_early_buffer_does_not_move_late_buffer_entries_for_follower() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3]);
        paxos.last_released_deadline = 10;

        let rid = Uuid::new_v4();
        paxos.late_buffer.insert(
            (5, rid),
            PrepareWithDeadline {
                from: 2,
                entry: TestEntry::new(1, rid, 5),
                sent: 0,
            },
        );

        paxos.process_early_buffer();

        assert_eq!(paxos.late_buffer.len(), 1);
        assert!(paxos.late_buffer.contains_key(&(5, rid)));
        assert!(paxos.early_buffer.is_empty());
    }

    #[test]
    fn fast_reply_accumulates_in_reply_set() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        let freply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: false,
            log_idx: None,
        };

        paxos.handle_fast_reply(freply, 2);

        assert!(paxos.reply_set.contains_key(&rid));
        let (replies, leader_opt) = paxos.reply_set.get(&rid).unwrap();
        assert_eq!(replies.len(), 1);
        assert!(replies.contains_key(&2));
        assert!(leader_opt.is_none()); // is_leader was false, so leader_opt should still be None
    }

    #[test]
    fn fast_reply_tracks_leader_pid() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        let freply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: true, // leader reply
            log_idx: Some(1),
        };

        paxos.handle_fast_reply(freply, 3);

        let (_, leader_opt) = paxos.reply_set.get(&rid).unwrap();
        assert_eq!(*leader_opt, Some((3, 1)));
    }

    #[test]
    fn fast_reply_ignores_stale_ballot() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        // Use a ballot lower than the current promise (set to 1 in helper function)
        let stale_ballot = Ballot::default();

        let freply = FastReply {
            n: stale_ballot,
            request_id: rid,
            log_hash,
            is_leader: false,
            log_idx: None,
        };

        paxos.handle_fast_reply(freply, 2);

        assert!(!paxos.reply_set.contains_key(&rid));
    }

    #[test]
    fn fast_reply_ignores_duplicate_from_same_node() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        let freply1 = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: false,
            log_idx: None,
        };
        let freply2 = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: true, // different is_leader to check it's truly ignored
            log_idx: None,
        };

        paxos.handle_fast_reply(freply1, 2);
        paxos.handle_fast_reply(freply2, 2); // duplicate from node 2

        let (replies, leader_opt) = paxos.reply_set.get(&rid).unwrap();
        assert_eq!(replies.len(), 1);
        // leader_opt should still be None since the first reply had is_leader=false
        // and the duplicate was ignored
        assert!(leader_opt.is_none());
    }

    #[test]
    fn fast_reply_ignored_when_not_in_accept_phase() {
        let mut paxos = create_paxos(1, vec![1, 2, 3, 4, 5], Role::Follower, Phase::Prepare);
        let ballot = Ballot::with(1, 1, 1, 5);
        paxos
            .internal_storage
            .set_promise(ballot)
            .expect(WRITE_ERROR_MSG);
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        let freply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: false,
            log_idx: None,
        };

        paxos.handle_fast_reply(freply, 2);

        assert!(!paxos.reply_set.contains_key(&rid));
    }

    #[test]
    fn slow_reply_accumulates_in_reply_set() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();

        let sreply = SlowReply {
            n: ballot,
            request_id: rid,
        };

        paxos.handle_slow_reply(sreply, 2);

        assert!(paxos.reply_set.contains_key(&rid));
        let (replies, leader_opt) = paxos.reply_set.get(&rid).unwrap();
        assert_eq!(replies.len(), 1);
        assert!(matches!(replies.get(&2), Some(NezhaReply::Slow(_))));
        assert!(leader_opt.is_none());
    }

    #[test]
    fn slow_reply_ignores_duplicate_from_same_node() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();

        let sreply1 = SlowReply {
            n: ballot,
            request_id: rid,
        };
        let sreply2 = SlowReply {
            n: ballot,
            request_id: rid,
        };

        paxos.handle_slow_reply(sreply1, 2);
        paxos.handle_slow_reply(sreply2, 2);

        let (replies, _) = paxos.reply_set.get(&rid).unwrap();
        assert_eq!(replies.len(), 1);
        assert!(matches!(replies.get(&2), Some(NezhaReply::Slow(_))));
    }

    #[test]
    fn slow_reply_ignored_when_not_in_accept_phase() {
        let mut paxos = create_paxos(1, vec![1, 2, 3, 4, 5], Role::Follower, Phase::Prepare);
        let ballot = Ballot::with(1, 1, 1, 5);
        paxos
            .internal_storage
            .set_promise(ballot)
            .expect(WRITE_ERROR_MSG);
        let rid = Uuid::new_v4();

        let sreply = SlowReply {
            n: ballot,
            request_id: rid,
        };

        paxos.handle_slow_reply(sreply, 2);

        assert!(!paxos.reply_set.contains_key(&rid));
    }

    #[test]
    fn slow_reply_commits_once_accept_quorum_is_reached_with_leader_reply_present() {
        // 5 nodes: majority/accept quorum = 3. Current implementation counts only SlowReply
        // messages toward the slow-path quorum, so 3 slow replies are needed in addition to
        // the leader FastReply being present.
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        let leader_reply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: true,
            log_idx: None,
        };
        paxos.handle_fast_reply(leader_reply, 2);

        assert!(!paxos.committed.contains_key(&rid));

        paxos.handle_slow_reply(
            SlowReply {
                n: ballot,
                request_id: rid,
            },
            3,
        );
        assert!(!paxos.committed.contains_key(&rid));

        paxos.handle_slow_reply(
            SlowReply {
                n: ballot,
                request_id: rid,
            },
            4,
        );
        assert!(!paxos.committed.contains_key(&rid));

        paxos.handle_slow_reply(
            SlowReply {
                n: ballot,
                request_id: rid,
            },
            5,
        );

        assert!(paxos.committed.contains_key(&rid));
        assert!(*paxos.committed.get(&rid).unwrap());
    }

    #[test]
    fn test_committed_idx_with_fast_reply_from_leader() {
        // 5 nodes: majority/accept quorum = 3. Current implementation counts only SlowReply
        // messages toward the slow-path quorum, so 3 slow replies are needed in addition to
        // the leader FastReply being present.
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);
        assert!(!paxos.committed.contains_key(&rid));

        paxos.handle_slow_reply(
            SlowReply {
                n: ballot,
                request_id: rid,
            },
            3,
        );
        assert!(!paxos.committed.contains_key(&rid));

        paxos.handle_slow_reply(
            SlowReply {
                n: ballot,
                request_id: rid,
            },
            4,
        );
        assert!(!paxos.committed.contains_key(&rid));

        paxos.handle_slow_reply(
            SlowReply {
                n: ballot,
                request_id: rid,
            },
            5,
        );

        let leader_reply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: true,
            log_idx: Some(1),
        };
        paxos.handle_fast_reply(leader_reply, 2);

        assert!(paxos.committed.contains_key(&rid));
        assert!(*paxos.committed.get(&rid).unwrap());
        assert!(paxos.committed_idx == 1);
    }

    #[test]
    fn test_committed_idx_with_slow_reply() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);
        assert!(!paxos.committed.contains_key(&rid));

        paxos.handle_slow_reply(
            SlowReply {
                n: ballot,
                request_id: rid,
            },
            3,
        );
        assert!(!paxos.committed.contains_key(&rid));

        paxos.handle_slow_reply(
            SlowReply {
                n: ballot,
                request_id: rid,
            },
            4,
        );

        let leader_reply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: true,
            log_idx: Some(2),
        };
        paxos.handle_fast_reply(leader_reply, 2);
        assert!(!paxos.committed.contains_key(&rid));

        paxos.handle_slow_reply(
            SlowReply {
                n: ballot,
                request_id: rid,
            },
            5,
        );

        assert!(paxos.committed.contains_key(&rid));
        assert!(*paxos.committed.get(&rid).unwrap());
        assert!(paxos.committed_idx == 2);
    }

    #[test]
    fn test_committed_idx_advances_only_after_full_slow_quorum() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let log_hash = LogHash::compute::<TestEntry>(&[]);
        let rid1 = Uuid::new_v4();
        let rid2 = Uuid::new_v4();

        for pid in [3, 4, 5] {
            paxos.handle_slow_reply(
                SlowReply {
                    n: ballot,
                    request_id: rid1,
                },
                pid,
            );
        }
        paxos.handle_fast_reply(
            FastReply {
                n: ballot,
                request_id: rid1,
                log_hash,
                is_leader: true,
                log_idx: Some(1),
            },
            2,
        );
        // committed_id should be 1 since rid1 is committed, but rid2 is not committed yet
        assert_eq!(paxos.get_committed_idx(), 1);

        // for rid2, send fast_reply from leader (with log_idx 2) and 2 slow replies, but not the full quorum of 3 slow replies yet
        paxos.handle_fast_reply(
            FastReply {
                n: ballot,
                request_id: rid2,
                log_hash,
                is_leader: true,
                log_idx: Some(2),
            },
            2,
        );
        for pid in [3, 4] {
            paxos.handle_slow_reply(
                SlowReply {
                    n: ballot,
                    request_id: rid2,
                },
                pid,
            );
            // committed_id should still be 1 since rid2 is not fully committed yet
            assert_eq!(paxos.get_committed_idx(), 1);
        }

        // reach full quorum for rid2 and check committed_id advances to 2
        paxos.handle_slow_reply(
            SlowReply {
                n: ballot,
                request_id: rid2,
            },
            5,
        );
        assert_eq!(paxos.get_committed_idx(), 2);
    }

    #[test]
    fn test_committed_idx_with_fast_reply_from_follower() {
        // 5 nodes: majority/accept quorum = 3. Current implementation counts only SlowReply
        // messages toward the slow-path quorum, so 3 slow replies are needed in addition to
        // the leader FastReply being present.
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);
        assert!(!paxos.committed.contains_key(&rid));

        paxos.handle_slow_reply(
            SlowReply {
                n: ballot,
                request_id: rid,
            },
            3,
        );
        assert!(!paxos.committed.contains_key(&rid));

        paxos.handle_slow_reply(
            SlowReply {
                n: ballot,
                request_id: rid,
            },
            4,
        );
        assert!(!paxos.committed.contains_key(&rid));

        let leader_reply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: true,
            log_idx: Some(1),
        };
        paxos.handle_fast_reply(leader_reply, 1);

        let follower_reply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: false,
            log_idx: Some(0),
        };
        paxos.handle_fast_reply(follower_reply, 2);

        assert!(paxos.committed.contains_key(&rid));
        assert!(*paxos.committed.get(&rid).unwrap());
        assert!(paxos.committed_idx == 1);
    }

    #[test]
    fn test_committed_idx_stays_monotonic() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);

        paxos.set_committed_idx(5);
        assert_eq!(paxos.get_committed_idx(), 5);

        paxos.set_committed_idx(3);
        assert_eq!(paxos.get_committed_idx(), 5);

        paxos.set_committed_idx(8);
        assert_eq!(paxos.get_committed_idx(), 8);
    }

    #[test]
    fn check_committed_returns_false_without_leader_reply() {
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        // Add replies from followers but none is marked as leader
        for from in [2, 3, 4, 5] {
            let freply = FastReply {
                n: ballot,
                request_id: rid,
                log_hash,
                is_leader: false,
                log_idx: None,
            };
            paxos.handle_fast_reply(freply, from);
        }

        // Should not be committed since no leader reply
        assert!(!paxos.committed.contains_key(&rid));
        assert!(paxos.reply_set.contains_key(&rid));
    }

    #[test]
    fn check_committed_super_quorum_of_matching_fast_replies() {
        // 5 nodes: majority = 3, f = 2, super_quorum = ceil(2/2) + 2 + 1 = 4
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        // Leader reply from node 2
        let leader_reply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: true,
            log_idx: None,
        };
        paxos.handle_fast_reply(leader_reply, 2);

        // Follower replies with matching hash from nodes 3, 4
        for from in [3, 4] {
            let freply = FastReply {
                n: ballot,
                request_id: rid,
                log_hash,
                is_leader: false,
                log_idx: None,
            };
            paxos.handle_fast_reply(freply, from);
        }

        // 3 matching fast replies (nodes 2, 3, 4) + need super quorum of 4- shouldn't be committed yet
        assert!(!paxos.committed.contains_key(&rid));

        // One more matching fast reply from node 5- 4 matching replies = super quorum
        let freply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: false,
            log_idx: None,
        };
        paxos.handle_fast_reply(freply, 5);

        assert!(paxos.committed.contains_key(&rid));
        assert!(*paxos.committed.get(&rid).unwrap());
        // reply_set should be cleaned up once committed
        assert!(!paxos.reply_set.contains_key(&rid));
    }

    #[test]
    fn check_committed_mismatched_hash_does_not_count_as_fast() {
        // 5 nodes: majority = 3, f = 2, super_quorum = 4
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let leader_hash = LogHash::compute(&[TestEntry::new(1, rid, 0)]);
        let different_hash = LogHash::compute(&[TestEntry::new(99, rid, 0)]);

        // Leader reply
        let leader_reply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash: leader_hash,
            is_leader: true,
            log_idx: None,
        };
        paxos.handle_fast_reply(leader_reply, 2);

        // Followers 3, 4 send matching hash
        for from in [3, 4] {
            let freply = FastReply {
                n: ballot,
                request_id: rid,
                log_hash: leader_hash,
                is_leader: false,
                log_idx: None,
            };
            paxos.handle_fast_reply(freply, from);
        }

        // Follower 5 sends different hash- should not count towards fast quorum
        let freply_mismatch = FastReply {
            n: ballot,
            request_id: rid,
            log_hash: different_hash,
            is_leader: false,
            log_idx: None,
        };
        paxos.handle_fast_reply(freply_mismatch, 5);

        // Only 3 matching fast replies (2, 3, 4) < super quorum of 4- not committed
        assert!(!paxos.committed.contains_key(&rid));
    }

    #[test]
    fn check_committed_returns_false_for_unknown_request_id() {
        let paxos = create_accept_paxos(1, false, vec![1, 2, 3]);
        let unknown_rid = Uuid::new_v4();
        assert!(!paxos.check_committed(unknown_rid));
    }

    #[test]
    fn leader_change_clears_reply_set() {
        let mut paxos = create_accept_paxos(1, true, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        // Accumulate some replies
        let freply = FastReply {
            n: ballot,
            request_id: rid,
            log_hash,
            is_leader: true,
            log_idx: None,
        };
        paxos.handle_fast_reply(freply, 1);

        assert!(!paxos.reply_set.is_empty());

        // Simulate a new leader being elected with a higher ballot
        let new_ballot = Ballot::with(2, 2, 2, 5);
        paxos.handle_leader(new_ballot);

        // reply_set should be cleared after leader change
        assert!(paxos.reply_set.is_empty());
    }

    #[test]
    fn slow_replies_count_towards_accept_quorum() {
        // 5 nodes: majority = 3 (accept quorum). Slow replies count for both slow and fast.
        let mut paxos = create_accept_paxos(1, false, vec![1, 2, 3, 4, 5]);
        let ballot = paxos.internal_storage.get_promise();
        let rid = Uuid::new_v4();
        let log_hash = LogHash::compute::<TestEntry>(&[]);

        // Manually populate reply_set with leader FastReply + slow replies
        let mut replies = HashMap::new();
        replies.insert(
            2u64,
            NezhaReply::Fast(FastReply {
                n: ballot,
                request_id: rid,
                log_hash,
                is_leader: true,
                log_idx: None,
            }),
        );
        replies.insert(
            3u64,
            NezhaReply::Slow(SlowReply {
                n: ballot,
                request_id: rid,
            }),
        );
        replies.insert(
            4u64,
            NezhaReply::Slow(SlowReply {
                n: ballot,
                request_id: rid,
            }),
        );
        replies.insert(
            5u64,
            NezhaReply::Slow(SlowReply {
                n: ballot,
                request_id: rid,
            }),
        );
        paxos.reply_set.insert(rid, (replies, Some((2, 1))));

        // 3 slow replies >= accept quorum of 3 → should be committed
        assert!(paxos.check_committed(rid));
    }
}
