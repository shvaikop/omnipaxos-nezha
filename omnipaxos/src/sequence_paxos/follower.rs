use super::super::ballot_leader_election::Ballot;

use super::*;

use crate::util::{MessageStatus, WRITE_ERROR_MSG};

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /*** Follower ***/
    pub(crate) fn handle_prepare(&mut self, prep: Prepare, from: NodeId) {
        let old_promise = self.internal_storage.get_promise();
        if old_promise < prep.n || (old_promise == prep.n && self.state.1 == Phase::Recover) {
            // Flush any pending writes
            // Don't have to handle flushed entries here because we will sync with followers
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage
                .set_promise(prep.n)
                .expect(WRITE_ERROR_MSG);
            self.state = (Role::Follower, Phase::Prepare);
            self.current_seq_num = SequenceNumber::default();
            let na = self.internal_storage.get_accepted_round();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let log_sync = if na > prep.n_accepted {
                // I'm more up to date: send leader what he is missing after his decided index.
                Some(self.create_log_sync(prep.decided_idx, prep.decided_idx))
            } else if na == prep.n_accepted && accepted_idx > prep.accepted_idx {
                // I'm more up to date and in same round: send leader what he is missing after his
                // accepted index.
                Some(self.create_log_sync(prep.accepted_idx, prep.decided_idx))
            } else {
                // I'm equally or less up to date
                None
            };
            let promise = Promise {
                n: prep.n,
                n_accepted: na,
                decided_idx: self.internal_storage.get_decided_idx(),
                accepted_idx,
                log_sync,
            };
            self.cached_promise_message = Some(promise.clone());
            self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Promise(promise),
            }));
        }
    }

    pub(crate) fn handle_acceptsync(&mut self, accsync: AcceptSync<T>, from: NodeId) {
        if self.check_valid_ballot(accsync.n) && self.state == (Role::Follower, Phase::Prepare) {
            self.cached_promise_message = None;
            let new_accepted_idx = self
                .internal_storage
                .sync_log(accsync.n, accsync.decided_idx, Some(accsync.log_sync))
                .expect(WRITE_ERROR_MSG);
            if self.internal_storage.get_stopsign().is_none() {
                self.forward_buffered_proposals();
            }
            let accepted = Accepted {
                n: accsync.n,
                accepted_idx: new_accepted_idx,
            };
            self.state = (Role::Follower, Phase::Accept);
            self.current_seq_num = accsync.seq_num;
            let cached_idx = self.outgoing.len();
            self.latest_accepted_meta = Some((accsync.n, cached_idx));
            self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: from,
                msg: PaxosMsg::Accepted(accepted),
            }));
            #[cfg(feature = "unicache")]
            self.internal_storage.set_unicache(accsync.unicache);
        }
    }

    fn forward_buffered_proposals(&mut self) {
        let proposals = std::mem::take(&mut self.buffered_proposals);
        if !proposals.is_empty() {
            self.forward_proposals(proposals);
        }
    }

    pub(crate) fn handle_acceptdecide(&mut self, acc_dec: AcceptDecide<T>) {
        if self.check_valid_ballot(acc_dec.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_dec.seq_num, acc_dec.n.pid) == MessageStatus::Expected
        {
            #[cfg(not(feature = "unicache"))]
            let entries = acc_dec.entries;
            #[cfg(feature = "unicache")]
            let entries = self.internal_storage.decode_entries(acc_dec.entries);
            let mut new_accepted_idx = self
                .internal_storage
                .append_entries_and_get_accepted_idx(entries)
                .expect(WRITE_ERROR_MSG);
            let flushed_after_decide =
                self.update_decided_idx_and_get_accepted_idx(acc_dec.decided_idx);
            if flushed_after_decide.is_some() {
                new_accepted_idx = flushed_after_decide;
            }
            if let Some(idx) = new_accepted_idx {
                self.reply_accepted(acc_dec.n, idx);
            }
        }
    }

    pub(crate) fn handle_accept_stopsign(&mut self, acc_ss: AcceptStopSign) {
        if self.check_valid_ballot(acc_ss.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc_ss.seq_num, acc_ss.n.pid) == MessageStatus::Expected
        {
            // Flush entries before appending stopsign. The accepted index is ignored here as
            // it will be updated when appending stopsign.
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            let new_accepted_idx = self
                .internal_storage
                .set_stopsign(Some(acc_ss.ss))
                .expect(WRITE_ERROR_MSG);
            self.reply_accepted(acc_ss.n, new_accepted_idx);
        }
    }

    pub(crate) fn handle_decide(&mut self, dec: Decide) {
        if self.check_valid_ballot(dec.n)
            && self.state.1 == Phase::Accept
            && self.handle_sequence_num(dec.seq_num, dec.n.pid) == MessageStatus::Expected
        {
            let new_accepted_idx = self.update_decided_idx_and_get_accepted_idx(dec.decided_idx);
            if let Some(idx) = new_accepted_idx {
                self.reply_accepted(dec.n, idx);
            }
        }
    }

    /// To maintain decided index <= accepted index, batched entries may be flushed.
    /// Returns `Some(new_accepted_idx)` if entries are flushed, otherwise `None`.
    fn update_decided_idx_and_get_accepted_idx(&mut self, new_decided_idx: usize) -> Option<usize> {
        if new_decided_idx <= self.internal_storage.get_decided_idx() {
            return None;
        }
        if new_decided_idx > self.internal_storage.get_accepted_idx() {
            let new_accepted_idx = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage
                .set_decided_idx(new_decided_idx.min(new_accepted_idx))
                .expect(WRITE_ERROR_MSG);
            Some(new_accepted_idx)
        } else {
            self.internal_storage
                .set_decided_idx(new_decided_idx)
                .expect(WRITE_ERROR_MSG);
            None
        }
    }

    fn reply_accepted(&mut self, n: Ballot, accepted_idx: usize) {
        let latest_accepted = self.get_latest_accepted_message(n);
        match latest_accepted {
            Some(acc) => acc.accepted_idx = accepted_idx,
            None => {
                let accepted = Accepted { n, accepted_idx };
                let cached_idx = self.outgoing.len();
                self.latest_accepted_meta = Some((n, cached_idx));
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: n.pid,
                    msg: PaxosMsg::Accepted(accepted),
                }));
            }
        }
    }

    fn get_latest_accepted_message(&mut self, n: Ballot) -> Option<&mut Accepted> {
        if let Some((ballot, outgoing_idx)) = &self.latest_accepted_meta {
            if *ballot == n {
                if let Message::SequencePaxos(PaxosMessage {
                    msg: PaxosMsg::Accepted(a),
                    ..
                }) = self.outgoing.get_mut(*outgoing_idx).unwrap()
                {
                    return Some(a);
                } else {
                    #[cfg(feature = "logging")]
                    debug!(self.logger, "Cached idx is not an Accepted message!");
                }
            }
        }
        None
    }

    /// Also returns whether the message's ballot was promised
    fn check_valid_ballot(&mut self, message_ballot: Ballot) -> bool {
        let my_promise = self.internal_storage.get_promise();
        match my_promise.cmp(&message_ballot) {
            std::cmp::Ordering::Equal => true,
            std::cmp::Ordering::Greater => {
                let not_acc = NotAccepted { n: my_promise };
                #[cfg(feature = "logging")]
                trace!(
                    self.logger,
                    "NotAccepted. My promise: {:?}, theirs: {:?}",
                    my_promise,
                    message_ballot
                );
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: message_ballot.pid,
                    msg: PaxosMsg::NotAccepted(not_acc),
                }));
                false
            }
            std::cmp::Ordering::Less => {
                // Should never happen, but to be safe send PrepareReq
                #[cfg(feature = "logging")]
                warn!(
                    self.logger,
                    "Received non-prepare message from a leader I've never promised. My: {:?}, theirs: {:?}", my_promise, message_ballot
                );
                self.reconnected(message_ballot.pid);
                false
            }
        }
    }

    /// Also returns the MessageStatus of the sequence based on the incoming sequence number.
    fn handle_sequence_num(&mut self, seq_num: SequenceNumber, from: NodeId) -> MessageStatus {
        let msg_status = self.current_seq_num.check_msg_status(seq_num);
        match msg_status {
            MessageStatus::Expected => self.current_seq_num = seq_num,
            MessageStatus::DroppedPreceding => self.reconnected(from),
            MessageStatus::Outdated => (),
        };
        msg_status
    }

    pub(crate) fn resend_messages_follower(&mut self) {
        match self.state.1 {
            Phase::Prepare => {
                // Resend Promise
                match &self.cached_promise_message {
                    Some(promise) => {
                        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                            from: self.pid,
                            to: promise.n.pid,
                            msg: PaxosMsg::Promise(promise.clone()),
                        }));
                    }
                    None => {
                        // Shouldn't be possible to be in prepare phase without having
                        // cached the promise sent as a response to the prepare
                        #[cfg(feature = "logging")]
                        warn!(self.logger, "In Prepare phase without a cached promise!");
                        self.state = (Role::Follower, Phase::Recover);
                        self.send_preparereq_to_all_peers();
                    }
                }
            }
            Phase::Recover => {
                // Resend PrepareReq
                self.send_preparereq_to_all_peers();
            }
            Phase::Accept => (),
            Phase::None => (),
        }
    }

    fn send_preparereq_to_all_peers(&mut self) {
        let prepreq = PrepareReq {
            n: self.get_promise(),
        };
        for peer in &self.peers {
            self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: *peer,
                msg: PaxosMsg::PrepareReq(prepreq),
            }));
        }
    }

    pub(crate) fn flush_batch_follower(&mut self) {
        let accepted_idx = self.internal_storage.get_accepted_idx();
        let new_accepted_idx = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
        if new_accepted_idx > accepted_idx {
            self.reply_accepted(self.get_promise(), new_accepted_idx);
        }
    }

    pub(crate) fn handle_log_modifications(&mut self, lm: LogModifications<T>) {
        if self.state.0 != Role::Follower || self.state.1 != Phase::Accept {
            #[cfg(feature = "logging")]
            debug!(self.logger, "Not handling LogModifications message"; "from" => self.pid);
            return;
        }
        if lm.n != self.leader_state.n_leader {
            #[cfg(feature = "logging")]
            info!(
                self.logger,
                "Ignoring LogModifications message from ballot: {:?}, current balot: {:?}",
                lm.n,
                self.leader_state.n_leader
            );
            return;
        }
        if let Some(first_modification) = lm.modifications.first() {
            if first_modification.log_id != self.internal_storage.get_accepted_idx() {
                #[cfg(feature = "logging")]
                warn!(
                    self.logger,
                    "First log modification's log_id {} does not match accepted_idx {}",
                    first_modification.log_id,
                    self.internal_storage.get_accepted_idx()
                );
            }
        }

        let mut append_start = None;

        for modification in lm.modifications.iter() {
            let local_entry = self
                .internal_storage
                .get_entry(modification.log_id)
                .expect(READ_ERROR_MSG);

            match local_entry {
                Some(entry) => {
                    // there is an entry at this index, check if it's the same request or not
                    self.update_deadline_or_replace_entry(&entry, modification);
                }
                None => {
                    // no entry at this index, need to append the rest of the modifications
                    append_start = Some(modification.log_id);
                    break;
                }
            }
        }

        // if append_start is Some, it means there are modifications that need to be appended
        if let Some(start) = append_start {
            self.append_missing_entries(&lm.modifications[start..]);
        }

        // update the accepted index to the end of the modifications, which is the new sync point for the leader
        self.internal_storage
            .set_accepted_idx(lm.modifications.last().unwrap().log_id)
            .expect(WRITE_ERROR_MSG);
    }

    fn update_deadline_or_replace_entry(
        &mut self,
        entry: &T,
        modification: &SingleLogModification<T>,
    ) {
        if entry.get_request_id() == modification.request_id {
            if entry.get_deadline() != modification.deadline {
                // same request but different deadline, update the deadline
                self.internal_storage
                    .update_deadline(modification.log_id, modification.deadline)
                    .expect(WRITE_ERROR_MSG);
            }
        } else {
            // different request, replace the entry
            let _old_entry = self
                .internal_storage
                .replace_entry(modification.log_id, modification.entry.clone())
                .expect(WRITE_ERROR_MSG);
        }
    }

    fn append_missing_entries(&mut self, modifications: &[SingleLogModification<T>]) {
        let entries: Vec<T> = modifications.iter().map(|m| m.entry.clone()).collect();
        self.internal_storage
            .append_entries_without_batching(entries, true)
            .expect(WRITE_ERROR_MSG);
    }

    pub(crate) fn send_log_status(&mut self) {
        if self.state.0 != Role::Follower || self.state.1 != Phase::Accept {
            #[cfg(feature = "logging")]
            debug!(
                self.logger,
                "Not sending log status message";
                "from" => self.pid
            );
            return;
        }
        let log_status = LogStatus {
            n: self.internal_storage.get_promise(),
            sync_point: self.internal_storage.get_accepted_idx(),
        };
        #[cfg(feature = "logging")]
        debug!(
            self.logger,
            "Sending log status message";
            "from" => self.pid,
            "msg" => format!("{:?}", log_status)
        );
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to: self.get_current_leader(),
            msg: PaxosMsg::LogStatus(log_status),
        }));
    }
}

#[cfg(all(test, not(feature = "unicache")))]
mod tests {
    use super::*;
    use crate::test_storage::TestStorage;
    use crate::{
        messages::sequence_paxos::{LogModifications, SingleLogModification},
        messages::RequestId,
        storage::{Entry, Snapshot},
        ClusterConfig, OmniPaxosConfig, ServerConfig,
    };
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEntry {
        value: u64,
        request_id: RequestId,
        deadline: u64,
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
    }

    impl TestEntry {
        fn new(value: u64, request_id: RequestId, deadline: u64) -> Self {
            Self {
                value,
                request_id,
                deadline,
            }
        }
    }

    fn create_seq_paxos(
        entries: Vec<TestEntry>,
        accepted_idx: usize,
    ) -> SequencePaxos<TestEntry, TestStorage<TestEntry>> {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2],
            flexible_quorum: None,
        };
        let server_config = ServerConfig {
            pid: 1,
            ..ServerConfig::default()
        };
        let omni_config = OmniPaxosConfig {
            cluster_config,
            server_config,
        };
        let storage = TestStorage::with_entries(entries);
        let mut paxos = SequencePaxos::with(omni_config.into(), storage);
        paxos.sync_point = accepted_idx;
        paxos
    }

    fn build_modifications(
        start_log_idx: usize,
        entries: Vec<TestEntry>,
    ) -> LogModifications<TestEntry> {
        let modifications = entries
            .into_iter()
            .enumerate()
            .map(|(offset, entry)| SingleLogModification {
                request_id: entry.request_id,
                deadline: entry.deadline,
                log_id: start_log_idx + offset,
                entry,
            })
            .collect();
        LogModifications {
            n: Ballot::default(),
            modifications,
        }
    }

    #[test]
    fn updates_deadlines_for_matching_entries() {
        // create entries
        let request_id_1 = Uuid::new_v4();
        let request_id_2 = Uuid::new_v4();
        let entries = vec![
            TestEntry::new(10, request_id_1, 5),
            TestEntry::new(20, request_id_2, 7),
        ];
        let mut paxos = create_seq_paxos(entries, 0);
        paxos.state = (Role::Follower, Phase::Accept);

        // create modifications with same request ids but different deadlines
        let updated = vec![
            TestEntry::new(10, request_id_1, 15),
            TestEntry::new(20, request_id_2, 25),
        ];
        let modifications = build_modifications(paxos.sync_point, updated);
        let expected_accepted_idx = modifications.modifications.last().unwrap().log_id;

        paxos.handle_log_modifications(modifications);

        // the deadlines should be updated but the entries should not be replaced
        let stored_entries = paxos.internal_storage.get_entries(0, 2).unwrap();
        assert_eq!(stored_entries[0].deadline, 15);
        assert_eq!(stored_entries[1].deadline, 25);
        assert_eq!(stored_entries[0].get_request_id(), request_id_1);
        assert_eq!(stored_entries[1].get_request_id(), request_id_2);
        assert_eq!(
            paxos.internal_storage.get_accepted_idx(),
            expected_accepted_idx
        );
    }

    #[test]
    fn replaces_entry_when_request_id_differs() {
        // createa entries and let accepted_idx point to the second entry
        let entry_below_accepted_idx = TestEntry::new(1, Uuid::new_v4(), 10);
        let to_replace = TestEntry::new(2, Uuid::new_v4(), 20);
        let mut paxos = create_seq_paxos(vec![entry_below_accepted_idx.clone(), to_replace], 1);
        paxos.state = (Role::Follower, Phase::Accept);

        // create modification with different request id than the entry at accepted_idx
        let replacement = TestEntry::new(99, Uuid::new_v4(), 30);
        let modifications = build_modifications(paxos.sync_point, vec![replacement.clone()]);
        let expected_accepted_idx = modifications.modifications.last().unwrap().log_id;

        paxos.handle_log_modifications(modifications);

        // the entry at accepted_idx should be replaced but the entry below accepted_idx should NOT be replaced
        let stored_entries = paxos.internal_storage.get_entries(0, 2).unwrap();
        assert_eq!(stored_entries[0], entry_below_accepted_idx);
        assert_eq!(stored_entries[1], replacement);
        assert_eq!(
            paxos.internal_storage.get_accepted_idx(),
            expected_accepted_idx
        );
    }

    #[test]
    fn replaces_two_entries_when_request_id_differs() {
        // create three entries and let accepted_idx be 0
        let untouched = TestEntry::new(1, Uuid::new_v4(), 10);
        let to_replace_1 = TestEntry::new(2, Uuid::new_v4(), 20);
        let to_replace_2 = TestEntry::new(3, Uuid::new_v4(), 25);

        let mut paxos = create_seq_paxos(vec![untouched.clone(), to_replace_1, to_replace_2], 0);
        paxos.state = (Role::Follower, Phase::Accept);

        // create two modifications with different request ids than the entries at log idx 1 and 2
        let replacement_1 = TestEntry::new(99, Uuid::new_v4(), 30);
        let replacement_2 = TestEntry::new(100, Uuid::new_v4(), 35);

        let modifications = build_modifications(
            paxos.sync_point,
            vec![
                untouched.clone(),
                replacement_1.clone(),
                replacement_2.clone(),
            ],
        );
        let expected_accepted_idx = modifications.modifications.last().unwrap().log_id;

        paxos.handle_log_modifications(modifications);

        // the entries at log idx 1 and 2 should be replaced but the entry at log idx 0 should NOT be replaced
        let stored_entries = paxos.internal_storage.get_entries(0, 3).unwrap();
        assert_eq!(stored_entries[0], untouched);
        assert_eq!(stored_entries[1], replacement_1);
        assert_eq!(stored_entries[2], replacement_2);
        assert_eq!(
            paxos.internal_storage.get_accepted_idx(),
            expected_accepted_idx
        );
    }

    #[test]
    fn appends_missing_entries_when_local_log_is_shorter() {
        // create one entry and let accepted_idx point to it
        let existing = TestEntry::new(1, Uuid::new_v4(), 11);
        let mut paxos = create_seq_paxos(vec![existing.clone()], 0);
        paxos.state = (Role::Follower, Phase::Accept);

        // adding enw entries with log idx 1 and 2, which are missing in the local log
        let new_entry_1 = TestEntry::new(2, Uuid::new_v4(), 22);
        let new_entry_2 = TestEntry::new(3, Uuid::new_v4(), 33);
        let modifications = build_modifications(
            paxos.sync_point,
            vec![existing.clone(), new_entry_1.clone(), new_entry_2.clone()],
        );
        let expected_accepted_idx = modifications.modifications.last().unwrap().log_id;

        paxos.handle_log_modifications(modifications);

        // the existing entry should remain and the new entries should be appended
        let stored_entries = paxos.internal_storage.get_entries(0, 3).unwrap();
        assert_eq!(stored_entries, vec![existing, new_entry_1, new_entry_2]);
        assert_eq!(
            paxos.internal_storage.get_accepted_idx(),
            expected_accepted_idx
        );
    }
}
