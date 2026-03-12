use super::super::{
    ballot_leader_election::Ballot,
    util::{LeaderState, PromiseMetaData},
};
use crate::util::{AcceptedMetaData, WRITE_ERROR_MSG};

use super::*;

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /// Handle a new leader. Should be called when the leader election has elected a new leader with the ballot `n`
    /*** Leader ***/
    pub(crate) fn handle_leader(&mut self, n: Ballot) {
        if n <= self.leader_state.n_leader || n <= self.internal_storage.get_promise() {
            return;
        }

        // Leader has changed, all previous Nezha fast-path state is stale.
        #[cfg(feature = "logging")]
        if !self.reply_set.is_empty() {
            debug!(
                self.logger,
                "Leader change: clearing {} pending Nezha reply sets",
                self.reply_set.len()
            );
        }
        self.reply_set.clear();
        #[cfg(feature = "logging")]
        debug!(self.logger, "Newly elected leader: {:?}", n);
        if self.pid == n.pid {
            self.leader_state =
                LeaderState::with(n, self.leader_state.max_pid, self.leader_state.quorum);
            // Flush any pending writes
            // Don't have to handle flushed entries here because we will sync with followers
            let _ = self.internal_storage.flush_batch().expect(WRITE_ERROR_MSG);
            self.internal_storage.set_promise(n).expect(WRITE_ERROR_MSG);
            /* insert my promise */
            let na = self.internal_storage.get_accepted_round();
            let decided_idx = self.get_decided_idx();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let my_promise = Promise {
                n,
                n_accepted: na,
                decided_idx,
                accepted_idx,
                log_sync: None,
            };
            self.leader_state.set_promise(my_promise, self.pid, true);
            /* initialise longest chosen sequence and update state */
            self.state = (Role::Leader, Phase::Prepare);
            let prep = Prepare {
                n,
                decided_idx,
                n_accepted: na,
                accepted_idx,
            };
            /* send prepare */
            for pid in &self.peers {
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: *pid,
                    msg: PaxosMsg::Prepare(prep),
                }));
            }
        } else {
            self.become_follower();
        }
    }

    pub(crate) fn become_follower(&mut self) {
        self.state.0 = Role::Follower;
    }

    pub(crate) fn handle_preparereq(&mut self, prepreq: PrepareReq, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(self.logger, "Incoming message PrepareReq from {}", from);
        if self.state.0 == Role::Leader && prepreq.n <= self.leader_state.n_leader {
            self.leader_state.reset_promise(from);
            self.leader_state.set_latest_accept_meta(from, None);
            self.send_prepare(from);
        }
    }

    pub(crate) fn handle_forwarded_proposal(&mut self, mut entries: Vec<T>) {
        if !self.accepted_reconfiguration() {
            match self.state {
                (Role::Leader, Phase::Prepare) => self.buffered_proposals.append(&mut entries),
                (Role::Leader, Phase::Accept) => self.process_buffered_proposals(entries),
                _ => self.forward_proposals(entries),
            }
        }
    }

    pub(crate) fn handle_forwarded_stopsign(&mut self, ss: StopSign) {
        if self.accepted_reconfiguration() {
            return;
        }
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_stopsign = Some(ss),
            (Role::Leader, Phase::Accept) => self.accept_stopsign_leader(ss),
            _ => self.forward_stopsign(ss),
        }
    }

    pub(crate) fn send_prepare(&mut self, to: NodeId) {
        let prep = Prepare {
            n: self.leader_state.n_leader,
            decided_idx: self.internal_storage.get_decided_idx(),
            n_accepted: self.internal_storage.get_accepted_round(),
            accepted_idx: self.internal_storage.get_accepted_idx(),
        };
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Prepare(prep),
        }));
    }

    #[allow(dead_code)]
    pub(crate) fn accept_entry_leader(&mut self, entry: T) {
        let accepted_metadata = self
            .internal_storage
            .append_entry_with_batching(entry)
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn accept_entries_leader(&mut self, entries: Vec<T>) {
        let accepted_metadata = self
            .internal_storage
            .append_entries_with_batching(entries)
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
        }
    }

    fn process_buffered_proposals(&mut self, entries: Vec<T>) {
        match self.state {
            (Role::Leader, Phase::Accept) => {
                for entry in entries {
                    let deadline = entry.get_deadline();
                    let request_id = entry.get_request_id();
                    let prep = PrepareWithDeadline {
                        from: entry.get_nezha_proxy_id(),
                        entry,
                        sent: self.clock.now_us(),  // we do not really use sent_time
                    };
                    self.late_buffer.insert((deadline, request_id), prep);
                    self.req_id_to_late_buffer_deadline.insert(request_id, deadline);
                    #[cfg(feature = "logging")]
                    trace!(self.logger, "Moved request: {:?} from buffered_proposals to late_buffer", request_id);
                }
            }
            (Role::Leader, _) => self.buffered_proposals.extend(entries),
            // Proposals were forwarded to node but it is no longer a leader
            _ => self.forward_proposals(entries),
        }
    }

    pub(crate) fn accept_stopsign_leader(&mut self, ss: StopSign) {
        let accepted_metadata = self
            .internal_storage
            .append_stopsign(ss.clone())
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.send_acceptdecide(metadata);
        }
        let accepted_idx = self.internal_storage.get_accepted_idx();
        self.leader_state.set_accepted_idx(self.pid, accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accept_stopsign(pid, ss.clone(), false);
        }
    }

    fn send_accsync(&mut self, to: NodeId) {
        let current_n = self.leader_state.n_leader;
        let PromiseMetaData {
            n_accepted: prev_round_max_promise_n,
            accepted_idx: prev_round_max_accepted_idx,
            ..
        } = &self.leader_state.get_max_promise_meta();
        let PromiseMetaData {
            n_accepted: followers_promise_n,
            accepted_idx: followers_accepted_idx,
            pid,
            ..
        } = self.leader_state.get_promise_meta(to);
        let followers_decided_idx = self
            .leader_state
            .get_decided_idx(*pid)
            .expect("Received PromiseMetaData but not found in ld");
        // Follower can have valid accepted entries depending on which leader they were previously following
        let followers_valid_entries_idx = if *followers_promise_n == current_n {
            *followers_accepted_idx
        } else if *followers_promise_n == *prev_round_max_promise_n {
            *prev_round_max_accepted_idx.min(followers_accepted_idx)
        } else {
            followers_decided_idx
        };
        let log_sync = self.create_log_sync(followers_valid_entries_idx, followers_decided_idx);
        self.leader_state.increment_seq_num_session(to);
        let acc_sync = AcceptSync {
            n: current_n,
            seq_num: self.leader_state.next_seq_num(to),
            decided_idx: self.get_decided_idx(),
            log_sync,
            #[cfg(feature = "unicache")]
            unicache: self.internal_storage.get_unicache(),
        };
        let msg = Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::AcceptSync(acc_sync),
        });
        self.outgoing.push(msg);
    }

    fn send_acceptdecide(&mut self, accepted: AcceptedMetaData<T>) {
        let decided_idx = self.internal_storage.get_decided_idx();
        for pid in self.leader_state.get_promised_followers() {
            let latest_accdec = self.get_latest_accdec_message(pid);
            match latest_accdec {
                // Modify existing AcceptDecide message to follower
                Some(accdec) => {
                    accdec.entries.extend(accepted.entries.iter().cloned());
                    accdec.decided_idx = decided_idx;
                }
                // Add new AcceptDecide message to follower
                None => {
                    self.leader_state
                        .set_latest_accept_meta(pid, Some(self.outgoing.len()));
                    let acc = AcceptDecide {
                        n: self.leader_state.n_leader,
                        seq_num: self.leader_state.next_seq_num(pid),
                        decided_idx,
                        entries: accepted.entries.clone(),
                    };
                    self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                        from: self.pid,
                        to: pid,
                        msg: PaxosMsg::AcceptDecide(acc),
                    }));
                }
            }
        }
    }

    fn send_accept_stopsign(&mut self, to: NodeId, ss: StopSign, resend: bool) {
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let acc_ss = PaxosMsg::AcceptStopSign(AcceptStopSign {
            seq_num,
            n: self.leader_state.n_leader,
            ss,
        });
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: acc_ss,
        }));
    }

    pub(crate) fn send_decide(&mut self, to: NodeId, decided_idx: usize, resend: bool) {
        let seq_num = match resend {
            true => self.leader_state.get_seq_num(to),
            false => self.leader_state.next_seq_num(to),
        };
        let d = Decide {
            n: self.leader_state.n_leader,
            seq_num,
            decided_idx,
        };
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to,
            msg: PaxosMsg::Decide(d),
        }));
    }

    fn handle_majority_promises(&mut self) {
        let max_promise_sync = self.leader_state.take_max_promise_sync();
        let decided_idx = self.leader_state.get_max_decided_idx();
        let mut new_accepted_idx = self
            .internal_storage
            .sync_log(self.leader_state.n_leader, decided_idx, max_promise_sync)
            .expect(WRITE_ERROR_MSG);

        // we have to switch to Accept phase before processing `buffered_proposals`
        self.state = (Role::Leader, Phase::Accept);
        if !self.accepted_reconfiguration() {
            if !self.buffered_proposals.is_empty() {
                let entries = std::mem::take(&mut self.buffered_proposals);
                self.process_buffered_proposals(entries);
            }
            if let Some(ss) = self.buffered_stopsign.take() {
                self.internal_storage
                    .append_stopsign(ss)
                    .expect(WRITE_ERROR_MSG);
                new_accepted_idx = self.internal_storage.get_accepted_idx();
            }
        }
        self.leader_state
            .set_accepted_idx(self.pid, new_accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accsync(pid);
        }
    }

    pub(crate) fn handle_promise_prepare(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(
            self.logger,
            "Handling promise from {} in Prepare phase", from
        );
        if prom.n == self.leader_state.n_leader {
            let received_majority = self.leader_state.set_promise(prom, from, true);
            if received_majority {
                self.handle_majority_promises();
            }
        }
    }

    pub(crate) fn handle_promise_accept(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        {
            let (r, p) = &self.state;
            debug!(
                self.logger,
                "Self role {:?}, phase {:?}. Incoming message Promise Accept from {}", r, p, from
            );
        }
        if prom.n == self.leader_state.n_leader {
            self.leader_state.set_promise(prom, from, false);
            self.send_accsync(from);
        }
    }

    pub(crate) fn handle_accepted(&mut self, accepted: Accepted, from: NodeId) {
        #[cfg(feature = "logging")]
        trace!(
            self.logger,
            "Got Accepted from {}, idx: {}, chosen_idx: {}, accepted: {:?}",
            from,
            accepted.accepted_idx,
            self.internal_storage.get_decided_idx(),
            self.leader_state.accepted_indexes
        );
        if accepted.n == self.leader_state.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.leader_state
                .set_accepted_idx(from, accepted.accepted_idx);
            if accepted.accepted_idx > self.internal_storage.get_decided_idx()
                && self.leader_state.is_chosen(accepted.accepted_idx)
            {
                let decided_idx = accepted.accepted_idx;
                self.internal_storage
                    .set_decided_idx(decided_idx)
                    .expect(WRITE_ERROR_MSG);
                for pid in self.leader_state.get_promised_followers() {
                    let latest_accdec = self.get_latest_accdec_message(pid);
                    match latest_accdec {
                        Some(accdec) => accdec.decided_idx = decided_idx,
                        None => self.send_decide(pid, decided_idx, false),
                    }
                }
            }
        }
    }

    fn get_latest_accdec_message(&mut self, to: NodeId) -> Option<&mut AcceptDecide<T>> {
        if let Some((bal, outgoing_idx)) = self.leader_state.get_latest_accept_meta(to) {
            if bal == self.leader_state.n_leader {
                if let Message::SequencePaxos(PaxosMessage {
                    msg: PaxosMsg::AcceptDecide(accdec),
                    ..
                }) = self.outgoing.get_mut(outgoing_idx).unwrap()
                {
                    return Some(accdec);
                } else {
                    #[cfg(feature = "logging")]
                    debug!(self.logger, "Cached idx is not an AcceptedDecide!");
                }
            }
        }
        None
    }

    pub(crate) fn handle_notaccepted(&mut self, not_acc: NotAccepted, from: NodeId) {
        if self.state.0 == Role::Leader && self.leader_state.n_leader < not_acc.n {
            self.leader_state.lost_promise(from);
        }
    }

    pub(crate) fn resend_messages_leader(&mut self) {
        match self.state.1 {
            Phase::Prepare => {
                // Resend Prepare
                let preparable_peers = self.leader_state.get_preparable_peers(&self.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            Phase::Accept => {
                // Resend AcceptStopSign or StopSign's decide
                if let Some(ss) = self.internal_storage.get_stopsign() {
                    let decided_idx = self.internal_storage.get_decided_idx();
                    for follower in self.leader_state.get_promised_followers() {
                        if self.internal_storage.stopsign_is_decided() {
                            self.send_decide(follower, decided_idx, true);
                        } else if self.leader_state.get_accepted_idx(follower)
                            != self.internal_storage.get_accepted_idx()
                        {
                            self.send_accept_stopsign(follower, ss.clone(), true);
                        }
                    }
                }
                // Resend Prepare
                let preparable_peers = self.leader_state.get_preparable_peers(&self.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            Phase::Recover => (),
            Phase::None => (),
        }
    }

    pub(crate) fn flush_batch_leader(&mut self) {
        let accepted_metadata = self
            .internal_storage
            .flush_batch_and_get_entries()
            .expect(WRITE_ERROR_MSG);
        if let Some(metadata) = accepted_metadata {
            self.leader_state
                .set_accepted_idx(self.pid, metadata.accepted_idx);
            self.send_acceptdecide(metadata);
        }
    }

    pub(crate) fn broadcast_log_modifications(&mut self) {
        let n: Ballot = self.leader_state.n_leader;

        for pid in self.leader_state.get_promised_followers() {
            let leader_accepted_idx = self.internal_storage.get_accepted_idx();
            let follower_accepted_idx = self.leader_state.get_accepted_idx(pid);

            let entries = self
                .internal_storage
                .get_entries(follower_accepted_idx, leader_accepted_idx)
                .expect(READ_ERROR_MSG);

            let modifications: Vec<SingleLogModification<T>> = entries
                .into_iter()
                .enumerate()
                .map(|(offset, entry)| SingleLogModification {
                    request_id: entry.get_request_id(),
                    deadline: entry.get_deadline(),
                    log_id: follower_accepted_idx + offset,
                    entry: entry.clone(),
                })
                .collect();

            #[cfg(feature = "logging")]
            trace!(
                self.logger,
                "Broadcasting LogModifications";
                "to" => pid,
                "leader_accepted_idx" => leader_accepted_idx,
                "follower_accepted_idx" => follower_accepted_idx,
                "modifications" => format!("{:?}", modifications),
            );

            self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: pid,
                msg: PaxosMsg::LogModifications(LogModifications { n, modifications }),
            }));
        }
    }

    pub(crate) fn handle_log_status(&mut self, log_status: LogStatus, from: NodeId) {
        if self.state != (Role::Leader, Phase::Accept) {
            #[cfg(feature = "logging")]
            info!(self.logger, "Not handling LogStatus message");
            return;
        }
        // If message belongs to wrong ballot then ignore it
        if log_status.n != self.leader_state.n_leader {
            #[cfg(feature = "logging")]
            info!(
                self.logger,
                "Ignoring LogStatus message from ballot: {:?}, current balot: {:?}",
                log_status.n,
                self.leader_state.n_leader
            );
            return;
        }
        // if log_status.sync_point is outdated - it is ignored inside the setter
        self.leader_state
            .set_accepted_idx(from, log_status.sync_point);
        let new_decided_idx = self.leader_state.get_commit_idx();
        let old_decided_idx = self.internal_storage.get_decided_idx();
        if new_decided_idx > old_decided_idx {
            match self.internal_storage.set_decided_idx(new_decided_idx) {
                Ok(()) => {
                    #[cfg(feature = "logging")]
                    info!(
                        self.logger,
                        "Updated decided index from {} to {}", old_decided_idx, new_decided_idx
                    );
                    self.send_commit_status();
                }
                Err(_e) => {
                    #[cfg(feature = "logging")]
                    warn!(
                        self.logger,
                        "Failed to update decided index";
                        "old_decided_idx" => old_decided_idx,
                        "new_decided_idx" => new_decided_idx,
                        "error" => _e.to_string()
                    );
                }
            }
        }
    }

    pub(crate) fn send_commit_status(&mut self) {
        if self.state != (Role::Leader, Phase::Accept) {
            #[cfg(feature = "logging")]
            debug!(
                self.logger,
                "Not sending commit status message";
                "from" => self.pid
            );
            return;
        }

        let commit_status = CommitStatus {
            n: self.internal_storage.get_promise(),
            commit_point: self.internal_storage.get_decided_idx(),
        };
        #[cfg(feature = "logging")]
        debug!(
            self.logger,
            "Sending commit status message";
            "from" => self.pid,
            "msg" => format!("{:?}", commit_status)
        );

        // sending the commit status to all promised followers
        for pid in self.leader_state.get_promised_followers() {
            self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: pid,
                msg: PaxosMsg::CommitStatus(commit_status.clone()),
            }));
        }
    }
}
