use crate::{
    messages::{ballot_leader_election::BLEMessage, sequence_paxos::PaxosMessage},
    storage::Entry,
    util::NodeId,
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Internal component for log replication
pub mod sequence_paxos {
    use crate::{
        ballot_leader_election::Ballot,
        storage::{Entry, StopSign},
        util::{LogSync, NodeId, SequenceNumber},
    };
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};
    use std::fmt::Debug;

    /// Message sent by a follower on crash-recovery or dropped messages to request its leader to re-prepare them.
    #[derive(Copy, Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct PrepareReq {
        /// The current round.
        pub n: Ballot,
    }

    /// Prepare message sent by a newly-elected leader to initiate the Prepare phase.
    #[derive(Copy, Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Prepare {
        /// The current round.
        pub n: Ballot,
        /// The decided index of this leader.
        pub decided_idx: usize,
        /// The latest round in which an entry was accepted.
        pub n_accepted: Ballot,
        /// The log length of this leader.
        pub accepted_idx: usize,
    }

    /// Promise message sent by a follower in response to a [`Prepare`] sent by the leader.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Promise<T>
    where
        T: Entry,
    {
        /// The current round.
        pub n: Ballot,
        /// The latest round in which an entry was accepted.
        pub n_accepted: Ballot,
        /// The decided index of this follower.
        pub decided_idx: usize,
        /// The log length of this follower.
        pub accepted_idx: usize,
        /// The log update which the leader applies to its log in order to sync
        /// with this follower (if the follower is more up-to-date).
        pub log_sync: Option<LogSync<T>>,
    }

    /// AcceptSync message sent by the leader to synchronize the logs of all replicas in the prepare phase.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct AcceptSync<T>
    where
        T: Entry,
    {
        /// The current round.
        pub n: Ballot,
        /// The sequence number of this message in the leader-to-follower accept sequence
        pub seq_num: SequenceNumber,
        /// The decided index
        pub decided_idx: usize,
        /// The log update which the follower applies to its log in order to sync
        /// with the leader.
        pub log_sync: LogSync<T>,
        #[cfg(feature = "unicache")]
        /// The UniCache of the leader
        pub unicache: T::UniCache,
    }

    /// Message with entries to be replicated and the latest decided index sent by the leader in the accept phase.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct AcceptDecide<T>
    where
        T: Entry,
    {
        /// The current round.
        pub n: Ballot,
        /// The sequence number of this message in the leader-to-follower accept sequence
        pub seq_num: SequenceNumber,
        /// The decided index.
        pub decided_idx: usize,
        #[cfg(not(feature = "unicache"))]
        /// Entries to be replicated.
        pub entries: Vec<T>,
        #[cfg(feature = "unicache")]
        /// Entries to be replicated.
        pub entries: Vec<T::EncodeResult>,
    }

    /// Message sent by follower to leader when entries has been accepted.
    #[derive(Copy, Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Accepted {
        /// The current round.
        pub n: Ballot,
        /// The accepted index.
        pub accepted_idx: usize,
    }

    /// Message sent by leader to followers to decide up to a certain index in the log.
    #[derive(Copy, Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Decide {
        /// The current round.
        pub n: Ballot,
        /// The sequence number of this message in the leader-to-follower accept sequence
        pub seq_num: SequenceNumber,
        /// The decided index.
        pub decided_idx: usize,
    }

    /// Message sent by leader to followers to accept a StopSign
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct AcceptStopSign {
        /// The current round.
        pub n: Ballot,
        /// The sequence number of this message in the leader-to-follower accept sequence
        pub seq_num: SequenceNumber,
        /// The decided index.
        pub ss: StopSign,
    }

    /// Message sent by follower to leader when accepting an entry is rejected.
    /// This happens when the follower is promised to a greater leader.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct NotAccepted {
        /// The follower's current ballot
        pub n: Ballot,
    }

    /// Compaction Request
    #[allow(missing_docs)]
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum Compaction {
        Trim(usize),
        Snapshot(Option<usize>),
    }

    /// An enum for all the different message types.
    #[allow(missing_docs)]
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum PaxosMsg<T>
    where
        T: Entry,
    {
        /// Request a [`Prepare`] to be sent from the leader. Used for fail-recovery.
        PrepareReq(PrepareReq),
        #[allow(missing_docs)]
        Prepare(Prepare),
        Promise(Promise<T>),
        AcceptSync(AcceptSync<T>),
        AcceptDecide(AcceptDecide<T>),
        Accepted(Accepted),
        NotAccepted(NotAccepted),
        Decide(Decide),
        /// Forward client proposals to the leader.
        ProposalForward(Vec<T>),
        Compaction(Compaction),
        AcceptStopSign(AcceptStopSign),
        ForwardStopSign(StopSign),
    }

    /// A struct for a Paxos message that also includes sender and receiver.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct PaxosMessage<T>
    where
        T: Entry,
    {
        /// Sender of `msg`.
        pub from: NodeId,
        /// Receiver of `msg`.
        pub to: NodeId,
        /// The message content.
        pub msg: PaxosMsg<T>,
    }
}

/// Nezha protocol messages used by the NezhaProxy layer. These messages are seperate from 
/// SequencePaxos and BLE and are only used for the fast/slow coordniation 
pub mod nezha {
    use crate::{storage::{Entry}, util::{NodeId}};
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};

    /// Timestamp type used for Nezha messages, representing the time when a message is sent or a deadline for processing the message
    pub type Timestamp = u64;
    /// Unique identifier for client requests, used to track requests across the system and match replies to requests
    pub type RequestId = uuid::Uuid;

    /// Request sent by a client to the proxy to propose an entry for replication
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct ClientRequest<T>
    where
        T: Entry,
    {
        /// The id of the client request
        pub request_id: RequestId,
        /// The entry to be proposed for replication
        pub entry: T,
    }

    /// Reply sent by the proxy back to the client after the request completes
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct ClientReply {
        /// The id of the client request
        pub request_id: RequestId,
        /// Whether the request was applied successfully
        pub ok: bool,
    }

    /// Prepare message sent by the proxy to replicas with a deadline when the message should be processed
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct PrepareMsgWithDeadline<T>
    where
        T: Entry, 
    {
        /// The id of the client request
        pub request_id: RequestId,
        /// The entry to be proposed for replication
        pub entry: T,
        /// The timestamp when the message is sent
        pub sent: Timestamp,  
        /// The deadline timestap when the message should be processed
        pub deadline: Timestamp,   
    }

    /// Fast path is sent by every replica to prozy after it has appended or exexuted the request
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct FastReply {
        /// The id of the client request
        pub request_id: RequestId,
        /// Hash of all replicas logs 
        pub log_hash: u64,
    }

    /// Slow path reply from both leaders and followers to proxy
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct SlowReply {
        /// The id of the client request
        pub request_id: RequestId,
    }

    /// Periodically send from followers to the leader, reporting the follwer's sync-point
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct LogStatus {
        /// Highest log index for which this replica's log matches the leader's log
        pub sync_point: usize,
    }

    /// Broadcast by the leader so followers can adjust their log entry at log_id.
    /// E.g. insert the missing request or update its deadline to match the leader
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct LogModification {
        /// The id of the client request
        pub request_id: RequestId,
        /// The deadline timestap when the message should be processed
        pub deadline: Timestamp,  
        /// Position of this entry in the leader's log
        pub log_id: usize,
    }

    /// A struct for a Nezha message that also includes sender and receiver.
    #[allow(missing_docs)]
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum NezhaMsg<T>
    where
        T: Entry,
    {
        ClientRequest(ClientRequest<T>),
        ClientReply(ClientReply),
        PrepareMsgWithDeadline(PrepareMsgWithDeadline<T>),
        FastReply(FastReply),
        SlowReply(SlowReply),
        LogStatus(LogStatus),
        LogModification(LogModification),
    }

    /// A struct for a Nezha message that also includes sender and receiver
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct NezhaMessage<T>
    where
        T: Entry,
    {
        /// Sender of `msg`.
        pub from: NodeId,
        /// Receiver of `msg`.
        pub to: NodeId,
        /// The message content.
        pub msg: NezhaMsg<T>,
    }
}

/// The different messages BLE uses to communicate with other servers.
pub mod ballot_leader_election {

    use crate::{ballot_leader_election::Ballot, util::NodeId};
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};

    /// An enum for all the different BLE message types.
    #[allow(missing_docs)]
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum HeartbeatMsg {
        Request(HeartbeatRequest),
        Reply(HeartbeatReply),
    }

    /// Requests a reply from all the other servers.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct HeartbeatRequest {
        /// Number of the current round.
        pub round: u32,
    }

    /// Replies
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct HeartbeatReply {
        /// Number of the current heartbeat round.
        pub round: u32,
        /// Ballot of replying server.
        pub ballot: Ballot,
        /// Leader this server is following
        pub leader: Ballot,
        /// Whether the replying server sees a need for a new leader
        pub happy: bool,
    }

    /// A struct for a Paxos message that also includes sender and receiver.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct BLEMessage {
        /// Sender of `msg`.
        pub from: NodeId,
        /// Receiver of `msg`.
        pub to: NodeId,
        /// The message content.
        pub msg: HeartbeatMsg,
    }
}

#[allow(missing_docs)]
/// Message in OmniPaxos. Can be either a `SequencePaxos` message (for log replication) or `BLE` message (for leader election)
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Message<T>
where
    T: Entry,
{
    SequencePaxos(PaxosMessage<T>),
    BLE(BLEMessage),
}

impl<T> Message<T>
where
    T: Entry,
{
    /// Get the sender id of the message
    pub fn get_sender(&self) -> NodeId {
        match self {
            Message::SequencePaxos(p) => p.from,
            Message::BLE(b) => b.from,
        }
    }

    /// Get the receiver id of the message
    pub fn get_receiver(&self) -> NodeId {
        match self {
            Message::SequencePaxos(p) => p.to,
            Message::BLE(b) => b.to,
        }
    }
}
