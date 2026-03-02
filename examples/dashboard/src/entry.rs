use omnipaxos::util::NodeId;
use omnipaxos::{
    messages::RequestId,
    storage::{Entry, NoSnapshot},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub value: u64,
    pub deadline: u64,
    pub request_id: RequestId,
}

impl Entry for LogEntry {
    type Snapshot = NoSnapshot;

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

    fn stable_encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.value.to_le_bytes());
        out.extend_from_slice(&self.deadline.to_le_bytes());
        out.extend(self.request_id.to_bytes_le());
    }

    fn get_nezha_proxy_id(&self) -> NodeId {
        0
    }

    fn set_nezha_proxy_id(&mut self, _node_id: NodeId) {
        // no-op for testing
    }
}
