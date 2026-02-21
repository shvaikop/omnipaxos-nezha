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
}
