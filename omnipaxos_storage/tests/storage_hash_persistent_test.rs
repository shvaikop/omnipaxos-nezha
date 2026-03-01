#![cfg(feature = "persistent_storage")]

use omnipaxos::storage::{Entry, LogHash, NoSnapshot, Storage};
use omnipaxos::util::NodeId;
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use serde::{Deserialize, Serialize};
use tempfile::tempdir;
// cargo test -p omnipaxos_storage --features persistent_storage --test storage_hash_persistent_test

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TestEntry {
    value: u64,
}

impl Entry for TestEntry {
    type Snapshot = NoSnapshot;

    fn stable_encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.value.to_le_bytes());
    }

    fn get_deadline(&self) -> u64 {
        0
    }

    fn set_deadline(&mut self, _deadline: u64) {
        // no-op for testing
    }

    fn get_request_id(&self) -> omnipaxos::messages::RequestId {
        omnipaxos::messages::RequestId::new_v4()
    }

    fn set_request_id(&mut self, _request_id: omnipaxos::messages::RequestId) {
        // no-op for testing
    }

    fn get_nezha_proxy_id(&self) -> NodeId {
        0
    }

    fn set_nezha_proxy_id(&mut self, _node_id: NodeId) {
        // no-op for testing
    }
}

#[test]
fn persistent_get_hash_prefix_and_out_of_bounds() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let config = PersistentStorageConfig::with_path(path);
    let mut storage = PersistentStorage::<TestEntry>::open(config);

    let entries = vec![
        TestEntry { value: 10 },
        TestEntry { value: 20 },
        TestEntry { value: 30 },
    ];
    storage.append_entries(entries.clone()).unwrap();

    let expected_prefix = LogHash::compute(&entries[0..2]);
    assert_eq!(storage.get_hash(2).unwrap(), expected_prefix);

    let expected_empty = LogHash::compute(&[] as &[TestEntry]);
    assert_eq!(storage.get_hash(100).unwrap(), expected_empty);
}

#[test]
fn persistent_get_hash_after_trim_uses_compacted_offset() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let config = PersistentStorageConfig::with_path(path);
    let mut storage = PersistentStorage::<TestEntry>::open(config);

    storage
        .append_entries(vec![
            TestEntry { value: 1 },
            TestEntry { value: 2 },
            TestEntry { value: 3 },
            TestEntry { value: 4 },
        ])
        .unwrap();
    storage.trim(2).unwrap();
    storage.set_compacted_idx(2).unwrap();

    let expected = LogHash::compute(&[TestEntry { value: 3 }]);
    assert_eq!(storage.get_hash(1).unwrap(), expected);
}

#[test]
fn persistent_get_hash_differs_for_permuted_entries() {
    let dir_a = tempdir().unwrap();
    let path_a = dir_a.path().to_string_lossy().to_string();
    let mut storage_a =
        PersistentStorage::<TestEntry>::open(PersistentStorageConfig::with_path(path_a));

    let dir_b = tempdir().unwrap();
    let path_b = dir_b.path().to_string_lossy().to_string();
    let mut storage_b =
        PersistentStorage::<TestEntry>::open(PersistentStorageConfig::with_path(path_b));

    storage_a
        .append_entries(vec![
            TestEntry { value: 1 },
            TestEntry { value: 2 },
            TestEntry { value: 3 },
        ])
        .unwrap();
    storage_b
        .append_entries(vec![
            TestEntry { value: 3 },
            TestEntry { value: 1 },
            TestEntry { value: 2 },
        ])
        .unwrap();

    let hash_a = storage_a.get_hash(3).unwrap();
    let hash_b = storage_b.get_hash(3).unwrap();
    assert_ne!(hash_a, hash_b);
}
