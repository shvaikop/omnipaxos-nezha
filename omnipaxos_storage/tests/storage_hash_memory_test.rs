use omnipaxos::storage::{Entry, LogHash, NoSnapshot, Storage};
use omnipaxos_storage::memory_storage::MemoryStorage;

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestEntry {
    value: u64,
}

impl Entry for TestEntry {
    type Snapshot = NoSnapshot;

    fn stable_encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.value.to_le_bytes());
    }
}

#[test]
fn memory_get_hash_prefix_and_out_of_bounds() {
    let mut storage = MemoryStorage::<TestEntry>::default();
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
fn memory_get_hash_after_trim_uses_retained_log() {
    let mut storage = MemoryStorage::<TestEntry>::default();
    storage
        .append_entries(vec![
            TestEntry { value: 1 },
            TestEntry { value: 2 },
            TestEntry { value: 3 },
            TestEntry { value: 4 },
        ])
        .unwrap();

    storage.trim(2).unwrap();

    let expected = LogHash::compute(&[TestEntry { value: 3 }]);
    assert_eq!(storage.get_hash(1).unwrap(), expected);
}

#[test]
fn memory_get_hash_differs_for_permuted_entries() {
    let mut storage_a = MemoryStorage::<TestEntry>::default();
    let mut storage_b = MemoryStorage::<TestEntry>::default();

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
