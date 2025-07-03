// Copyright Alexey Kotvitskiy
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;

use raft::eraftpb::Entry;
use tempfile::tempdir;
use raft_wal::{segment_size, Batch, Record, Wal};
use raft_wal::page_block::page_size;

fn make_entry(index: u64, data: &[u8]) -> Entry {
    let mut ent = Entry::default();
    ent.index = index;
    ent.term = 1;
    ent.data = data.to_vec();
    ent
}

#[test]
fn test_write_reopen_write_read_integrity() {
    let tmp = tempdir().unwrap();
    let wal_path = tmp.path().join("wal");

    {
        let wal = Wal::new(wal_path.clone(), Default::default()).unwrap();
        let entries: Vec<Entry> =
            (1..=10).map(|i| make_entry(i, format!("data-{}", i).as_bytes())).collect();

        let mut batch = Batch::new(entries.len());
        batch.push_entries(&entries);
        wal.write(batch).unwrap();

        assert_eq!(wal.last_entry_index(), 10);
    }

    {
        let wal = Wal::new(wal_path.clone(), Default::default()).unwrap();
        assert_eq!(wal.last_entry_index(), 10);

        let mut iter = wal.read().unwrap();
        for i in 1..=10 {
            let rec = iter.next().unwrap().unwrap();
            match rec {
                Record::Entry(ent) => {
                    assert_eq!(ent.index, i);
                    assert_eq!(ent.data, format!("data-{}", i).as_bytes());
                },
                _ => unreachable!(),
            }
        }
        assert!(iter.next().is_none());
    }

    {
        let wal = Wal::new(wal_path.clone(), Default::default()).unwrap();
        let base_index = wal.last_entry_index() + 1;
        let entries: Vec<Entry> = (0..10)
            .map(|i| {
                let idx = base_index + i;
                make_entry(idx, format!("tail-{}", idx).as_bytes())
            })
            .collect();
        let mut batch = Batch::new(entries.len());
        batch.push_entries(&entries);
        wal.write(batch).unwrap();
    }

    {
        let wal = Wal::new(wal_path.clone(), Default::default()).unwrap();
        assert_eq!(wal.last_entry_index(), 20);

        let mut iter = wal.read().unwrap();
        for i in 1..=20 {
            let rec = iter.next().unwrap().unwrap();
            match rec {
                Record::Entry(ent) => {
                    let expected =
                        if i <= 10 { format!("data-{}", i) } else { format!("tail-{}", i) };
                    assert_eq!(ent.index, i);
                    assert_eq!(ent.data, expected.as_bytes());
                },
                _ => unreachable!(),
            }
        }
    }
}

#[test]
fn test_partial_restore_on_crash_simulation() {
    let tmp = tempdir().unwrap();
    let wal_path = tmp.path().join("wal");

    {
        let wal = Wal::new(wal_path.clone(), Default::default()).unwrap();
        let entries: Vec<Entry> =
            (1..=5).map(|i| make_entry(i, format!("stable-{}", i).as_bytes())).collect();
        let mut batch = Batch::new(entries.len());
        batch.push_entries(&entries);
        wal.write(batch).unwrap();
    }

    {
        let _wal = Wal::new(wal_path.clone(), Default::default()).unwrap();
        let entries: Vec<Entry> =
            (6..=10).map(|i| make_entry(i, format!("unstable-{}", i).as_bytes())).collect();
        let mut batch = Batch::new(entries.len());
        batch.push_entries(&entries);
        drop(batch);
    }

    {
        let wal = Wal::new(wal_path.clone(), Default::default()).unwrap();
        assert_eq!(wal.last_entry_index(), 5);

        let mut iter = wal.read().unwrap();
        for i in 1..=5 {
            let rec = iter.next().unwrap().unwrap();
            match rec {
                Record::Entry(ent) => {
                    assert_eq!(ent.index, i);
                    assert_eq!(ent.data, format!("stable-{}", i).as_bytes());
                },
                _ => unreachable!(),
            }
        }
        assert!(iter.next().is_none());
    }
}

#[test]
fn test_page_block_expansion_and_recovery_continuity() {
    let tmp = tempdir().unwrap();
    let wal_path = tmp.path().join("wal");

    let page_sz = page_size();

    let sample_entry = make_entry(1, &[0u8; 100]);
    let mut sample_batch = Batch::new(1);
    sample_batch.push_entry(&sample_entry);
    let approx_frame_size = sample_batch.size();

    let entries_per_page = page_sz as u64 / approx_frame_size;
    let total_entries = entries_per_page * 3;

    {
        let wal = Wal::new(wal_path.clone(), Default::default()).unwrap();
        let entries: Vec<Entry> = (1..=total_entries)
            .map(|i| make_entry(i, format!("page-fill-{}", i).as_bytes()))
            .collect();

        let mut batch = Batch::new(entries.len());
        batch.push_entries(&entries);
        wal.write(batch).unwrap();

        assert_eq!(wal.last_entry_index(), total_entries);
    }

    {
        let wal = Wal::new(wal_path.clone(), Default::default()).unwrap();
        assert_eq!(wal.last_entry_index(), total_entries);

        let extra_index = total_entries + 1;
        let extra_entry = make_entry(extra_index, b"post-recovery");
        let mut batch = Batch::new(1);
        batch.push_entry(&extra_entry);
        wal.write(batch).unwrap();
        assert_eq!(wal.last_entry_index(), extra_index);
    }

    {
        let wal = Wal::new(wal_path.clone(), Default::default()).unwrap();
        let final_index = entries_per_page * 3 + 1;
        assert_eq!(wal.last_entry_index(), final_index);

        let mut iter = wal.read().unwrap();
        for i in 1..=final_index {
            let rec = iter.next().unwrap().unwrap();
            match rec {
                Record::Entry(ent) => {
                    let expected = if i <= total_entries {
                        format!("page-fill-{}", i)
                    } else {
                        "post-recovery".to_string()
                    };
                    assert_eq!(ent.index, i);
                    assert_eq!(ent.data, expected.as_bytes());
                },
                _ => unreachable!(),
            }
        }
        assert!(iter.next().is_none());
    }
}

#[test]
fn test_segment_rotation() {
    let tmp = tempdir().unwrap();
    let wal_path = tmp.path().join("wal");

    let segment_size = segment_size();
    let entry_data = vec![0u8; (segment_size + 1) as usize];

    let wal = Wal::new(wal_path.clone(), Default::default()).unwrap();
    let first_segment = wal.current_segment_number();

    {
        let entry = make_entry(1, &entry_data);
        let mut batch = Batch::new(1);
        batch.push_entry(&entry);
        wal.write(batch).unwrap();
    }

    {
        let entry = make_entry(2, &entry_data);
        let mut batch = Batch::new(1);
        batch.push_entry(&entry);
        wal.write(batch).unwrap();
    }
    let second_segment = wal.current_segment_number();
    assert!(second_segment > first_segment);

    std::thread::sleep(std::time::Duration::from_secs(1));
    let wal = Wal::new(wal_path.clone(), Default::default()).unwrap();
    assert_eq!(wal.last_entry_index(), 2);
    let mut iter = wal.read().unwrap();
    for i in 1..=2 {
        let rec = iter.next().unwrap().unwrap();
        match rec {
            Record::Entry(ent) => {
                assert_eq!(ent.index, i);
                assert_eq!(ent.data, entry_data.as_slice());
            },
            _ => unreachable!(),
        }
    }

    let files: Vec<_> = fs::read_dir(&wal_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().into_string().unwrap())
        .collect();

    let segment_count = files.iter().filter(|f| f.contains('-')).count();
    assert!(segment_count >= 2);
}