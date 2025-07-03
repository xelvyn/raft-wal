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

use raft_wal::{Batch, Wal};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use raft::eraftpb::Entry;
use tempfile::tempdir;

fn bench_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");

    for &(entry_size, num_entries) in &[
        (256, 1),     // 256 * 100 = 25,600 bytes ≈ 25.0 KiB
        (1024, 50),   // 1024 * 50 = 51,200 bytes ≈ 50.0 KiB
        (4096, 25),   // 4096 * 25 = 102,400 bytes ≈ 100.0 KiB
        (8192, 10),   // 8192 * 10 = 81,920 bytes ≈ 80.0 KiB
        (16384, 5),   // 16384 * 5 = 81,920 bytes ≈ 80.0 KiB
        (32768, 5),   // 16384 * 2 = 32,768 * 5 = 163,840 bytes ≈ 160.0 KiB
        (65536, 4),   // 16384 * 4 = 65,536 * 4 = 262,144 bytes ≈ 256.0 KiB
        (98304, 3),   // 16384 * 6 = 98,304 * 3 = 294,912 bytes ≈ 288.0 KiB
        (131072, 2),  // 16384 * 8 = 131,072 * 2 = 262,144 bytes ≈ 256.0 KiB
        (262144, 1),  // 16384 * 16 = 262,144 bytes = 256.0 KiB
        (2097152, 1), // 16384 * 128 = 2,097,152 bytes = 2.0 MiB
    ] {
        let bytes = (entry_size * num_entries) as u64;
        group.throughput(Throughput::Bytes(bytes));

        let bench_id = BenchmarkId::new("write", format!("{}x{}", entry_size, num_entries));

        group.bench_with_input(bench_id, &(), |b, &_input| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let wal = Wal::new(dir.path().join("wal"), Default::default()).unwrap();
                    let data = vec![0u8; entry_size];
                    (wal, data, dir)
                },
                |(wal, data, _dir)| {
                    for i in 0..num_entries {
                        let mut entry = Entry::default();
                        entry.index = i as u64 + 1;
                        entry.term = 1;
                        entry.data = data.clone();
                        let mut batch = Batch::new(1);
                        let entries = [entry];
                        batch.push_entries(&entries);
                        wal.write(batch).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");

    for &(entry_size, num_entries) in &[
        (256, 100),   // 256 * 100 = 25,600 bytes ≈ 25.0 KiB
        (1024, 100),  // 1024 * 50 = 51,200 bytes ≈ 50.0 KiB
        (4096, 25),   // 4096 * 25 = 102,400 bytes ≈ 100.0 KiB
        (8192, 10),   // 8192 * 10 = 81,920 bytes ≈ 80.0 KiB
        (16384, 5),   // 16384 * 5 = 81,920 bytes ≈ 80.0 KiB
        (32768, 5),   // 16384 * 2 = 32,768 * 5 = 163,840 bytes ≈ 160.0 KiB
        (65536, 4),   // 16384 * 4 = 65,536 * 4 = 262,144 bytes ≈ 256.0 KiB
        (98304, 3),   // 16384 * 6 = 98,304 * 3 = 294,912 bytes ≈ 288.0 KiB
        (131072, 2),  // 16384 * 8 = 131,072 * 2 = 262,144 bytes ≈ 256.0 KiB
        (262144, 1),  // 16384 * 16 = 262,144 bytes = 256.0 KiB
        (2097152, 3), // 16384 * 128 = 2,097,152 bytes = 2.0 MiB
    ] {
        let bytes = (entry_size * num_entries) as u64;
        group.throughput(Throughput::Bytes(bytes));

        let bench_id = BenchmarkId::new("read", format!("{}x{}", entry_size, num_entries));

        group.bench_with_input(bench_id, &(), |b, &_input| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let wal = Wal::new(dir.path().join("wal"), Default::default()).unwrap();
                    let data = vec![0u8; entry_size];
                    let mut entries = Vec::new();
                    for index in 1..num_entries {
                        entries.push(Entry {
                            index: index as u64,
                            data: data.clone(),
                            ..Default::default()
                        });
                    }
                    let mut batch = Batch::new(1);
                    batch.push_entries(&entries);
                    wal.write(batch).unwrap();
                    (wal, dir)
                },
                |(wal, _)| {
                    for result in wal.read().unwrap() {
                        result.unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}
criterion_group!(wal_benches, bench_write, bench_read);
criterion_main!(wal_benches);