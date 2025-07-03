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

use lazy_static::lazy_static;
use prometheus::{Histogram, exponential_buckets, register_histogram};

lazy_static! {
    pub static ref WRITE_BATCH_ENCODE_HISTOGRAM: Histogram = register_histogram!(
        "raft_wal_batch_encode_duration",
        "Bucketed histogram of WAL write batch encoding duration.",
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    )
    .unwrap();
    pub static ref RECORDS_COUNT_PER_WRITE_BATCH_HISTOGRAM: Histogram = register_histogram!(
        "raft_wal_records_count_per_batch",
        "Bucketed histogram of how many records are in each WAL batch.",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SIZE_PER_WRITE_BATCH_HISTOGRAM: Histogram = register_histogram!(
        "raft_wal_bytes_per_batch",
        "Bucketed histogram of total byte size of each WAL batch.",
        exponential_buckets(64.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref WRITE_BATCH_DURATION: Histogram = register_histogram!(
        "raft_wal_write_batch_duration",
        "Bucketed histogram of write WAL duration.",
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    )
    .unwrap();
    pub static ref FSYNC_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_wal_fsync_duration",
        "Bucketed histogram of WAL fsync duration.",
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    )
    .unwrap();
}
