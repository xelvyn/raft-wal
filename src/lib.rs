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

//! Write-Ahead Log (WAL) for Raft.
//!
//! This crate provides a durable, append-only log optimized for use with Raft.
//!
//! It uses direct I/O (`O_DIRECT`) to bypass the OS page cache, ensuring consistent write latency
//! and avoiding double-buffering.
//!
//! Entries are written in page-aligned frames and are never modified in-place.
//! All writes are strictly append-only, preserving the integrity of existing data.
//!
//! The WAL consists of immutable segments and a mutable head segment. Older segments are read
//! sequentially; new entries are appended to the writable head segment.
//!
//! This implementation delegates batching, snapshotting, and caching to higher-level components,
//! focusing solely on the correctness and efficiency of low-level WAL persistence.

pub mod batch;
mod codec;
pub mod config;
pub mod error;
mod head;
mod iter;
mod metrics;
mod segment;
pub mod types;
pub mod page_block;

use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{
    batch::BatchUnion,
    config::{INITIAL_LAST_ENTRY_INDEX, INITIAL_SEGMENT_FIRST_INDEX, INITIAL_SEGMENT_NUMBER},
    head::Head,
    iter::WalIterator,
    segment::{Segment, SegmentKey, SegmentManager},
    types::{RaftIndex, SegmentNumber},
};
pub use crate::{
    batch::{Batch, BatchOwned},
    config::Config,
    error::{Error, Result},
    segment::segment_size,
    types::Record,
};

/// WAL state that shared across threads.
#[derive(Debug)]
struct WalInner {
    head: Head,
    head_number: SegmentNumber,
    last_entry_index: RaftIndex,
    segment_manager: SegmentManager,
}

/// Durable, append-only Raft WAL.
#[derive(Debug, Clone)]
pub struct Wal {
    inner: Arc<Mutex<WalInner>>,
}

impl Wal {
    /// Opens or initializes a new [`Wal`] instance.
    ///
    /// - If no segments exist yet, a new one is created and initialized.
    ///
    /// - If segments are found, the latest one is scanned to determine the last written entry and
    ///   the offset at which to resume appending.
    pub fn new(path: impl AsRef<Path>, _config: Config) -> Result<Wal> {
        let path = path.as_ref();
        let path_exists = fs::exists(path)?;
        if !path_exists {
            fs::create_dir_all(path)?;
        };

        let segment_manager = SegmentManager::new(path).expect("path exists or just created");
        let segment_keys = segment_manager.get_segment_keys()?;

        if segment_keys.is_empty() {
            let temp_dir = tempfile::tempdir()?;
            let temp_dir_path = temp_dir.path().to_path_buf();

            let key = SegmentKey::new(INITIAL_SEGMENT_NUMBER, INITIAL_SEGMENT_FIRST_INDEX);
            let head_path = temp_dir_path.join(key.to_string());
            let head = Segment::create(&head_path, segment_size())?;
            // Ensure durability before moving into place.
            head.fsync()?;

            // Atomically move initialized segment into final WAL path.
            fs::rename(temp_dir_path, path)?;

            // Re-initialize segment manager after moving directory.
            let segment_manager = SegmentManager::new(path)?;
            let head_segment = segment_manager.open_segment(key)?;

            let head = Head::create(head_segment)?;
            #[allow(clippy::arc_with_non_send_sync)]
            let inner = Arc::new(Mutex::new(WalInner {
                head,
                head_number: INITIAL_SEGMENT_NUMBER,
                last_entry_index: INITIAL_LAST_ENTRY_INDEX,
                segment_manager,
            }));
            let wal = Wal { inner };
            Ok(wal)
        } else {
            // WAL already exists restore state from latest segment.
            let last_key = *segment_keys.last().unwrap();
            let head = segment_manager.open_segment(last_key)?;

            let mut logical_offset = 0;
            let mut last_entry_index = last_key.first_index;

            let head_number = last_key.number;
            // Replay all records in the latest segment to determine last_entry_index
            for result in WalIterator::new(std::iter::once(head.clone()))? {
                let read_frame = result?;
                if let Record::Entry(existing_entry) = &read_frame.frame.record {
                    last_entry_index = existing_entry.index;
                };

                logical_offset += read_frame.frame.encoded_len();
            }

            // Reconstruct Head from recovered offset.
            let head = Head::recover(head, logical_offset)?;
            #[allow(clippy::arc_with_non_send_sync)]
            let inner = Arc::new(Mutex::new(WalInner {
                head,
                head_number,
                last_entry_index,
                segment_manager,
            }));
            let wal = Wal { inner };
            Ok(wal)
        }
    }

    /// Appends a borrowed batch of [`Record`]s to the WAL.
    pub fn write(&self, batch: Batch<'_>) -> Result<()> {
        self.write_union(batch.into())
    }

    /// Appends an owned batch of [`Record`]s to the WAL.
    #[inline]
    pub fn write_owned(&self, batch: BatchOwned) -> Result<()> {
        self.write_union(batch.into())
    }

    /// Writes a batch of records to the WAL.
    ///
    /// If the current segment is full and the batch contains Raft entries, a new segment is created
    /// and the write head is rotated to it. The last written entry index is updated accordingly.
    fn write_union(&self, batch: BatchUnion<'_>) -> Result<()> {
        let mut inner = self.inner.lock().expect("wal mutex poisoned");
        if batch.is_empty() {
            return Ok(());
        };

        // If the current head segment is full AND the batch contains Raft entries,
        // we rotate to a new segment before writing.
        //
        // Why rotation must happen before write:
        // - We use the first entry index of the batch to name the new segment SegmentKey.
        // - Raft does not guarantee that entry indices are strictly monotonic across batches. Only
        //   the pair `(term, index)` is guaranteed to be strictly increasing, but we do not track
        //   terms at the WAL level.
        // - Therefore, if we rotated after writing, we might not be able to assign a correct and
        //   unique first index to the new segment without risking ambiguity.
        //
        // By rotating before write, we ensure that the new segment key is always valid,and reflects
        // the starting index of the batch that begins the segment.
        if inner.head.len() >= segment_size() && batch.first_entry_index().is_some() {
            inner.head_number += 1;

            // The new segment key is based on the incremented segment number and the first entry
            // index of the incoming batch.
            let segment_key =
                SegmentKey::new(inner.head_number, batch.first_entry_index().unwrap());
            let head_segment = inner.segment_manager.create_segment(segment_key, segment_size())?;
            inner.head = Head::create(head_segment)?;
        };

        // Apply the batch to the current head segment.
        let last_entry_index = batch.last_entry_index();
        inner.head.write(batch)?;
        // Update the last known index if available.
        inner.last_entry_index = last_entry_index.unwrap_or(inner.last_entry_index);
        Ok(())
    }

    /// Returns an iterator over all [`Record`]s stored in the WAL.
    ///
    /// Records are returned in logical order. The iterator lazily loads and decodes data from disk
    /// as needed.
    pub fn read(&self) -> Result<impl Iterator<Item = Result<Record>> + use<>> {
        let inner = self.inner.lock().expect("wal mutex poisoned");
        let segments = inner.segment_manager.open_segments()?;
        // Wrap `WalIterator` and extract only the `.record` from each decoded frame.
        // This avoids exposing frame metadata at the top level.
        let iter = WalIterator::new(segments.into_iter())?
            .map(|result| result.map(|read_frame| read_frame.frame.record));
        Ok(iter)
    }

    /// Returns the current segment number being written to.
    #[inline]
    pub fn current_segment_number(&self) -> SegmentNumber {
        self.inner.lock().expect("wal mutex poisoned").head_number
    }

    /// Returns the index of the last entry written to the WAL.
    #[inline]
    pub fn last_entry_index(&self) -> u64 {
        self.inner.lock().expect("wal mutex poisoned").last_entry_index
    }
}

// SAFETY: All internal mutable state is guarded by `Mutex`.
unsafe impl Send for Wal {}
unsafe impl Sync for Wal {}
