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

use raft::eraftpb::{Entry, HardState};

use crate::{
    Record,
    error::Result,
    page_block::PageBlockBuf,
    types::{FrameRef, RecordRef},
};

/// Tracks the cumulative encoded size of Raft records without retaining their actual data.
///
/// `WriteBatchSizeTrace` allows estimating the size of a prospective write batch before
/// allocating memory or constructing a full [`Batch`].
#[derive(Debug, Clone, Copy, Default, PartialOrd, PartialEq)]
pub struct WriteBatchSizeTrace {
    size: u64,
    capacity: usize,
}

impl WriteBatchSizeTrace {
    /// Creates a new empty trace.
    #[inline]
    pub fn new() -> WriteBatchSizeTrace {
        WriteBatchSizeTrace::default()
    }

    /// Adds the size of a slice of [`Entry`] instances to the trace.
    #[inline]
    pub fn on_entries(&mut self, entries: &[Entry]) {
        for entry in entries {
            self.on_entry(entry);
        }
    }

    /// Optionally adds the size of a [`HardState`] to the trace, if provided.
    #[inline]
    pub fn on_maybe_hard_state(&mut self, hard_state: Option<&HardState>) {
        if let Some(hard_state) = hard_state {
            self.on_hard_state(hard_state);
        }
    }

    /// Adds the size of a [`HardState`] to the trace.
    #[inline]
    pub fn on_hard_state(&mut self, hard_state: &HardState) {
        self.on_record(RecordRef::HardState(hard_state));
    }

    /// Adds the size of a single [`Entry`] to the trace.
    #[inline]
    pub fn on_entry(&mut self, entry: &Entry) {
        self.on_record(RecordRef::Entry(entry));
    }

    /// Internal method to record the size of a [`RecordRef`].
    #[inline]
    fn on_record(&mut self, record: RecordRef<'_>) {
        self.capacity += 1;
        self.size += FrameRef(record).encoded_len();
    }

    /// Returns the total encoded size in bytes.
    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Returns the number of records processed.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// A batch of Raft records prepared for writing to the write-ahead log (WAL).
///
/// This structure collects [`HardState`] and [`Entry`] instances into a single memory-aligned
/// block, tracking their total encoded size and entry indices to support sequential WAL
/// persistence.
#[derive(Debug)]
pub struct Batch<'a> {
    records: Vec<RecordRef<'a>>,
    size_trace: WriteBatchSizeTrace,
    first_index: Option<u64>,
    last_entry_index: Option<u64>,
}

impl<'a> Batch<'a> {
    /// Creates a new `WriteBatch` with reserved capacity for records.
    ///
    /// The `capacity` specifies the number of records to pre-allocate space for.
    pub fn new(capacity: usize) -> Batch<'a> {
        Batch {
            records: Vec::with_capacity(capacity),
            size_trace: WriteBatchSizeTrace::default(),
            first_index: None,
            last_entry_index: None,
        }
    }

    /// Adds a single [`Entry`] to the batch.
    ///
    /// Updates the `first_entry_index` and `last_entry_index` accordingly.
    #[inline]
    pub fn push_entry(&mut self, entry: &'a Entry) {
        if self.first_index.is_none() {
            self.first_index = Some(entry.index);
        }
        self.last_entry_index = Some(entry.index);
        self.push_record(RecordRef::Entry(entry));
    }

    /// Adds a slice of [`Entry`] instances to the batch.
    ///
    /// Entries are pushed in order; updates `first_entry_index` and `last_entry_index`.
    #[inline]
    pub fn push_entries(&mut self, entries: &'a [Entry]) {
        for entry in entries {
            self.push_entry(entry);
        }
    }

    /// Optionally adds a [`HardState`] to the batch.
    ///
    /// No-op if the argument is `None`.
    #[inline]
    pub fn maybe_push_hard_state(&mut self, hard_state: Option<&'a HardState>) {
        if let Some(hard_state) = hard_state {
            self.push_hard_state(hard_state)
        }
    }

    /// Adds a [`HardState`] to the batch.
    #[inline]
    pub fn push_hard_state(&mut self, hard_state: &'a HardState) {
        self.push_record(RecordRef::HardState(hard_state));
    }

    /// Adds a single [`RecordRef`] to the batch and updates the size trace.
    #[inline]
    fn push_record(&mut self, record: RecordRef<'a>) {
        self.size_trace.on_record(record);
        self.records.push(record);
    }

    /// Returns the total encoded size of all records in the batch.
    #[inline]
    pub fn size(&self) -> u64 {
        self.size_trace.size()
    }

    /// Returns `true` if the batch contains no records.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Returns len of records in [`Batch`].
    #[inline]
    pub fn records_len(&self) -> usize {
        self.records.len()
    }

    /// Returns the index of the first appended `Entry`, if any.
    #[inline]
    pub fn first_entry_index(&self) -> Option<u64> {
        self.first_index
    }

    /// Returns the index of the last appended `Entry`, if any.
    #[inline]
    pub fn last_entry_index(&self) -> Option<u64> {
        self.last_entry_index
    }

    /// Encodes the batch into the provided [`PageBlockBuf`] for persistence.
    ///
    /// # Panics
    ///
    /// Panics if the batch is empty.
    pub(crate) fn encode(self, dst: &mut PageBlockBuf) -> Result<()> {
        assert!(self.size() > 0, "Cannot encode an empty batch");
        for record in self.records {
            let frame = FrameRef(record);
            let len = frame.encoded_len() as usize;
            frame.encode(dst.as_mut_slice(len))?;
        }
        Ok(())
    }
}

impl<'a> Default for Batch<'a> {
    fn default() -> Batch<'a> {
        Batch::new(1024)
    }
}

/// An owned version of [`Batch`].
///
/// Instead of storing references to [`Entry`] and/or [`HardState`] this batch will store data
/// directly.
#[derive(Debug)]
pub struct BatchOwned {
    records: Vec<Record>,
    size_trace: WriteBatchSizeTrace,
    first_index: Option<u64>,
    last_entry_index: Option<u64>,
}

impl BatchOwned {
    /// Creates a new `WriteBatch` with reserved capacity for records.
    ///
    /// The `capacity` specifies the number of records to pre-allocate space for.
    pub fn new(capacity: usize) -> BatchOwned {
        BatchOwned {
            records: Vec::with_capacity(capacity),
            size_trace: WriteBatchSizeTrace::default(),
            first_index: None,
            last_entry_index: None,
        }
    }

    /// Adds a single [`Entry`] to the batch.
    ///
    /// Updates the `first_entry_index` and `last_entry_index` accordingly.
    #[inline]
    pub fn push_entry(&mut self, entry: Entry) {
        if self.first_index.is_none() {
            self.first_index = Some(entry.index);
        }
        self.last_entry_index = Some(entry.index);
        self.push_record(Record::Entry(entry));
    }

    /// Adds a slice of [`Entry`] instances to the batch.
    ///
    /// Entries are pushed in order; updates `first_entry_index` and `last_entry_index`.
    #[inline]
    pub fn push_entries(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            self.push_entry(entry);
        }
    }

    /// Optionally adds a [`HardState`] to the batch.
    ///
    /// No-op if the argument is `None`.
    #[inline]
    pub fn maybe_push_hard_state(&mut self, hard_state: Option<HardState>) {
        if let Some(hard_state) = hard_state {
            self.push_hard_state(hard_state)
        }
    }

    /// Adds a [`HardState`] to the batch.
    #[inline]
    pub fn push_hard_state(&mut self, hard_state: HardState) {
        self.push_record(Record::HardState(hard_state));
    }

    /// Adds a single [`RecordRef`] to the batch and updates the size trace.
    #[inline]
    fn push_record(&mut self, record: Record) {
        self.size_trace.on_record(record.as_ref());
        self.records.push(record);
    }

    /// Returns the total encoded size of all records in the batch.
    #[inline]
    pub fn size(&self) -> u64 {
        self.size_trace.size()
    }

    /// Returns `true` if the batch contains no records.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Returns len of records in [`BatchOwned`].
    #[inline]
    pub fn records_len(&self) -> usize {
        self.records.len()
    }

    /// Returns the index of the first appended `Entry`, if any.
    #[inline]
    pub fn first_entry_index(&self) -> Option<u64> {
        self.first_index
    }

    /// Returns the index of the last appended `Entry`, if any.
    #[inline]
    pub fn last_entry_index(&self) -> Option<u64> {
        self.last_entry_index
    }

    /// Encodes the batch into the provided [`PageBlockBuf`] for persistence.
    ///
    /// # Panics
    ///
    /// Panics if the batch is empty.
    pub(crate) fn encode(self, dst: &mut PageBlockBuf) -> Result<()> {
        assert!(self.size() > 0);
        for record in self.records {
            let frame = FrameRef(record.as_ref());
            let len = frame.encoded_len() as usize;
            frame.encode(dst.as_mut_slice(len))?;
        }
        Ok(())
    }
}

impl Default for BatchOwned {
    fn default() -> BatchOwned {
        BatchOwned::new(1024)
    }
}

#[derive(Debug)]
pub(crate) enum BatchUnion<'a> {
    Ref(Batch<'a>),
    Owned(BatchOwned),
}

impl<'a> BatchUnion<'a> {
    #[inline]
    pub(crate) fn size(&self) -> u64 {
        match self {
            BatchUnion::Ref(r) => r.size(),
            BatchUnion::Owned(o) => o.size(),
        }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            BatchUnion::Ref(r) => r.is_empty(),
            BatchUnion::Owned(o) => o.is_empty(),
        }
    }

    #[inline]
    pub(crate) fn records_len(&self) -> usize {
        match self {
            BatchUnion::Ref(r) => r.records_len(),
            BatchUnion::Owned(o) => o.records_len(),
        }
    }

    #[inline]
    pub(crate) fn first_entry_index(&self) -> Option<u64> {
        match self {
            BatchUnion::Ref(r) => r.first_entry_index(),
            BatchUnion::Owned(o) => o.first_entry_index(),
        }
    }

    #[inline]
    pub(crate) fn last_entry_index(&self) -> Option<u64> {
        match self {
            BatchUnion::Ref(r) => r.last_entry_index(),
            BatchUnion::Owned(o) => o.last_entry_index(),
        }
    }

    #[inline]
    pub(crate) fn encode(self, dst: &mut PageBlockBuf) -> Result<()> {
        match self {
            BatchUnion::Ref(r) => r.encode(dst),
            BatchUnion::Owned(o) => o.encode(dst),
        }
    }
}

impl<'a> From<Batch<'a>> for BatchUnion<'a> {
    fn from(write_batch: Batch<'a>) -> BatchUnion<'a> {
        BatchUnion::Ref(write_batch)
    }
}

impl<'a> From<BatchOwned> for BatchUnion<'a> {
    fn from(write_batch: BatchOwned) -> BatchUnion<'a> {
        BatchUnion::Owned(write_batch)
    }
}
