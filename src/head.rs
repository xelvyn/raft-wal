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

use std::time::Instant;

use crate::{
    Result,
    batch::BatchUnion,
    metrics::{
        FSYNC_DURATION_HISTOGRAM, RECORDS_COUNT_PER_WRITE_BATCH_HISTOGRAM,
        SIZE_PER_WRITE_BATCH_HISTOGRAM, WRITE_BATCH_DURATION, WRITE_BATCH_ENCODE_HISTOGRAM,
    },
    page_block::{PageBlock, PageBlockBuf, page_size},
    segment::TrackedSegment,
    types::{PageIndex, SegmentLen},
};

/// Active write head of a WAL.
///
/// The [`Head`] tracks where new data should be written in the active segment.
/// It buffers writes in memory by page-sized blocks and ensures consistency
/// across writes, recovery, and flushes.
#[derive(Debug)]
pub(crate) struct Head {
    /// The segment where data is currently being written.
    segment: TrackedSegment,
    /// Current length of the segment on disk (after last successful write).
    ///
    /// This is updated manually after each write, and is used to compute
    /// the true WAL length, including partial page data.
    segment_len: SegmentLen,
    /// In-memory buffer representing the current page being written to.
    ///
    /// This buffer always has page-aligned capacity, even if the contents only partially fill the
    /// page. After a multi-page write, it is cleared to prepare for reuse.
    page_block_buf: PageBlockBuf,
    /// Index of the page currently targeted for writing.
    ///
    /// For example, if `page_index = 2`, the next write will begin at byte offset `2 *
    /// page_size()`.
    page_index: PageIndex,
}

impl Head {
    /// Creates a new [`Head`] for the specified segment.
    ///
    /// Starts writing at page index 1. The segment length is initialized from the existing segment.
    pub(crate) fn create(segment: TrackedSegment) -> Result<Head> {
        let segment_len = segment.len()?;
        Ok(Head { segment, segment_len, page_index: 0, page_block_buf: PageBlockBuf::new() })
    }

    /// Recovers a [`Head`] from an existing segment and logical offset.
    ///
    /// This is typically used after WAL initialization to resume writing from a previously flushed
    /// position.
    ///
    /// It reads the corresponding page from disk and reconstructs the in-memory buffer, trimming it
    /// to the correct logical offset.
    pub(crate) fn recover(segment: TrackedSegment, logical_offset: u64) -> Result<Head> {
        let segment_len = segment.len()?;

        // Determine which page contains the logical offset
        let page = (logical_offset as usize) / page_size();
        let page_offset = page * page_size();

        // Read that page from disk
        let mut page_block = PageBlock::new(1);
        segment.read_at(&mut page_block, page_offset as u64)?;

        // Reconstruct buffer, trimming out unused bytes before the logical offset
        let page_block_buf =
            PageBlockBuf::from_page_block(page_block, logical_offset as usize - page_offset);

        Ok(Head { segment, segment_len, page_index: page, page_block_buf })
    }

    /// Writes a [`WriteBatch`] to the current page in the segment.
    ///
    /// Handles encoding, updates the segment length if needed, writes data to the segment, and
    /// performs an `fsync` to ensure data durability If the batch spans multiple pages, the write
    /// head is advanced accordingly.
    pub(crate) fn write(&mut self, batch: BatchUnion<'_>) -> Result<()> {
        RECORDS_COUNT_PER_WRITE_BATCH_HISTOGRAM.observe(batch.records_len() as f64);
        SIZE_PER_WRITE_BATCH_HISTOGRAM.observe(batch.size() as f64);

        let batch_encode_timer = Instant::now();
        batch.encode(&mut self.page_block_buf)?;
        WRITE_BATCH_ENCODE_HISTOGRAM.observe(batch_encode_timer.elapsed().as_secs_f64());

        let offset = (self.page_index * page_size()) as u64;
        let encoded = (self.page_block_buf.block().pages() * page_size()) as u64;
        let new_len = offset + encoded;

        // Update segment length if the write goes beyond the current end.
        if self.segment_len < new_len {
            self.segment.set_len(new_len)?;
            self.segment_len = new_len;
        };

        let write_batch_timer = Instant::now();
        self.segment.write_at(self.page_block_buf.block(), offset)?;
        WRITE_BATCH_DURATION.observe(write_batch_timer.elapsed().as_secs_f64());

        let fsync_timer = Instant::now();
        self.segment.fsync()?;
        FSYNC_DURATION_HISTOGRAM.observe(fsync_timer.elapsed().as_secs_f64());

        let pages_written = self.page_block_buf.block().pages();
        // If we used multiple pages, advance the page index accordingly.
        // We also purge the in-memory buffer so it's ready for the next write.
        if pages_written > 1 {
            self.page_block_buf.purge(page_size());
            self.page_index += pages_written - 1;
        };

        Ok(())
    }

    /// Returns the logical length of the WAL segment.
    #[inline]
    pub(crate) fn len(&self) -> u64 {
        self.segment_len - (page_size() - self.page_block_buf.len()) as u64
    }
}
