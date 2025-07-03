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

use crate::{
    Error,
    codec::FRAME_HEADER_LEN,
    error::Result,
    page_block::{PageBlock, page_size, pages_for},
    segment::{SegmentKey, TrackedSegment},
    types::{Frame, FrameHeader, SegmentLen, SegmentOffset},
};

/// A wrapper around a [`TrackedSegment`] that tracks read progress.
#[derive(Debug)]
struct ReadSegment {
    /// The underlying tracked WAL segment.
    segment: TrackedSegment,
    /// Total length of the segment in bytes.
    len: SegmentLen,
    /// Current read offset within the segment.
    offset: SegmentOffset,
}

impl ReadSegment {
    /// Creates a new [`ReadSegment`] instance.
    #[inline]
    fn new(segment: TrackedSegment) -> Result<ReadSegment> {
        let len = segment.len()?;
        Ok(ReadSegment { segment, len, offset: 0 })
    }

    /// Reads a block of data consisting of the given number of pages.
    #[inline]
    fn read_block(&mut self, pages: usize) -> Result<PageBlock> {
        let mut block = PageBlock::new(pages);
        self.segment.read_at(&mut block, self.offset)?;
        self.offset += block.size() as u64;
        Ok(block)
    }

    /// Returns `true` if the segment is considered exhausted.
    #[inline]
    fn is_exhausted(&self) -> bool {
        self.offset + page_size() as u64 >= self.len
    }

    /// Marks the segment as fully read by setting the offset to the end.
    #[inline]
    fn mark_exhausted(&mut self) {
        self.offset = self.len;
    }

    /// Returns the number of remaining unread bytes in the segment.
    #[inline]
    fn remaining(&self) -> u64 {
        self.len.saturating_sub(self.offset)
    }

    /// Returns the current read offset within the segment.
    #[inline]
    fn offset(&self) -> SegmentOffset {
        self.offset
    }
}

/// A frame read from the WAL.
///
/// [`ReadFrame`] contains the actual [`Frame`] data as well as precise location information
/// indicating where the frame was read from in the WAL.
///
/// Location details include:
///
/// - The key of the segment where the frame is stored
/// - The byte offset within that segment where the frame starts
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ReadFrame {
    /// The decoded frame.
    pub(crate) frame: Frame,
    /// The key identifying the WAL segment that contains this frame.
    pub(crate) segment_key: SegmentKey,
    /// Byte offset within the segment where this frame begins.
    pub(crate) offset: SegmentOffset,
}

/// An iterator over frames stored in a WAL segment sequence.
///
/// [`WalIterator`] traverses a sequence of [`TrackedSegment`]s, decoding frames one by one
/// and yielding [`ReadFrame`] instances that include both the decoded frame and its location.
///
/// Segments are read lazily: the iterator advances to the next segment only after the
/// current one is fully consumed.
///
/// Internally, it maintains a scratch buffer that accumulates data as needed to decode
/// a complete frame from potentially partial page reads.
pub(crate) struct WalIterator<I>
where
    I: Iterator<Item = TrackedSegment>,
{
    segments: I,
    current: Option<ReadSegment>,
    scratch: Vec<u8>,
}

impl<I> WalIterator<I>
where
    I: Iterator<Item = TrackedSegment>,
{
    /// Creates a new [`WalIterator`] instance. from a sequence of segments.
    pub(crate) fn new(mut segments: I) -> Result<WalIterator<I>> {
        let current = segments.next().map(ReadSegment::new).transpose()?;
        let scratch = Vec::with_capacity(page_size());
        Ok(WalIterator { segments, current, scratch })
    }

    /// Attempts to read the next frame from the WAL.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(ReadFrame))` on success.
    /// - `Ok(None)` if all data has been exhausted.
    /// - `Err` if decoding or I/O fails.
    pub(crate) fn read_next(&mut self) -> Result<Option<ReadFrame>> {
        let header = loop {
            let segment = match self.current.as_mut() {
                Some(s) => s,
                None => return Ok(None),
            };

            if (self.scratch.len() as u64) < FRAME_HEADER_LEN {
                if segment.is_exhausted() {
                    self.current = self.segments.next().map(ReadSegment::new).transpose()?;
                    continue;
                };
                let block = segment.read_block(1)?;
                self.scratch.extend_from_slice(block.as_slice());
            };

            // Although in practice `page_size()` is always much larger than `FRAME_HEADER_LEN` (12
            // bytes), we cannot rely on that implicitly. If `page_size()` were smaller
            // — due to misconfiguration or an unusual platform — then reading just one-
            // page may not be enough to decode a full frame header.
            //
            // Because of that, after reading a page, we re-check whether `scratch` has accumulated
            // at least `FRAME_HEADER_LEN` bytes. If not, we loop again to read more data.
            //
            // This avoids attempting to decode an incomplete header, which could otherwise result
            // in incorrect parsing, unexpected errors, or even panics.
            if (self.scratch.len() as u64) < FRAME_HEADER_LEN {
                continue;
            };

            let header = FrameHeader::decode(&self.scratch)?;
            if header.record_len == 0 {
                // FrameHeader can technically be decoded from any 12-byte slice, even if it
                // contains all zeros. For example, [0; 12] is a valid header from a
                // parsing standpoint.
                //
                // However, such a header is semantically invalid in a real WAL stream,
                // because a valid frame always includes a record payload, and that payload
                // must begin with at least one byte — specifically, the record type.
                //
                // Since segments are often preallocated and may contain uninitialized or zeroed
                // space near the end, it’s possible for such "empty" headers to appear.
                //
                // Therefore, we treat a decoded header with `record_len == 0` as an indicator
                // that there's no more meaningful data — and skip it by clearing the buffer
                // and marking the segment as exhausted.
                self.scratch.clear();
                segment.mark_exhausted();
                continue;
            };
            break header;
        };

        // SAFETY: Header was decoded from current segment, so it must exist
        let segment = self.current.as_mut().unwrap();
        let need = header.need_len_for_record();
        let have = (self.scratch.len() as u64).saturating_sub(FRAME_HEADER_LEN);
        if have < need {
            let read = need - have;
            // INVARIANT: record must not cross segment boundaries.
            if read > segment.remaining() {
                return Err(Error::SegmentFrameSplit);
            };
            let block = segment.read_block(pages_for(read as usize))?;
            self.scratch.extend_from_slice(block.as_slice());
        }

        // Decode the frame from the scratch buffer using the header.
        let frame = Frame::decode(&self.scratch[FRAME_HEADER_LEN as usize..], header)?;
        let frame_encoded_len = frame.encoded_len();
        // segment.offset()      → absolute byte position after the last read.
        // self.scratch.len()    → bytes we have buffered but NOT yet consumed.
        //
        // Because a frame is never allowed to cross segment boundaries (see
        // SegmentFrameSplit guard earlier), the beginning of this frame must be exactly
        //
        //     <current segment offset>  -  <bytes still in scratch>
        //
        // In other words, once we’ve read enough data for the whole frame, all of those
        // freshly-read bytes sit at the tail of the segment.  Subtracting their length
        // rewinds us to the frame’s first byte — giving us its precise on-disk offset
        // without needing to track any extra state.
        let offset = segment.offset() - (self.scratch.len() as u64);
        let segment_key = segment.segment.key();
        self.scratch.drain(..frame_encoded_len as usize);
        Ok(Some(ReadFrame { frame, segment_key, offset }))
    }
}

impl<I> Iterator for WalIterator<I>
where
    I: Iterator<Item = TrackedSegment>,
{
    type Item = Result<ReadFrame>;

    /// Advances the iterator and returns the next frame from the WAL.
    ///
    /// # Returns
    ///
    /// - `Some(Ok(ReadFrame))` if a frame was successfully read.
    /// - `Some(Err(_))` if an error occurred during reading or decoding.
    /// - `None` when all segments are exhausted.
    fn next(&mut self) -> Option<Result<ReadFrame>> {
        self.read_next().transpose()
    }
}
