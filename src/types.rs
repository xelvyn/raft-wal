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

use crate::error::RecordCodecError;

pub type SegmentNumber = u64;
pub(crate) type SegmentLen = u64;
pub(crate) type SegmentOffset = u64;
pub(crate) type PageIndex = usize;
pub type RaftIndex = u64;
pub type RaftEntry = Entry;

/// An owned WAL record.
#[derive(Debug, Clone)]
pub enum Record {
    Entry(Entry),
    HardState(HardState),
}

impl Record {
    /// Converts [`Record`] into [`RecordRef`]
    #[inline]
    pub(crate) fn as_ref(&self) -> RecordRef<'_> {
        match self {
            Record::Entry(e) => RecordRef::Entry(e),
            Record::HardState(h) => RecordRef::HardState(h),
        }
    }
}

/// A reference-based representation of a WAL record used during encoding.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RecordRef<'a> {
    Entry(&'a Entry),
    HardState(&'a HardState),
}

/// A marker indicating the type of WAL record.
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq)]
#[repr(u8)]
pub(crate) enum RecordType {
    Entry = 0,
    HardState = 1,
}

impl TryFrom<u8> for RecordType {
    type Error = RecordCodecError;

    fn try_from(value: u8) -> Result<RecordType, RecordCodecError> {
        match value {
            0 => Ok(RecordType::Entry),
            1 => Ok(RecordType::HardState),
            _ => Err(RecordCodecError::UnknownRecordType(value)),
        }
    }
}

/// A reference to an encodable WAL record frame.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FrameRef<'a>(pub RecordRef<'a>);

/// Metadata header for a frame, containing only the record length and CRC.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FrameHeader {
    pub(crate) record_len: u64,
    pub(crate) crc: u32,
}

/// A fully decoded WAL frame (header + record).
#[derive(Debug, Clone)]
pub(crate) struct Frame {
    pub(crate) header: FrameHeader,
    pub(crate) record: Record,
}
