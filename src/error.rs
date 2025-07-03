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

use std::io;

/// Type alias for [`std::result::Result`] with predefined [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Enum of errors that can occur when encoding or decoding record.
#[derive(Debug, thiserror::Error)]
pub enum RecordCodecError {
    #[error("Failed to decode entry: {0}")]
    EntryDecode(prost::DecodeError),
    #[error("Failed to decode hard state: {0}")]
    HardStateDecode(prost::DecodeError),
    #[error("Buf too small to fit record: required: {required}, remaining: {remaining}")]
    BufTooSmallToFit { required: u64, remaining: u64 },
    #[error("Buf too small to read record: required at least: {required}, remaining: {remaining}")]
    BufTooSmallToRead { required: u64, remaining: u64 },
    #[error("Unknown record type: {0}")]
    UnknownRecordType(u8),
}

/// Enum of errors that can occur when encoding or decoding frame.
#[derive(Debug, thiserror::Error)]
pub enum FrameCodecError {
    /// CRC check failed — frame is corrupted or tampered with.
    #[error("CRC mismatch")]
    CrcMismatch,
    #[error("Buf too small to fit frame: required: {required}, remaining: {remaining}")]
    BufTooSmallToFit { required: u64, remaining: u64 },
    #[error("Buf too small to read frame header: required: {required}, remaining: {remaining}")]
    BufTooSmallToReadFrameHeader { required: u64, remaining: u64 },
    #[error("Buf too small to read frame: required: {required}, remaining: {remaining}")]
    BufTooSmallToReadFrame { required: u64, remaining: u64 },
    #[error("Record codec: {0}")]
    RecordCodec(#[from] RecordCodecError),
}

/// Enum of errors that can occur at WAL.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Attempted to read a segment that does not exist.
    #[error("Segment not found")]
    SegmentNotFound,
    /// Attempted to create a segment that already exists.
    #[error("Segment already exists")]
    SegmentAlreadyExists,
    /// A frame spans across two WAL segments, which is not allowed.
    #[error("Segment frame split")]
    SegmentFrameSplit,
    /// A segment file has an invalid name that cannot be parsed into a segment key.
    #[error("Invalid segment key: {0}")]
    InvalidSegmentKey(String),
    /// Frame codec error.
    #[error("Frame codec: {0}")]
    FrameCodec(#[from] FrameCodecError),
    /// I/O error.
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
}

impl From<Error> for raft::StorageError {
    fn from(err: Error) -> raft::StorageError {
        raft::StorageError::Other(Box::new(err))
    }
}

impl From<Error> for raft::Error {
    fn from(err: Error) -> raft::Error {
        raft::StorageError::from(err).into()
    }
}
