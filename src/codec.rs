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

use bytes::{Buf, BufMut};
use prost::Message;
use raft::eraftpb::{Entry, HardState};

use crate::{
    error::{FrameCodecError, RecordCodecError},
    types::{Frame, FrameHeader, FrameRef, Record, RecordRef, RecordType},
};

/// Frame alignment in bytes.
pub(crate) const FRAME_ALIGN: u64 = 8;
/// Number of bytes used to encode the record length (u64).
pub(crate) const FRAME_RECORD_LEN_FIELD_LEN: u64 = 8;
/// Number of bytes used to encode the CRC checksum (u32).
pub(crate) const FRAME_CRC_FIELD_LEN: u64 = 4;
/// Total size of the frame header.
pub(crate) const FRAME_HEADER_LEN: u64 = FRAME_RECORD_LEN_FIELD_LEN + FRAME_CRC_FIELD_LEN;

impl<'a> RecordRef<'a> {
    /// Returns encoded length of record.
    pub(crate) fn encoded_len(&self) -> u64 {
        match self {
            RecordRef::Entry(e) => (e.encoded_len() + 1) as u64,
            RecordRef::HardState(h) => (h.encoded_len() + 1) as u64,
        }
    }

    /// Encodes the [`RecordRef`] into the provided buffer.
    pub(crate) fn encode<B>(self, buf: &mut B) -> Result<(), RecordCodecError>
    where
        B: BufMut,
    {
        let required = self.encoded_len();
        let remaining = buf.remaining_mut() as u64;
        if remaining < required {
            return Err(RecordCodecError::BufTooSmallToFit { required, remaining });
        };

        match self {
            RecordRef::Entry(e) => {
                buf.put_u8(RecordType::Entry as u8);
                // SAFETY: Length checked at the line above.
                e.encode_raw(buf);
                Ok(())
            },
            RecordRef::HardState(h) => {
                buf.put_u8(RecordType::HardState as u8);
                // SAFETY: Length checked at the line above.
                h.encode_raw(buf);
                Ok(())
            },
        }
    }
}

impl Record {
    /// Decodes a [`Record`] from the given buffer.
    pub(crate) fn decode<B>(mut buf: B) -> Result<Record, RecordCodecError>
    where
        B: Buf,
    {
        let required = 1;
        let remaining = buf.remaining() as u64;
        if required > remaining {
            return Err(RecordCodecError::BufTooSmallToRead { required, remaining });
        };
        // SAFETY: Length checked at the line above.
        let record_type = buf.get_u8();
        match RecordType::try_from(record_type)? {
            RecordType::Entry => {
                let entry = Entry::decode(buf).map_err(RecordCodecError::EntryDecode)?;
                Ok(Record::Entry(entry))
            },
            RecordType::HardState => {
                let hard_state =
                    HardState::decode(buf).map_err(RecordCodecError::HardStateDecode)?;
                Ok(Record::HardState(hard_state))
            },
        }
    }
}

impl Frame {
    /// Returns encoded length of frame.
    #[inline]
    pub(crate) fn encoded_len(&self) -> u64 {
        self.header.need_len_for_record() + FRAME_HEADER_LEN
    }

    /// Decodes a [`Frame`] from the given buffer and a previously parsed [`FrameHeader`].
    pub(crate) fn decode(buf: &[u8], header: FrameHeader) -> Result<Frame, FrameCodecError> {
        let required = header.need_len_for_record();
        let remaining = buf.len() as u64;
        if remaining < required {
            return Err(FrameCodecError::BufTooSmallToReadFrame { required, remaining });
        };

        let record = &buf[..header.record_len as usize];
        if crc32fast::hash(record) != header.crc {
            return Err(FrameCodecError::CrcMismatch);
        };

        let record = Record::decode(record)?;

        Ok(Frame { header, record })
    }
}
impl<'a> FrameRef<'a> {
    /// Returns encoded length of frame.
    #[inline]
    pub(crate) fn encoded_len(&self) -> u64 {
        (FRAME_HEADER_LEN + self.0.encoded_len()).div_ceil(FRAME_ALIGN) * FRAME_ALIGN
    }

    /// Encodes the frame (header + record) into the provided buffer.
    pub(crate) fn encode(self, buf: &mut [u8]) -> Result<(), FrameCodecError> {
        let required = self.encoded_len();
        let remaining = buf.len() as u64;
        if remaining < required {
            return Err(FrameCodecError::BufTooSmallToFit { required, remaining });
        };

        let record_len = self.0.encoded_len();

        self.0.encode(
            &mut &mut buf[FRAME_HEADER_LEN as usize..(FRAME_HEADER_LEN + record_len) as usize],
        )?;

        let crc = crc32fast::hash(
            &buf[FRAME_HEADER_LEN as usize..(FRAME_HEADER_LEN + record_len) as usize],
        );

        buf[..FRAME_RECORD_LEN_FIELD_LEN as usize].copy_from_slice(&record_len.to_le_bytes());
        buf[FRAME_RECORD_LEN_FIELD_LEN as usize..FRAME_HEADER_LEN as usize]
            .copy_from_slice(&crc.to_le_bytes());

        Ok(())
    }
}

impl FrameHeader {
    /// Returns number of bytes to allocate to read record.
    #[inline]
    pub(crate) fn need_len_for_record(&self) -> u64 {
        ((FRAME_HEADER_LEN + self.record_len).div_ceil(FRAME_ALIGN) * FRAME_ALIGN)
            - FRAME_HEADER_LEN
    }

    /// Decodes a [`FrameHeader`] from the beginning of the given buffer.
    pub(crate) fn decode(buf: &[u8]) -> Result<FrameHeader, FrameCodecError> {
        let remaining = buf.len() as u64;
        if remaining < FRAME_HEADER_LEN {
            return Err(FrameCodecError::BufTooSmallToReadFrameHeader {
                required: FRAME_HEADER_LEN,
                remaining,
            });
        };
        // SAFETY: Length checked at the line above.
        let record_len_bytes = buf[..FRAME_RECORD_LEN_FIELD_LEN as usize].try_into().unwrap();
        let record_len = u64::from_le_bytes(record_len_bytes);
        // SAFETY: Length checked at the line above.
        let crc_bytes = buf[FRAME_RECORD_LEN_FIELD_LEN as usize..FRAME_HEADER_LEN as usize]
            .try_into()
            .unwrap();
        let crc = u32::from_le_bytes(crc_bytes);
        Ok(FrameHeader { record_len, crc })
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use raft::{
        eraftpb::{Entry, EntryType},
        prelude::HardState,
    };

    use super::*;

    #[test]
    fn test_frame_ref_encode_and_decode_entry() {
        let mut entry = Entry::default();
        entry.term = 1;
        entry.index = 2;
        entry.entry_type = EntryType::EntryNormal as i32;
        entry.data = b"entry data".to_vec();

        let rec_ref = RecordRef::Entry(&entry);
        let frame_ref = FrameRef(rec_ref);
        let mut buf = vec![0u8; frame_ref.encoded_len() as usize];
        frame_ref.encode(&mut buf[..]).unwrap();

        let header = FrameHeader::decode(&buf[..]).unwrap();
        let frame = Frame::decode(&buf[FRAME_HEADER_LEN as usize..], header).unwrap();

        match frame.record {
            Record::Entry(e) => assert_eq!(e, entry),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_frame_ref_encode_and_decode_hard_state() {
        let mut hs = HardState::default();
        hs.term = 3;
        hs.vote = 1;
        hs.commit = 10;

        let rec_ref = RecordRef::HardState(&hs);
        let frame_ref = FrameRef(rec_ref);
        let mut buf = vec![0u8; frame_ref.encoded_len() as usize];
        frame_ref.encode(&mut buf[..]).unwrap();

        let header = FrameHeader::decode(&buf[..]).unwrap();
        let frame = Frame::decode(&buf[FRAME_HEADER_LEN as usize..], header).unwrap();

        match frame.record {
            Record::HardState(h) => assert_eq!(h, hs),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_frame_header_decode_too_small() {
        let buf = vec![0u8; (FRAME_HEADER_LEN - 1) as usize];
        let _ = FrameHeader::decode(&buf[..]).unwrap_err();
    }

    #[test]
    fn test_frame_decode_fails_on_crc_mismatch() {
        let mut entry = Entry::default();
        entry.term = 1;
        entry.index = 2;
        entry.data = b"data".to_vec();

        let rec_ref = RecordRef::Entry(&entry);
        let frame_ref = FrameRef(rec_ref);
        let mut buf = vec![0u8; frame_ref.encoded_len() as usize];
        frame_ref.encode(&mut buf[..]).unwrap();

        let header = FrameHeader::decode(&buf[..]).unwrap();
        let mut tampered = buf.clone();
        let start = FRAME_HEADER_LEN as usize;
        tampered[start] ^= 0xFF;

        let _ = Frame::decode(&tampered[start..], header).unwrap_err();
    }

    #[test]
    fn test_frame_ref_encode_fails_if_too_small() {
        let mut entry = Entry::default();
        entry.term = 1;
        entry.index = 2;
        entry.data = b"data".to_vec();

        let rec_ref = RecordRef::Entry(&entry);
        let frame_ref = FrameRef(rec_ref);
        let too_small = (frame_ref.encoded_len() - 1) as usize;
        let mut buf = vec![0u8; too_small];

        let _ = frame_ref.encode(&mut buf[..]).unwrap_err();
    }

    #[test]
    fn test_frame_decode_fails_if_buf_too_small() {
        let mut entry = Entry::default();
        entry.term = 1;
        entry.index = 2;
        entry.data = b"data".to_vec();

        let rec_ref = RecordRef::Entry(&entry);
        let frame_ref = FrameRef(rec_ref);
        let mut buf = vec![0u8; frame_ref.encoded_len() as usize];
        frame_ref.encode(&mut buf[..]).unwrap();

        let header = FrameHeader::decode(&buf[..]).unwrap();
        let too_short =
            &buf[FRAME_HEADER_LEN as usize..(FRAME_HEADER_LEN + header.record_len - 1) as usize];

        let _ = Frame::decode(too_short, header).unwrap_err();
    }

    #[test]
    fn test_entry_roundtrip() {
        let mut entry = Entry::default();
        entry.index = 42;
        entry.term = 7;
        entry.entry_type = EntryType::EntryNormal as i32;
        entry.data = b"test entry".to_vec();

        let rec = RecordRef::Entry(&entry);
        let mut buf = BytesMut::with_capacity(rec.encoded_len() as usize);
        rec.encode(&mut buf).unwrap();

        let decoded = Record::decode(&buf[..]).unwrap();
        match decoded {
            Record::Entry(e) => assert_eq!(e, entry),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_hard_state_roundtrip() {
        let mut hs = HardState::default();
        hs.term = 7;
        hs.vote = 1;
        hs.commit = 10;

        let rec = RecordRef::HardState(&hs);
        let mut buf = BytesMut::with_capacity(rec.encoded_len() as usize);
        rec.encode(&mut buf).unwrap();

        let decoded = Record::decode(&buf[..]).unwrap();
        match decoded {
            Record::HardState(h) => assert_eq!(h, hs),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_decode_fails_on_empty() {
        let buf = BytesMut::new();
        let _ = Record::decode(&buf[..]).unwrap_err();
    }

    #[test]
    fn test_decode_fails_on_unknown_tag() {
        let mut buf = BytesMut::with_capacity(1);
        buf.put_u8(0xFF);
        let _ = Record::decode(&buf[..]).unwrap_err();
    }
}
