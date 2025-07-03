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

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    fs,
    fs::{File, OpenOptions},
    os::{fd::AsRawFd, unix::fs::FileExt},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, OnceLock, Weak},
};

use crate::{
    error::{Error, Result},
    page_block::{PageBlock, page_size},
    types::{RaftIndex, SegmentLen, SegmentNumber, SegmentOffset},
};

const PREFERRED_SEGMENT_SIZE: u64 = 1024 * 1024 * 64;
static SEGMENT_SIZE: OnceLock<u64> = OnceLock::new();

pub fn segment_size() -> u64 {
    *SEGMENT_SIZE
        .get_or_init(|| PREFERRED_SEGMENT_SIZE - (PREFERRED_SEGMENT_SIZE % page_size() as u64))
}

/// WAL segment that reads and writes data using page-aligned blocks.
#[derive(Debug)]
pub(crate) struct Segment {
    file: File,
}

impl Segment {
    /// Creates a new [`Segment`] at the specified `path`, preallocating `allocate` bytes.
    ///
    /// Returns [`Error::SegmentAlreadyExists`] if a segment already exists at the given path.
    pub(crate) fn create(path: impl AsRef<Path>, allocate: u64) -> Result<Segment> {
        let path = path.as_ref();
        if fs::exists(path)? {
            return Err(Error::SegmentAlreadyExists);
        };

        let file = Segment::file_open_options(true).open(path)?;
        file.set_len(allocate)?;
        Segment::fcntl(&file)?;

        Ok(Segment { file })
    }

    /// Opens an existing [`Segment`] from the specified `path`.
    ///
    /// Returns [`Error::SegmentNotFound`] if no segment is found at the given path.
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Segment> {
        let path = path.as_ref();
        if !fs::exists(path)? {
            return Err(Error::SegmentNotFound);
        };

        let file = Segment::file_open_options(false).open(path)?;
        Segment::fcntl(&file)?;

        Ok(Segment { file })
    }

    /// Writes the given [`PageBlock`] to the segment at the specified offset.
    #[inline]
    pub(crate) fn write_at(&self, block: &PageBlock, offset: u64) -> Result<()> {
        self.file.write_all_at(block.as_slice(), offset)?;
        Ok(())
    }

    /// Reads data into the provided [`PageBlock`] from the segment at the specified offset.
    #[inline]
    pub(crate) fn read_at(&self, block: &mut PageBlock, offset: u64) -> Result<()> {
        self.file.read_exact_at(block.as_mut_slice(), offset)?;
        Ok(())
    }

    /// Flushes written data to the underlying storage using `fsync`.
    #[inline]
    pub(crate) fn fsync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Returns length of this [`Segment`].
    #[inline]
    pub(crate) fn len(&self) -> Result<SegmentLen> {
        Ok(self.file.metadata()?.len())
    }

    #[inline]
    pub(crate) fn set_len(&self, len: SegmentLen) -> Result<()> {
        self.file.set_len(len)?;
        Ok(())
    }

    /// Returns configured [`OpenOptions`] for creating or opening a segment.
    fn file_open_options(create: bool) -> OpenOptions {
        let mut opts = OpenOptions::new();

        opts.read(true).write(true).create(create);

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;

            opts.custom_flags(libc::O_DIRECT);
        };

        opts
    }

    /// Applies platform-specific configuration to the file to control caching behavior.
    fn fcntl(_file: &File) -> Result<()> {
        #[cfg(target_os = "macos")]
        {
            let fd = _file.as_raw_fd();
            let ret = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };

            if ret == -1 {
                let err = std::io::Error::last_os_error();
                return Err(Error::Io(err));
            }
        }

        Ok(())
    }
}

/// Key of segment.
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub(crate) struct SegmentKey {
    pub(crate) number: SegmentNumber,
    pub(crate) first_index: RaftIndex,
}

impl SegmentKey {
    /// Creates a new [`SegmentKey`] instance.
    pub(crate) fn new(number: SegmentNumber, first_index: RaftIndex) -> SegmentKey {
        SegmentKey { number, first_index }
    }
}

impl FromStr for SegmentKey {
    type Err = Error;

    fn from_str(s: &str) -> Result<SegmentKey> {
        let (number, first_index) =
            s.split_once('-').ok_or_else(|| Error::InvalidSegmentKey(s.to_owned()))?;

        Ok(SegmentKey {
            number: SegmentNumber::from_str_radix(number, 16)
                .map_err(|_| Error::InvalidSegmentKey(s.to_owned()))?,
            first_index: RaftIndex::from_str_radix(first_index, 16)
                .map_err(|_| Error::InvalidSegmentKey(s.to_owned()))?,
        })
    }
}

impl Display for SegmentKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}-{:016x}", self.number, self.first_index)
    }
}

/// A smart wrapper around a segment that tracks its usage.
///
/// When all [`Arc`] references to the underlying segment are dropped, this structure ensures it is
/// removed from the manager's tracking map automatically.
#[derive(Debug, Clone)]
pub(crate) struct TrackedSegment {
    key: SegmentKey,
    inner: Arc<Segment>,
    segment_manager: SegmentManager,
}

impl TrackedSegment {
    /// Creates a new [`TrackedSegment`] instance.
    fn new(inner: Arc<Segment>, key: SegmentKey, segmentmgr: SegmentManager) -> TrackedSegment {
        TrackedSegment { key, inner, segment_manager: segmentmgr }
    }

    /// Returns the [`SegmentKey`] identifying this segment.
    #[inline]
    pub(crate) fn key(&self) -> SegmentKey {
        self.key
    }

    /// Writes a [`PageBlock`] to the segment at the given offset.
    #[inline]
    pub(crate) fn write_at(&self, block: &PageBlock, offset: SegmentOffset) -> Result<()> {
        self.inner.write_at(block, offset)
    }

    /// Reads into a [`PageBlock`] from the segment at the given offset.
    #[inline]
    pub(crate) fn read_at(&self, block: &mut PageBlock, offset: SegmentOffset) -> Result<()> {
        self.inner.read_at(block, offset)
    }

    /// Flushes the segment's data to disk using `fsync`.
    #[inline]
    pub(crate) fn fsync(&self) -> Result<()> {
        self.inner.fsync()
    }

    /// Returns the current length (in bytes) of the segment.
    #[inline]
    pub(crate) fn len(&self) -> Result<SegmentLen> {
        self.inner.len()
    }

    #[inline]
    pub(crate) fn set_len(&self, len: SegmentLen) -> Result<()> {
        self.inner.set_len(len)
    }
}

impl Drop for TrackedSegment {
    fn drop(&mut self) {
        // Let the manager decide if the segment can be safely untracked.
        self.segment_manager.free_segment(self);
    }
}

/// WAL segment manager.
#[derive(Debug, Clone)]
pub(crate) struct SegmentManager {
    path: PathBuf,
    segments: Arc<Mutex<HashMap<SegmentKey, Weak<Segment>>>>,
}

impl SegmentManager {
    pub(crate) fn new(path: impl AsRef<Path>) -> Result<SegmentManager> {
        Ok(SegmentManager {
            path: path.as_ref().to_path_buf(),
            segments: Arc::new(Default::default()),
        })
    }

    /// Creates a new segment file and returns a [`TrackedSegment`] handle to it.
    pub(crate) fn create_segment(
        &self,
        key: SegmentKey,
        allocate: SegmentLen,
    ) -> Result<TrackedSegment> {
        let mut segments = self.segments.lock().expect("mutex poisoned");

        // Skipping the check for whether the segment is already open or present on disk.
        // Segment::create will fail if the file already exists.
        let segment = Arc::new(Segment::create(self.path.join(key.to_string()), allocate)?);
        segments.insert(key, Arc::downgrade(&segment));

        Ok(TrackedSegment { inner: segment, key, segment_manager: self.clone() })
    }

    /// Opens a segment by its key.
    pub(crate) fn open_segment(&self, key: SegmentKey) -> Result<TrackedSegment> {
        let mut segments = self.segments.lock().expect("mutex poisoned");
        SegmentManager::open_inner(self.clone(), key, &mut segments)
    }

    /// Opens all segments.
    pub(crate) fn open_segments(&self) -> Result<Vec<TrackedSegment>> {
        let mut segments = self.segments.lock().expect("mutex poisoned");
        let segment_keys = self.get_segment_keys()?;
        let mut result = Vec::with_capacity(segment_keys.len());
        for segment_key in segment_keys {
            let segment = SegmentManager::open_inner(self.clone(), segment_key, &mut segments)?;
            result.push(segment);
        }
        Ok(result)
    }

    /// Internal helper that encapsulates shared logic for opening segments.
    fn open_inner(
        this: SegmentManager,
        key: SegmentKey,
        segments: &mut HashMap<SegmentKey, Weak<Segment>>,
    ) -> Result<TrackedSegment> {
        if let Some(segment) = segments.get(&key) {
            if let Some(segment) = segment.upgrade() {
                return Ok(TrackedSegment::new(segment, key, this));
            };
        };
        let segment = Arc::new(Segment::open(this.path.join(key.to_string()))?);
        segments.insert(key, Arc::downgrade(&segment));
        Ok(TrackedSegment::new(segment, key, this))
    }

    /// Scans a directory and returns all segments keys.
    pub(crate) fn get_segment_keys(&self) -> Result<Vec<SegmentKey>> {
        let mut segment_keys = Vec::new();
        for result in fs::read_dir(&self.path)? {
            let entry = result?;
            let filename = entry.file_name().to_str().unwrap_or_default().to_owned();
            let segment_key = SegmentKey::from_str(&filename)?;
            segment_keys.push(segment_key);
        }
        segment_keys.sort();
        Ok(segment_keys)
    }

    /// Frees a segment if it's no longer actively referenced.
    ///
    /// This is called by [`TrackedSegment`] in its `Drop` implementation.
    fn free_segment(&self, segment: &TrackedSegment) {
        // 1. Acquire write lock to ensure exclusive access to the segment map.
        let mut segments = self.segments.lock().expect("mutex poisoned");

        // 2. If the only remaining strong reference is this TrackedSegment, then it's safe to
        //    remove the weak reference from the manager.
        if let Some(weak) = segments.get(&segment.key()) {
            if weak.strong_count() == 1 {
                segments.remove(&segment.key());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::page_block::{PageBlock, page_size};

    #[test]
    fn test_segment_create_and_open() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("segment1");

        let allocate_size = page_size() as u64;
        let _segment = Segment::create(&file_path, allocate_size).unwrap();

        let err = Segment::create(&file_path, allocate_size).unwrap_err();
        assert!(matches!(err, Error::SegmentAlreadyExists));

        let opened_segment = Segment::open(&file_path).unwrap();

        opened_segment.fsync().unwrap();
    }

    #[test]
    fn test_segment_open_not_found() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("nonexistent_segment");

        let err = Segment::open(&file_path).unwrap_err();
        assert!(matches!(err, Error::SegmentNotFound));
    }

    #[test]
    fn test_segment_write_and_read() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("segment_rw");

        let page_size = page_size();
        let segment = Segment::create(&file_path, page_size as u64).unwrap();

        let mut write_block = PageBlock::new(1);
        let content = b"hello world";
        write_block.as_mut_slice()[..content.len()].copy_from_slice(content);

        segment.write_at(&write_block, 0).unwrap();

        let mut read_block = PageBlock::new(1);
        segment.read_at(&mut read_block, 0).unwrap();
        assert_eq!(&read_block.as_slice()[..content.len()], content);
    }

    #[test]
    fn test_segment_key_display_and_parse() {
        let key = SegmentKey::new(1, 42);
        let key_str = key.to_string();
        assert_eq!(key_str, "0000000000000001-000000000000002a");

        let parsed = SegmentKey::from_str(&key_str).unwrap();
        assert_eq!(parsed, key);
    }

    #[test]
    fn test_segment_mgr_create_and_open_segment() {
        let dir = tempdir().unwrap();
        let mgr = SegmentManager::new(dir.path()).unwrap();
        let key = SegmentKey::new(2, 100);
        let segment = mgr.create_segment(key, page_size() as u64).unwrap();

        assert_eq!(segment.key(), key);
        assert_eq!(segment.len().unwrap(), page_size() as u64);

        drop(segment);

        let reopened = mgr.open_segment(key).unwrap();
        assert_eq!(reopened.key(), key);
    }

    #[test]
    fn test_segment_manager_open_segments_scans_correctly() {
        let dir = tempdir().unwrap();
        let mgr = SegmentManager::new(dir.path()).unwrap();

        let key1 = SegmentKey::new(1, 100);
        let key2 = SegmentKey::new(2, 200);

        mgr.create_segment(key1, page_size() as u64).unwrap();
        mgr.create_segment(key2, page_size() as u64).unwrap();

        let opened = mgr.open_segments().unwrap();
        let keys: Vec<_> = opened.iter().map(|s| s.key()).collect();

        assert_eq!(keys, vec![key1, key2]);
    }

    #[test]
    fn test_segment_manager_get_segment_keys_fails_on_invalid_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("not_a_segment.txt");
        fs::write(&path, "irrelevant").unwrap();

        let mgr = SegmentManager::new(dir.path()).unwrap();
        let result = mgr.get_segment_keys();
        assert!(matches!(result, Err(Error::InvalidSegmentKey(_))));
    }
}
