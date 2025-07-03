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
    alloc::{Layout, alloc_zeroed, dealloc},
    ptr::NonNull,
    sync::OnceLock,
};

/// Lazily initialized system page size in bytes.
///
/// Determined using the `libc::sysconf` function with `_SC_PAGESIZE`.
static PAGE_SIZE: OnceLock<usize> = OnceLock::new();

/// Returns the system memory page size in bytes.
///
/// The result is cached after the first call using a thread-safe static initializer.
///
/// Panics if `sysconf(_SC_PAGESIZE)` returns a non-positive value.
#[inline]
pub fn page_size() -> usize {
    *PAGE_SIZE.get_or_init(|| {
        // SAFETY: `sysconf` is safe to call with `_SC_PAGESIZE`.
        let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        assert!(size > 0, "sysconf(_SC_PAGESIZE) must return a positive value");
        size as usize
    })
}

/// Computes the number of whole memory pages required to hold a given number of bytes.
///
/// The returned value is the ceiling of `bytes / page_size()`.
#[inline]
pub fn pages_for(bytes: usize) -> usize {
    bytes.div_ceil(page_size())
}

/// A heap-allocated, page-aligned, zero-initialized memory block.
#[derive(Debug)]
pub struct PageBlock {
    ptr: NonNull<u8>,
    layout: Layout,
    size: usize,
}

impl PageBlock {
    /// Allocates a new [`PageBlock`] containing the given number of memory pages.
    ///
    /// The memory is zero-initialized and aligned to the system page size.
    ///
    /// # Panics
    ///
    /// Panics if `pages` is zero or if the allocation fails.
    pub fn new(pages: usize) -> PageBlock {
        assert!(pages > 0);
        let page_size = page_size();
        let size = pages * page_size;
        let layout = Layout::from_size_align(size, page_size).unwrap();
        let ptr = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).unwrap();
        PageBlock { ptr, layout, size }
    }

    /// Returns a mutable byte slice covering the entire allocated memory region.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }

    /// Returns an immutable byte slice covering the entire allocated memory region.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    /// Returns the total size of the memory block in bytes.
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns the number of memory pages contained in this block.
    #[inline]
    pub fn pages(&self) -> usize {
        pages_for(self.size)
    }

    /// Returns a new [`PageBlock`] with additional pages appended to the existing data.
    ///
    /// The new block is zero-initialized and contains a copy of the existing memory.
    /// The original block is consumed.
    ///
    /// # Panics
    ///
    /// Panics if `pages` is zero or if allocation fails.
    #[inline]
    pub fn grow(self, pages: usize) -> PageBlock {
        assert!(pages > 0);
        let page_sz = page_size();
        let new_size = self.size + pages * page_sz;
        let layout = Layout::from_size_align(new_size, page_sz).unwrap();

        let new_ptr = unsafe { alloc_zeroed(layout) };
        let new_ptr = NonNull::new(new_ptr).unwrap();

        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr.as_ptr(), new_ptr.as_ptr(), self.size);
        }

        PageBlock { ptr: new_ptr, layout, size: new_size }
    }

    /// Creates a new [`PageBlock`] containing only the last `pages` pages from this block.
    ///
    /// Useful for reducing memory while preserving recent or ending data.
    ///
    /// The original block is consumed.
    ///
    /// # Panics
    ///
    /// Panics if `pages` is zero or exceeds the number of pages in the original block.
    #[inline]
    pub fn purge(self, pages: usize) -> PageBlock {
        assert!(pages > 0);
        let page_size = page_size();
        let keep_size = pages * page_size;
        assert!(self.size >= keep_size);

        let mut new_block = PageBlock::new(pages);
        let offset = self.size - keep_size;

        let src = unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().add(offset), keep_size) };
        let dst = new_block.as_mut_slice();
        dst.copy_from_slice(src);

        new_block
    }
}

impl Drop for PageBlock {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

/// A resizable buffer built on top of [`PageBlock`] with automatic growth.
///
/// The buffer writes sequential data into a page-aligned memory block, automatically growing the
/// backing allocation as needed.
#[derive(Debug)]
pub struct PageBlockBuf {
    block: Option<PageBlock>,
    offset: usize,
}

impl PageBlockBuf {
    /// Creates a new [`PageBlockBuf`] with a single memory page.
    pub fn new() -> PageBlockBuf {
        PageBlockBuf { block: Some(PageBlock::new(1)), offset: 0 }
    }

    #[inline]
    pub fn from_page_block(block: PageBlock, offset: usize) -> PageBlockBuf {
        PageBlockBuf { block: Some(block), offset }
    }

    /// Returns a reference to the underlying page-aligned memory block.
    ///
    /// This allows access to the full allocated memory, not just the written portion.
    pub fn block(&self) -> &PageBlock {
        self.block.as_ref().unwrap()
    }

    /// Returns a mutable slice of `num_bytes` starting at the current write position.
    ///
    /// Grows the underlying buffer if necessary. Advances the internal offset by `num_bytes`.
    ///
    /// # Panics
    ///
    /// Panics if `num_bytes == 0`.
    #[inline]
    pub fn as_mut_slice(&mut self, num_bytes: usize) -> &mut [u8] {
        assert!(num_bytes > 0);

        let needed = self.offset + num_bytes;

        if needed > self.block.as_ref().unwrap().size() {
            let block = self.block.take().unwrap();
            let additional = pages_for(needed - block.size());
            self.block = Some(block.grow(additional));
        };

        let slice =
            &mut self.block.as_mut().unwrap().as_mut_slice()[self.offset..self.offset + num_bytes];
        self.offset += num_bytes;
        slice
    }

    /// Returns an immutable slice of the written portion of the buffer.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.block.as_ref().unwrap().as_slice()[..self.offset]
    }

    /// Returns the total number of bytes written to the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.offset
    }

    /// Returns true if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.offset == 0
    }

    /// Keeps only the last `keep_bytes` of data, discarding earlier data.
    ///
    /// If `keep_bytes >= self.len()`, this is a no-op.
    #[inline]
    pub fn purge(&mut self, keep_bytes: usize) {
        if keep_bytes >= self.offset {
            return;
        }

        let mut block = self.block.take().unwrap();
        block = block.purge(pages_for(keep_bytes.max(1)));

        self.offset = keep_bytes;
        self.block = Some(block);
    }

    /// Clears the buffer without freeing memory.
    #[inline]
    pub fn clear(&mut self) {
        self.offset = 0;
    }
}

impl Default for PageBlockBuf {
    fn default() -> PageBlockBuf {
        PageBlockBuf::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pages_for() {
        assert_eq!(pages_for(0), 0);
        assert_eq!(pages_for(1), 1);
        assert_eq!(pages_for(page_size()), 1);
        assert_eq!(pages_for(page_size() + 1), 2);
        assert_eq!(pages_for(page_size() * 3), 3);
    }

    #[test]
    fn test_page_block_size() {
        let block = PageBlock::new(2);
        assert_eq!(block.size, 2 * page_size());
    }

    #[test]
    fn test_page_block_zeroed_and_aligned() {
        let block = PageBlock::new(1);
        let slice = block.as_slice();
        assert_eq!(slice.len(), page_size());
        assert!(slice.iter().all(|&b| b == 0));
    }

    #[test]
    #[should_panic(expected = "assertion failed: pages > 0")]
    fn test_zero_pages_should_panic() {
        let _ = PageBlock::new(0);
    }

    #[test]
    fn test_multiple_blocks_are_independent() {
        let mut block1 = PageBlock::new(1);
        let mut block2 = PageBlock::new(1);

        block1.as_mut_slice()[0] = 42;
        block2.as_mut_slice()[0] = 99;

        assert_eq!(block1.as_slice()[0], 42);
        assert_eq!(block2.as_slice()[0], 99);
    }

    #[test]
    fn test_alignment_is_correct() {
        let block = PageBlock::new(1);
        let ptr = block.as_slice().as_ptr() as usize;
        let alignment = page_size();

        assert_eq!(ptr % alignment, 0, "memory is not page-aligned");
    }

    #[test]
    fn test_large_allocation() {
        let block = PageBlock::new(16);
        let slice = block.as_slice();
        assert_eq!(slice.len(), page_size() * 16);
        assert!(slice.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_page_block_buf_write_and_read() {
        let mut buf = PageBlockBuf::new();
        let data = b"hello world";
        let slice = buf.as_mut_slice(data.len());
        slice.copy_from_slice(data);
        assert_eq!(buf.as_slice(), data);
        assert_eq!(buf.len(), data.len());
    }

    #[test]
    fn test_page_block_buf_grow() {
        let mut buf = PageBlockBuf::new();
        let large_size = page_size() * 2;
        let slice = buf.as_mut_slice(large_size);
        assert_eq!(slice.len(), large_size);
        assert_eq!(buf.len(), large_size);
    }

    #[test]
    fn test_page_block_buf_purge_partial() {
        let mut buf = PageBlockBuf::new();
        let data = b"1234567890";
        let _ = buf.as_mut_slice(data.len()).copy_from_slice(data);
        buf.purge(4);
        assert_eq!(buf.len(), 4);
        let result = buf.as_slice();
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_page_block_buf_purge_all() {
        let mut buf = PageBlockBuf::new();
        let data = b"abc";
        let _ = buf.as_mut_slice(data.len()).copy_from_slice(data);
        buf.purge(10); // more than current len
        assert_eq!(buf.len(), data.len());
        assert_eq!(buf.as_slice(), data);
    }

    #[test]
    fn test_page_block_buf_clear() {
        let mut buf = PageBlockBuf::new();
        let _ = buf.as_mut_slice(4);
        assert!(!buf.is_empty());
        buf.clear();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    #[should_panic(expected = "assertion failed: num_bytes > 0")]
    fn test_page_block_buf_zero_write_should_panic() {
        let mut buf = PageBlockBuf::new();
        let _ = buf.as_mut_slice(0);
    }
}
