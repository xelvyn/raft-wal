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
    alloc::System,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use raft_wal::page_block::PageBlock;
use rand::{Rng, rng};
use tracking_allocator::{AllocationGroupId, AllocationRegistry, AllocationTracker, Allocator};

#[global_allocator]
static GLOBAL: Allocator<System> = Allocator::from_allocator(System);

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

struct Tracker;

impl AllocationTracker for Tracker {
    fn allocated(
        &self,
        _addr: usize,
        object_size: usize,
        _wrapped_size: usize,
        _group_id: AllocationGroupId,
    ) {
        ALLOCATED.fetch_add(object_size, Ordering::Relaxed);
    }

    fn deallocated(
        &self,
        _addr: usize,
        object_size: usize,
        _wrapped_size: usize,
        _source_group_id: AllocationGroupId,
        _current_group_id: AllocationGroupId,
    ) {
        ALLOCATED.fetch_sub(object_size, Ordering::Relaxed);
    }
}

static INIT: OnceLock<()> = OnceLock::new();

#[test]
fn test_page_block_no_leak() {
    INIT.get_or_init(|| {
        AllocationRegistry::set_global_tracker(Tracker).unwrap();
        AllocationRegistry::enable_tracking();
    });

    let mut rng = rng();
    for _ in 0..1000 {
        let baseline = ALLOCATED.load(Ordering::Relaxed);
        {
            let pages = rng.random_range(1..10);
            let mut block = PageBlock::new(pages);
            let _as_slice = block.as_slice();
            let _as_mut_slice = block.as_mut_slice();
        };
        let leak = ALLOCATED.load(Ordering::Relaxed).wrapping_sub(baseline);
        assert_eq!(leak, 0);
    }
}
