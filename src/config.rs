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

use serde::{Deserialize, Serialize};

/// The segment number assigned to the very first segment in the WAL.
pub const INITIAL_SEGMENT_NUMBER: u64 = 1;

/// The index of the first entry in the initial WAL segment.
pub const INITIAL_SEGMENT_FIRST_INDEX: u64 = 1;

/// The initial value of `last_entry_index` before any entries are appended.
pub const INITIAL_LAST_ENTRY_INDEX: u64 = 1;

/// Configures a WAL.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {}
