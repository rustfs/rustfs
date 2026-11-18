// Copyright 2024 RustFS Team
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

//! Test-only seams over the use-case entry points that tests of other operation families call as an oracle.
//!
//! Each seam is owned by the batch that owns its operation. A test outside that family calls the seam, never the
//! use-case method, so a signature change touches one seam body instead of another family's tests.
//! `scripts/check_test_seams.py` enforces this.

mod get_object;
mod head_object;
mod list_objects_v2;
mod put_object;

pub(crate) use get_object::get_object;
pub(crate) use head_object::head_object;
pub(crate) use list_objects_v2::list_objects_v2;
pub(crate) use put_object::put_object;
