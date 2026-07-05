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

//! Home for the `SetDisks` operation families extracted during the
//! God-Object split (tracking #815). Each family owns its module here and
//! borrows shared state through [`super::ctx::SetDisksCtx`]; the storage-api
//! contract impls stay `for SetDisks`, so contract bounds are unchanged.

pub(crate) mod bucket;
pub(crate) mod heal;
pub(crate) mod list;
pub(crate) mod multipart;
pub(crate) mod object;
