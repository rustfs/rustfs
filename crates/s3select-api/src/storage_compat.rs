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

pub(crate) mod ecstore {
    pub(crate) use rustfs_ecstore::{error, resolve_object_store_handle, set_disk, store, store_api};
}

use ecstore::store_api::{
    GetObjectReader as EcstoreGetObjectReader, ObjectInfo as EcstoreObjectInfo, ObjectOptions as EcstoreObjectOptions,
};

pub(crate) type SelectGetObjectReader = EcstoreGetObjectReader;
pub(crate) type SelectObjectInfo = EcstoreObjectInfo;
pub(crate) type SelectObjectOptions = EcstoreObjectOptions;
