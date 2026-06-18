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

use http::HeaderMap;
use rustfs_ecstore::{
    error::Error,
    store_api::{
        GetObjectReader as EcstoreGetObjectReader, ObjectInfo as EcstoreObjectInfo, ObjectOptions as EcstoreObjectOptions,
        ObjectToDelete as EcstoreObjectToDelete, PutObjReader as EcstorePutObjReader,
    },
};
use rustfs_storage_api::{HTTPRangeSpec, ObjectIO};

pub type ScannerGetObjectReader = EcstoreGetObjectReader;
pub type ScannerObjectInfo = EcstoreObjectInfo;
pub type ScannerObjectOptions = EcstoreObjectOptions;
pub type ScannerObjectToDelete = EcstoreObjectToDelete;
pub type ScannerPutObjReader = EcstorePutObjReader;

pub trait ScannerObjectIO:
    ObjectIO<
        Error = Error,
        RangeSpec = HTTPRangeSpec,
        HeaderMap = HeaderMap,
        ObjectOptions = ScannerObjectOptions,
        ObjectInfo = ScannerObjectInfo,
        GetObjectReader = ScannerGetObjectReader,
        PutObjectReader = ScannerPutObjReader,
    >
{
}

impl<T> ScannerObjectIO for T where
    T: ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ScannerObjectOptions,
            ObjectInfo = ScannerObjectInfo,
            GetObjectReader = ScannerGetObjectReader,
            PutObjectReader = ScannerPutObjReader,
        >
{
}
