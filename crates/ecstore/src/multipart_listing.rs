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

use crate::storage_api_contracts::multipart::MultipartInfo;

enum MultipartListingEntry {
    Upload(MultipartInfo),
    CommonPrefix(String),
}

impl MultipartListingEntry {
    fn key(&self) -> &str {
        match self {
            Self::Upload(upload) => &upload.object,
            Self::CommonPrefix(prefix) => prefix,
        }
    }

    fn upload_id(&self) -> &str {
        match self {
            Self::Upload(upload) => &upload.upload_id,
            Self::CommonPrefix(_) => "",
        }
    }
}

pub(crate) struct MultipartListingPage {
    pub(crate) uploads: Vec<MultipartInfo>,
    pub(crate) common_prefixes: Vec<String>,
    pub(crate) is_truncated: bool,
    pub(crate) next_key_marker: Option<String>,
    pub(crate) next_upload_id_marker: Option<String>,
}

pub(crate) fn paginate_multipart_listing(
    uploads: Vec<MultipartInfo>,
    common_prefixes: Vec<String>,
    key_marker: Option<&str>,
    upload_id_marker: Option<&str>,
    max_uploads: usize,
    source_truncated: bool,
) -> MultipartListingPage {
    let mut entries = uploads
        .into_iter()
        .map(MultipartListingEntry::Upload)
        .chain(common_prefixes.into_iter().map(MultipartListingEntry::CommonPrefix))
        .filter(|entry| match key_marker {
            None => true,
            Some(key_marker) => match entry {
                MultipartListingEntry::CommonPrefix(prefix) => prefix.as_str() > key_marker,
                MultipartListingEntry::Upload(upload) => {
                    upload.object.as_str() > key_marker
                        || (upload.object == key_marker
                            && upload_id_marker.is_some_and(|marker| upload.upload_id.as_str() > marker))
                }
            },
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.key()
            .cmp(right.key())
            .then_with(|| left.upload_id().cmp(right.upload_id()))
    });

    let is_truncated = source_truncated || entries.len() > max_uploads;
    entries.truncate(max_uploads);

    let (next_key_marker, next_upload_id_marker) = if is_truncated {
        entries.last().map_or((None, None), |entry| match entry {
            MultipartListingEntry::Upload(upload) => (Some(upload.object.clone()), Some(upload.upload_id.clone())),
            MultipartListingEntry::CommonPrefix(prefix) => (Some(prefix.clone()), None),
        })
    } else {
        (None, None)
    };

    let mut page = MultipartListingPage {
        uploads: Vec::new(),
        common_prefixes: Vec::new(),
        is_truncated,
        next_key_marker,
        next_upload_id_marker,
    };
    for entry in entries {
        match entry {
            MultipartListingEntry::Upload(upload) => page.uploads.push(upload),
            MultipartListingEntry::CommonPrefix(prefix) => page.common_prefixes.push(prefix),
        }
    }
    page
}
