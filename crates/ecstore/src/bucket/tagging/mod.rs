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

use s3s::dto::Tag;
use std::collections::HashMap;
use url::form_urlencoded;

pub fn decode_tags(tags: &str) -> Vec<Tag> {
    let values = form_urlencoded::parse(tags.as_bytes());

    let mut list = Vec::new();

    for (k, v) in values {
        if k.is_empty() || v.is_empty() {
            continue;
        }

        list.push(Tag {
            key: Some(k.to_string()),
            value: Some(v.to_string()),
        });
    }

    list
}

pub fn decode_tags_to_map(tags: &str) -> HashMap<String, String> {
    let mut list = HashMap::new();

    for (k, v) in form_urlencoded::parse(tags.as_bytes()) {
        if k.is_empty() || v.is_empty() {
            continue;
        }

        list.insert(k.to_string(), v.to_string());
    }

    list
}

pub fn encode_tags(tags: Vec<Tag>) -> String {
    let mut encoded = form_urlencoded::Serializer::new(String::new());

    for tag in tags.iter() {
        if let (Some(k), Some(v)) = (tag.key.as_ref(), tag.value.as_ref()) {
            //encoded.append_pair(k.as_ref().unwrap().as_str(), v.as_ref().unwrap().as_str());
            encoded.append_pair(k.as_str(), v.as_str());
        }
    }

    encoded.finish()
}
