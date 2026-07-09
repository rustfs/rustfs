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

use std::collections::HashMap;

pub(crate) fn decode_tags_to_map(tags: &str) -> HashMap<String, String> {
    crate::bucket::tagging::decode_tags_to_map(tags)
}

#[cfg(test)]
mod tests {
    use super::decode_tags_to_map;

    #[test]
    fn decode_tags_to_map_preserves_bucket_tagging_parser_behavior() {
        let tags = decode_tags_to_map("env=prod&encoded=a%2Fb&=ignored");

        assert_eq!(tags.get("env").map(String::as_str), Some("prod"));
        assert_eq!(tags.get("encoded").map(String::as_str), Some("a/b"));
        assert!(!tags.contains_key(""));
    }
}
