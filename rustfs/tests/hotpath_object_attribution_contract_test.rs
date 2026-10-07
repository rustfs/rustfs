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

const GET_OBJECT: &str = include_str!("../src/app/object/get.rs");
const PUT_OBJECT: &str = include_str!("../src/app/object/put.rs");
const SELECT_OBJECT: &str = include_str!("../src/app/object/mod.rs");

fn assert_async_method_is_measured(source: &str, method: &str) {
    let method_marker = format!("pub async fn {method}");
    let method_start = source
        .find(&method_marker)
        .unwrap_or_else(|| panic!("missing method {method}"));
    let prefix = &source[..method_start];
    let attribute_start = prefix
        .rfind("#[hotpath::measure(")
        .unwrap_or_else(|| panic!("missing hotpath measurement for {method}"));
    let attribute = &prefix[attribute_start..];
    let attribute = attribute
        .split_once(")]")
        .map(|(attribute, _)| attribute)
        .unwrap_or_else(|| panic!("malformed hotpath measurement for {method}"));

    assert!(method_start - attribute_start < 256, "hotpath measurement is not attached to {method}",);
    assert!(
        attribute.contains("impl_type = \"DefaultObjectUsecase\""),
        "{method} must retain CPU symbol attribution",
    );
    assert!(attribute.contains("future = true"), "{method} must expose async future lifecycle metrics",);
}

#[test]
fn object_request_profiling_is_attached_to_the_matching_operation() {
    assert_async_method_is_measured(GET_OBJECT, "execute_get_object");
    assert_async_method_is_measured(PUT_OBJECT, "execute_put_object");
    assert_async_method_is_measured(SELECT_OBJECT, "execute_select_object_content");
}
