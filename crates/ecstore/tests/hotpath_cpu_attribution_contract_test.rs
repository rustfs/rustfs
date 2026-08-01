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

const STORE_MULTIPART: &str = include_str!("../src/store/multipart.rs");
const STORE_OBJECT: &str = include_str!("../src/store/object.rs");
const ERASURE: &str = include_str!("../src/erasure/coding/erasure.rs");
const DECODE: &str = include_str!("../src/erasure/coding/decode.rs");
const ENCODE: &str = include_str!("../src/erasure/coding/encode.rs");
const LOCAL_DISK: &str = include_str!("../src/disk/local.rs");
const BITROT: &str = include_str!("../src/erasure/coding/bitrot.rs");
const SET_DISK_READ: &str = include_str!("../src/set_disk/read.rs");

fn assert_measured_as(source: &str, impl_type: &str, function: &str) {
    let attribute = format!("#[hotpath::measure(impl_type = \"{impl_type}\")]\n");
    let function_start = format!("fn {function}");
    let mut remaining = source;

    while let Some(attribute_offset) = remaining.find(&attribute) {
        let measured_source = &remaining[attribute_offset + attribute.len()..];
        if measured_source.find(&function_start).is_some_and(|offset| offset < 256) {
            return;
        }
        remaining = measured_source;
    }

    panic!("missing HotPath impl_type={impl_type} for {function}");
}

#[test]
fn inherent_hotpath_measurements_have_cpu_attribution_types() {
    assert_measured_as(STORE_MULTIPART, "ECStore", "handle_put_object_part");
    for function in ["handle_get_object_reader", "handle_put_object"] {
        assert_measured_as(STORE_OBJECT, "ECStore", function);
    }
    for function in [
        "encode_data",
        "encode_data_owned",
        "encode_data_bytes_mut",
        "decode_data",
        "decode_data_and_parity",
    ] {
        assert_measured_as(ERASURE, "Erasure", function);
    }
    assert_measured_as(DECODE, "ParallelReader", "read");
    assert_measured_as(DECODE, "Erasure", "decode");
    for function in [
        "encode",
        "encode_batched",
        "encode_inline_small",
        "encode_single_block_non_inline",
    ] {
        assert_measured_as(ENCODE, "Erasure", function);
    }
    for function in ["read_metadata_with_dmtime", "read_all_data"] {
        assert_measured_as(LOCAL_DISK, "LocalDisk", function);
    }
    assert_measured_as(BITROT, "BitrotReader", "read");
    assert!(
        BITROT.contains("#[hotpath::measure(label = \"BitrotWriter::write\", impl_type = \"BitrotWriter\")]"),
        "BitrotWriter::write must retain its stable label and CPU impl_type",
    );
    for function in [
        "read_version_optimized",
        "get_object_fileinfo",
        "get_object_info_and_quorum",
        "get_object_with_fileinfo",
        "get_object_decode_reader_with_fileinfo",
        "build_codec_streaming_part_reader",
    ] {
        assert_measured_as(SET_DISK_READ, "SetDisks", function);
    }
}

#[test]
fn module_hotpath_measurements_remain_untyped() {
    assert!(
        BITROT.contains("#[hotpath::measure]\npub async fn bitrot_verify"),
        "bitrot_verify is a module function and must not claim an impl type",
    );
    assert!(
        SET_DISK_READ.contains("#[hotpath::measure]\nasync fn setup_multipart_part_readers"),
        "setup_multipart_part_readers is a module function and must not claim an impl type",
    );
}
