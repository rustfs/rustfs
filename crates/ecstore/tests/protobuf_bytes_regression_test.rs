// Copyright (c) RustFS contributors
// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;
use rustfs_protos::proto_gen::node_service::{
    ReadMultipleRequest, ReadMultipleResponse, ReadVersionResponse, ReadXlResponse, UpdateMetadataRequest, WriteMetadataRequest,
};

fn expect_bytes(_: &Bytes) {}

#[test]
fn protobuf_bytes_fields_use_bytes_consistently() {
    let update = UpdateMetadataRequest::default();
    expect_bytes(&update.file_info_bin);
    expect_bytes(&update.opts_bin);

    let write = WriteMetadataRequest::default();
    expect_bytes(&write.file_info_bin);

    let version = ReadVersionResponse::default();
    expect_bytes(&version.file_info_bin);

    let read_xl = ReadXlResponse::default();
    expect_bytes(&read_xl.raw_file_info_bin);

    let read_multiple = ReadMultipleRequest::default();
    expect_bytes(&read_multiple.read_multiple_req_bin);

    let read_multiple_response = ReadMultipleResponse::default();
    let first = read_multiple_response
        .read_multiple_resps_bin
        .first()
        .cloned()
        .unwrap_or_default();
    expect_bytes(&first);
}
