// Copyright 2026 RustFS Team
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

use std::io::{Read, Write};
use zstd::zstd_safe::{CCtx, DCtx, InBuffer, OutBuffer, ResetDirective};

#[test]
fn abandoned_encoder_cannot_prefix_the_next_request() {
    let mut context = CCtx::create();
    {
        let mut abandoned = zstd::stream::Encoder::with_context(Vec::new(), &mut context);
        abandoned
            .write_all(b"tenant-A private input")
            .expect("buffer the abandoned request");
    }
    let mut next = zstd::stream::Encoder::with_context(Vec::new(), &mut context);
    next.write_all(b"tenant-B public input").expect("encode the next request");
    let encoded = next.finish().expect("finish the next request");
    assert_eq!(
        zstd::decode_all(encoded.as_slice()).expect("decode the next frame"),
        b"tenant-B public input"
    );
}

#[test]
fn abandoned_decoder_cannot_return_the_previous_request() {
    let previous = zstd::encode_all(&b"tenant-A unread private output"[..], 1).expect("encode previous request");
    let current = zstd::encode_all(&b"tenant-B output"[..], 1).expect("encode current request");
    let mut context = DCtx::create();
    {
        let mut abandoned = zstd::stream::Decoder::with_context(previous.as_slice(), &mut context);
        abandoned.read_exact(&mut [0; 3]).expect("partially read the previous frame");
    }
    let mut actual = Vec::new();
    zstd::stream::Decoder::with_context(current.as_slice(), &mut context)
        .read_to_end(&mut actual)
        .expect("read the new request independently");
    assert_eq!(actual, b"tenant-B output");
}

#[test]
fn corrupt_legacy_frame_keeps_buffer_positions_and_length_unchanged() {
    let corrupt = [0x27, 0xb5, 0x2f, 0xfd, 0x08, 0x00];
    let mut context = DCtx::create();
    let mut destination = Vec::with_capacity(4096);
    {
        let mut input = InBuffer::around(&corrupt);
        let mut output = OutBuffer::around(&mut destination);
        assert!(context.decompress_stream(&mut output, &mut input).is_err());
        assert_eq!(input.pos(), 0);
        assert_eq!(output.pos(), 0);
    }
    assert!(destination.is_empty());
    let valid = zstd::encode_all(&b"ordinary frame after explicit reset"[..], 1).expect("encode valid control");
    context.reset(ResetDirective::SessionOnly).expect("reset failed decoder");
    {
        let mut input = InBuffer::around(valid.as_slice());
        let mut output = OutBuffer::around(&mut destination);
        assert_eq!(context.decompress_stream(&mut output, &mut input), Ok(0));
    }
    assert_eq!(destination, b"ordinary frame after explicit reset");
}

#[test]
fn corrupt_legacy_frame_cannot_emit_bytes_when_writer_is_dropped() {
    let mut visible = Vec::new();
    {
        let mut decoder = zstd::stream::write::Decoder::new(&mut visible)
            .expect("create writer decoder")
            .auto_flush();
        assert!(decoder.write_all(&[0x27, 0xb5, 0x2f, 0xfd, 0x08, 0x00]).is_err());
    }
    assert!(visible.is_empty(), "failed legacy decoding must not publish heap bytes");
}

#[test]
fn seekable_offsets_past_eof_keep_vec_length_within_capacity() {
    use zstd::zstd_safe::seekable::{Seekable, SeekableCStream};

    let source = b"seekable frame fixture with enough data for several independent frames";
    let mut encoded = [0; 1024];
    let written = {
        let mut encoder = SeekableCStream::create();
        encoder.init(3, true, 24).expect("initialize seekable encoder");
        let mut input = InBuffer::around(source);
        let mut output = OutBuffer::around(&mut encoded[..]);
        while input.pos() < source.len() {
            let previous = input.pos();
            encoder
                .compress_stream(&mut output, &mut input)
                .expect("encode seekable input");
            assert!(input.pos() > previous, "encoder must make progress");
        }
        let mut finished = false;
        for _ in 0..16 {
            if encoder.end_stream(&mut output).expect("finish seek table") == 0 {
                finished = true;
                break;
            }
        }
        assert!(finished, "bounded fixture must finish within the output capacity");
        output.pos()
    };
    let mut decoder = Seekable::create();
    decoder.init_buff(&encoded[..written]).expect("open seekable archive");
    let mut decoded = Vec::with_capacity(source.len());
    assert_eq!(decoder.decompress(&mut decoded, 0), Ok(source.len()));
    assert_eq!(decoded, source);
    let end = u64::try_from(source.len()).expect("fixture length fits u64");
    for offset in [end, end + 1, end + 4096, u64::MAX - 9, u64::MAX] {
        let mut output = Vec::with_capacity(16);
        assert_eq!(decoder.decompress(&mut output, offset), Ok(0), "offset {offset}");
        assert!(output.is_empty(), "offset {offset} must not extend the vector");
        assert!(output.len() <= output.capacity());
    }
}

#[test]
fn stored_registry_013_frame_remains_readable() {
    // Generated by registry zstd 0.13.3 with encode_all at level 1.
    let frame: &[u8] = &[
        0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x48, 0x99, 0x00, 0x00, 0x73, 0x79, 0x6e, 0x74, 0x68, 0x65, 0x74, 0x69, 0x63, 0x2d, 0x72,
        0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2d, 0x42,
    ];
    assert_eq!(zstd::decode_all(frame).expect("read pre-upgrade stored frame"), b"synthetic-request-B");
}
