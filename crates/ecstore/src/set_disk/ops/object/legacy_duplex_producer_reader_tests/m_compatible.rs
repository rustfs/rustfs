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

//! Tests of what only an `m-compatible` build compiles.

use super::*;

struct ObjectKeyResolver {
    object_key: [u8; 32],
}

#[async_trait::async_trait]
impl ObjectEncryptionResolver for ObjectKeyResolver {
    async fn resolve_read_material(
        &self,
        _request: crate::object_api::ReadEncryptionRequest<'_>,
    ) -> std::result::Result<Option<ReadEncryptionMaterial>, EncryptionResolutionError> {
        Ok(Some(ReadEncryptionMaterial {
            key_bytes: self.object_key,
            mode: ReadEncryptionMode::Object,
        }))
    }
}

const RIO_V2_FIXTURE_OBJECT_KEY: [u8; 32] = [0x67; 32];

fn rio_v2_fixture_plaintext(len: usize, seed: usize) -> Vec<u8> {
    (0..len)
        .map(|index| u8::try_from((index + seed) % 251).expect("a remainder below 251 fits in u8"))
        .collect()
}

/// Reads a rio-v2 encrypted object the way the legacy GET path does: a
/// producer task feeds the stored bytes into a pipe sized by
/// `adaptive_duplex_buffer_size`, so the decrypt reader sees `Pending`
/// wherever a DARE package outruns the pipe. `parts` lists each part's
/// `(plaintext, stored)` length as the object metadata records it, and
/// `stored` is what the drives actually hold, which a test may cut short.
async fn read_rio_v2_object_through_get_pipe(
    stored: Vec<u8>,
    parts: &[(usize, usize)],
    range: Option<HTTPRangeSpec>,
) -> std::io::Result<Vec<u8>> {
    let plaintext_size: usize = parts.iter().map(|(plaintext_len, _)| plaintext_len).sum();
    let multipart = parts.len() > 1;
    let object_info = ObjectInfo {
        bucket: "bucket".to_string(),
        name: "rio-v2-encrypted-object".to_string(),
        size: i64::try_from(parts.iter().map(|(_, stored_len)| stored_len).sum::<usize>())
            .expect("stored fixture length should fit in i64"),
        etag: multipart.then(|| format!("{}-{}", "0".repeat(32), parts.len())),
        parts: Arc::new(if multipart {
            parts
                .iter()
                .enumerate()
                .map(|(index, (plaintext_len, stored_len))| rustfs_filemeta::ObjectPartInfo {
                    number: index + 1,
                    size: *stored_len,
                    actual_size: i64::try_from(*plaintext_len).expect("part plaintext length should fit in i64"),
                    ..Default::default()
                })
                .collect()
        } else {
            Vec::new()
        }),
        user_defined: Arc::new(HashMap::from([
            ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
            (
                "x-amz-server-side-encryption-customer-original-size".to_string(),
                plaintext_size.to_string(),
            ),
        ])),
        ..Default::default()
    };
    let ctx = InstanceContext::new();
    assert!(
        ctx.set_object_encryption_resolver(Arc::new(ObjectKeyResolver {
            object_key: RIO_V2_FIXTURE_OBJECT_KEY
        }))
        .is_ok(),
        "fresh context should accept resolver"
    );

    let (mut writer, reader) = tokio::io::duplex(adaptive_duplex_buffer_size(object_info.size));
    let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
    let (mut reader, storage_offset, storage_length) = get_legacy_object_reader_with_context(
        &ctx,
        reader,
        terminal_rx,
        range,
        &object_info,
        &ObjectOptions::default(),
        &HeaderMap::new(),
    )
    .await
    .expect("encrypted read plan should build");
    let storage_length = usize::try_from(storage_length).expect("storage length should not be negative");
    let producer = tokio::spawn(async move {
        let window_end = stored.len().min(storage_offset + storage_length);
        let window_start = storage_offset.min(window_end);
        let written = writer.write_all(&stored[window_start..window_end]).await;
        drop(writer);
        // The reader may already have failed and dropped the receiver.
        let _ = terminal_tx.send(Ok(()));
        written
    });

    let mut out = Vec::new();
    let read = reader.read_to_end(&mut out).await;
    drop(reader);
    // A failed read drops the pipe early, so the producer's own result only
    // matters once the read itself succeeded.
    let written = producer.await.expect("producer task should not panic");
    read?;
    written?;
    Ok(out)
}

#[tokio::test]
async fn rio_v2_encrypted_get_survives_pipe_refills_mid_package() {
    const MIB: usize = 1024 * 1024;

    // Stored sizes up to 128 KiB get a 64 KiB pipe, smaller than one DARE
    // package; 3 MiB objects get a 1 MiB pipe that packages straddle.
    let mut failures = Vec::new();
    for plaintext_len in [65_535, 65_536, 65_537, 71_680, 3 * MIB, 3 * MIB + 1] {
        let plaintext = rio_v2_fixture_plaintext(plaintext_len, 0);
        let mut stored = Vec::new();
        crate::io_support::rio::EncryptReader::new_with_object_key(
            std::io::Cursor::new(plaintext.clone()),
            RIO_V2_FIXTURE_OBJECT_KEY,
        )
        .read_to_end(&mut stored)
        .await
        .expect("encrypt singlepart fixture");
        let stored_len = stored.len();

        match read_rio_v2_object_through_get_pipe(stored, &[(plaintext_len, stored_len)], None).await {
            Ok(out) if out == plaintext => {}
            Ok(_) => failures.push(format!("{plaintext_len}-byte object: plaintext mismatch")),
            Err(err) => failures.push(format!("{plaintext_len}-byte object: {err}")),
        }
    }

    assert!(failures.is_empty(), "rio-v2 encrypted GET failed for: {failures:#?}");
}

#[tokio::test]
async fn rio_v2_encrypted_multipart_get_survives_pipe_refills_mid_package() {
    const MIB: usize = 1024 * 1024;
    let part_plaintexts = [rio_v2_fixture_plaintext(5 * MIB, 1), rio_v2_fixture_plaintext(MIB + 17, 2)];

    let (stored, parts) = rio_v2_multipart_fixture(&part_plaintexts).await;

    let out = read_rio_v2_object_through_get_pipe(stored, &parts, None)
        .await
        .expect("read multipart object through the GET pipe");

    assert!(out == part_plaintexts.concat(), "multipart plaintext mismatch");
}

/// Encrypts each part with its own part key and returns the concatenated
/// stored stream with every part's `(plaintext, stored)` length.
async fn rio_v2_multipart_fixture(part_plaintexts: &[Vec<u8>]) -> (Vec<u8>, Vec<(usize, usize)>) {
    let mut stored = Vec::new();
    let mut parts = Vec::new();
    for (part_number, part_plaintext) in (1u32..).zip(part_plaintexts) {
        let stored_before = stored.len();
        crate::io_support::rio::EncryptReader::new_multipart_with_object_key(
            std::io::Cursor::new(part_plaintext.clone()),
            RIO_V2_FIXTURE_OBJECT_KEY,
            part_number,
        )
        .read_to_end(&mut stored)
        .await
        .expect("encrypt multipart fixture part");
        parts.push((part_plaintext.len(), stored.len() - stored_before));
    }
    (stored, parts)
}

#[tokio::test]
async fn rio_v2_encrypted_multipart_range_get_rejects_a_missing_trailing_part() {
    let part_plaintexts = [rio_v2_fixture_plaintext(70_000, 1), rio_v2_fixture_plaintext(140_000, 2)];
    let (mut stored, parts) = rio_v2_multipart_fixture(&part_plaintexts).await;
    // The drives hold part 1 only, although the metadata still lists part 2.
    stored.truncate(parts[0].1);

    // The range starts in part 1 and ends inside the missing part 2.
    let range = HTTPRangeSpec {
        is_suffix_length: false,
        start: 69_900,
        end: 70_099,
    };
    let outcome = read_rio_v2_object_through_get_pipe(stored, &parts, Some(range)).await;

    match outcome {
        Err(err) => assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof, "{err}"),
        Ok(out) => panic!("a 200-byte range over a missing part read {} bytes as a clean EOF", out.len()),
    }
}

#[tokio::test]
async fn rio_v2_encrypted_multipart_get_accepts_an_empty_trailing_part() {
    let part_plaintexts = [rio_v2_fixture_plaintext(70_000, 1), Vec::new()];
    let (stored, parts) = rio_v2_multipart_fixture(&part_plaintexts).await;
    assert_eq!(parts[1], (0, 0), "an empty part stores no DARE package");

    let out = read_rio_v2_object_through_get_pipe(stored, &parts, None)
        .await
        .expect("an empty trailing part is not a truncated stream");

    assert!(out == part_plaintexts[0], "plaintext mismatch for an object with an empty trailing part");
}
