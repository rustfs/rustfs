use super::*;

fn restore_request_active(opts: &ObjectOptions) -> bool {
    let restore = &opts.transition.restore_request;
    restore.type_.is_some() || restore.days.is_some() || restore.output_location.is_some() || restore.select_parameters.is_some()
}

fn decode_compression_index(index: Option<&bytes::Bytes>) -> Option<rustfs_rio::Index> {
    let bytes = index?;
    let mut decoded = rustfs_rio::Index::new();
    if decoded.load(bytes.as_ref()).is_ok() {
        Some(decoded)
    } else {
        None
    }
}

fn get_compressed_offsets(oi: &ObjectInfo, offset: i64) -> (i64, i64, usize, i64, u64) {
    let mut skip_length = 0_i64;
    let mut cumulative_actual_size = 0_i64;
    let mut first_part_idx = 0_usize;
    let mut compressed_offset = 0_i64;

    for (i, part) in oi.parts.iter().enumerate() {
        cumulative_actual_size += part.actual_size;
        if cumulative_actual_size <= offset {
            compressed_offset += part.size as i64;
        } else {
            first_part_idx = i;
            skip_length = cumulative_actual_size - part.actual_size;
            break;
        }
    }

    let mut part_skip = offset - skip_length;
    let decrypt_skip = 0_i64;
    let seq_num = 0_u64;

    if part_skip > 0
        && let Some(part) = oi.parts.get(first_part_idx)
        && let Some(index) = decode_compression_index(part.index.as_ref())
        && let Ok((comp_off, uncomp_off)) = index.find(part_skip)
        && comp_off > 0
    {
        compressed_offset += comp_off;
        part_skip -= uncomp_off;
    }

    (compressed_offset, part_skip, first_part_idx, decrypt_skip, seq_num)
}

pub struct PutObjReader {
    pub stream: HashReader,
}

impl Debug for PutObjReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutObjReader").finish()
    }
}

impl PutObjReader {
    pub fn new(stream: HashReader) -> Self {
        PutObjReader { stream }
    }

    pub fn as_hash_reader(&self) -> &HashReader {
        &self.stream
    }

    pub fn from_vec(data: Vec<u8>) -> Self {
        use sha2::{Digest, Sha256};
        let content_length = data.len() as i64;
        let sha256hex = if content_length > 0 {
            Some(hex_simd::encode_to_string(Sha256::digest(&data), hex_simd::AsciiCase::Lower))
        } else {
            None
        };
        PutObjReader {
            stream: HashReader::from_stream(Cursor::new(data), content_length, content_length, None, sha256hex, false).unwrap(),
        }
    }

    pub fn size(&self) -> i64 {
        self.stream.size()
    }

    pub fn actual_size(&self) -> i64 {
        self.stream.actual_size()
    }
}

pub struct GetObjectReader {
    pub stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    pub object_info: ObjectInfo,
}

impl GetObjectReader {
    #[tracing::instrument(level = "debug", skip(reader, rs, opts, _h))]
    pub fn new(
        reader: Box<dyn AsyncRead + Unpin + Send + Sync>,
        rs: Option<HTTPRangeSpec>,
        oi: &ObjectInfo,
        opts: &ObjectOptions,
        _h: &HeaderMap<HeaderValue>,
    ) -> Result<(Self, usize, i64)> {
        let mut rs = rs;

        if let Some(part_number) = opts.part_number
            && rs.is_none()
        {
            rs = HTTPRangeSpec::from_object_info(oi, part_number);
        }

        let mut is_encrypted = oi.is_encrypted();
        let (algo, mut is_compressed) = oi.is_compressed_ok()?;

        if restore_request_active(opts) {
            is_encrypted = false;
            is_compressed = false;
        }

        if is_compressed {
            let actual_size = oi.get_actual_size()?;
            let (off, length, dec_off, dec_length) = if let Some(rs) = rs {
                let (req_off, req_length) = rs.get_offset_length(actual_size)?;
                let (physical_off, decompressed_skip, _, _, _) = get_compressed_offsets(oi, req_off as i64);
                (physical_off as usize, oi.size - physical_off, decompressed_skip as usize, req_length)
            } else {
                (0, oi.size, 0, actual_size)
            };

            let dec_reader = DecompressReader::new(reader, algo);

            let actual_size_usize = if actual_size >= 0 {
                actual_size as usize
            } else {
                return Err(Error::other(format!("invalid decompressed size {actual_size}")));
            };

            let final_reader: Box<dyn AsyncRead + Unpin + Send + Sync> = if dec_off > 0 || dec_length != actual_size {
                // Use RangedDecompressReader for streaming range processing
                // The new implementation supports any offset size by streaming and skipping data
                match RangedDecompressReader::new(dec_reader, dec_off, dec_length, actual_size_usize) {
                    Ok(ranged_reader) => {
                        tracing::debug!(
                            "Successfully created RangedDecompressReader for offset={}, length={}",
                            dec_off,
                            dec_length
                        );
                        Box::new(ranged_reader)
                    }
                    Err(e) => {
                        // Only fail if the range parameters are fundamentally invalid (e.g., offset >= file size)
                        tracing::error!("RangedDecompressReader failed with invalid range parameters: {}", e);
                        return Err(e);
                    }
                }
            } else {
                Box::new(LimitReader::new(dec_reader, actual_size_usize))
            };

            let mut oi = oi.clone();
            oi.size = dec_length;

            return Ok((
                GetObjectReader {
                    stream: final_reader,
                    object_info: oi,
                },
                off,
                length,
            ));
        }

        if is_encrypted {
            let decrypted_size = oi.decrypted_size()?;
            if let Some(rs) = rs {
                let _ = rs.get_offset_length(decrypted_size)?;
            }
            return Err(Error::other("encrypted object reads are not implemented"));
        }

        if let Some(rs) = rs {
            let (off, length) = rs.get_offset_length(oi.size)?;

            Ok((
                GetObjectReader {
                    stream: reader,
                    object_info: oi.clone(),
                },
                off,
                length,
            ))
        } else {
            Ok((
                GetObjectReader {
                    stream: reader,
                    object_info: oi.clone(),
                },
                0,
                oi.size,
            ))
        }
    }
    pub async fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        self.stream.read_to_end(&mut data).await?;

        // while let Some(x) = self.stream.next().await {
        //     let buf = match x {
        //         Ok(res) => res,
        //         Err(e) => return Err(Error::other(e.to_string())),
        //     };
        //     data.extend_from_slice(buf.as_ref());
        // }

        Ok(data)
    }
}

impl AsyncRead for GetObjectReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

#[derive(Debug, Clone)]
pub struct HTTPRangeSpec {
    pub is_suffix_length: bool,
    pub start: i64,
    pub end: i64,
}

impl HTTPRangeSpec {
    pub fn from_object_info(oi: &ObjectInfo, part_number: usize) -> Option<Self> {
        if oi.size == 0 || oi.parts.is_empty() {
            return None;
        }

        if part_number == 0 || part_number > oi.parts.len() {
            return None;
        }

        let mut start = 0_i64;
        let mut end = -1_i64;
        for i in 0..part_number {
            let part = &oi.parts[i];
            start = end + 1;
            end = start + part.actual_size - 1;
        }

        Some(HTTPRangeSpec {
            is_suffix_length: false,
            start,
            end,
        })
    }

    pub fn get_offset_length(&self, res_size: i64) -> Result<(usize, i64)> {
        let len = self.get_length(res_size)?;

        let mut start = self.start;
        if self.is_suffix_length {
            let suffix_len = if self.start < 0 {
                self.start
                    .checked_neg()
                    .ok_or_else(|| Error::InvalidRangeSpec("range value invalid: suffix length overflow".to_string()))?
            } else {
                self.start
            };
            start = res_size - suffix_len;
            if start < 0 {
                start = 0;
            }
        }
        Ok((start as usize, len))
    }
    pub fn get_length(&self, res_size: i64) -> Result<i64> {
        if res_size < 0 {
            return Err(Error::InvalidRangeSpec("The requested range is not satisfiable".to_string()));
        }

        if self.is_suffix_length {
            let specified_len = if self.start < 0 {
                self.start
                    .checked_neg()
                    .ok_or_else(|| Error::InvalidRangeSpec("range value invalid: suffix length overflow".to_string()))?
            } else {
                self.start
            };
            let mut range_length = specified_len;

            if specified_len > res_size {
                range_length = res_size;
            }

            return Ok(range_length);
        }

        if self.start >= res_size {
            return Err(Error::InvalidRangeSpec("The requested range is not satisfiable".to_string()));
        }

        if self.end > -1 {
            let mut end = self.end;
            if res_size <= end {
                end = res_size - 1;
            }

            let range_length = end - self.start + 1;
            return Ok(range_length);
        }

        if self.end == -1 {
            let range_length = res_size - self.start;
            return Ok(range_length);
        }

        Err(Error::InvalidRangeSpec(format!(
            "range value invalid: start={}, end={}, expected start <= end and end >= -1",
            self.start, self.end
        )))
    }
}

/// A streaming decompression reader that supports range requests by skipping data in the decompressed stream.
/// This implementation acknowledges that compressed streams (like LZ4) must be decompressed sequentially
/// from the beginning, so it streams and discards data until reaching the target offset.
#[derive(Debug)]
pub struct RangedDecompressReader<R> {
    inner: R,
    target_offset: usize,
    target_length: usize,
    current_offset: usize,
    bytes_returned: usize,
}

impl<R: AsyncRead + Unpin + Send + Sync> RangedDecompressReader<R> {
    pub fn new(inner: R, offset: usize, length: i64, total_size: usize) -> Result<Self> {
        // Validate the range request
        if offset >= total_size {
            tracing::debug!("Range offset {} exceeds total size {}", offset, total_size);
            return Err(Error::InvalidRangeSpec("Range offset exceeds file size".to_string()));
        }

        // Adjust length if it extends beyond file end
        let actual_length = std::cmp::min(length as usize, total_size - offset);

        tracing::debug!(
            "Creating RangedDecompressReader: offset={}, length={}, total_size={}, actual_length={}",
            offset,
            length,
            total_size,
            actual_length
        );

        Ok(Self {
            inner,
            target_offset: offset,
            target_length: actual_length,
            current_offset: 0,
            bytes_returned: 0,
        })
    }
}

impl<R: AsyncRead + Unpin + Send + Sync> AsyncRead for RangedDecompressReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::pin::Pin;
        use std::task::Poll;
        use tokio::io::ReadBuf;

        loop {
            // If we've returned all the bytes we need, return EOF
            if self.bytes_returned >= self.target_length {
                return Poll::Ready(Ok(()));
            }

            // Read from the inner stream
            let buf_capacity = buf.remaining();
            if buf_capacity == 0 {
                return Poll::Ready(Ok(()));
            }

            // Prepare a temporary buffer for reading
            let mut temp_buf = vec![0u8; std::cmp::min(buf_capacity, 8192)];
            let mut temp_read_buf = ReadBuf::new(&mut temp_buf);

            match Pin::new(&mut self.inner).poll_read(cx, &mut temp_read_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    let n = temp_read_buf.filled().len();
                    if n == 0 {
                        // EOF from inner stream
                        if self.current_offset < self.target_offset {
                            // We haven't reached the target offset yet - this is an error
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!(
                                    "Unexpected EOF: only read {} bytes, target offset is {}",
                                    self.current_offset, self.target_offset
                                ),
                            )));
                        }
                        // Normal EOF after reaching target
                        return Poll::Ready(Ok(()));
                    }

                    // Update current position
                    let old_offset = self.current_offset;
                    self.current_offset += n;

                    // Check if we're still in the skip phase
                    if old_offset < self.target_offset {
                        // We're still skipping data
                        let skip_end = std::cmp::min(self.current_offset, self.target_offset);
                        let bytes_to_skip_in_this_read = skip_end - old_offset;

                        if self.current_offset <= self.target_offset {
                            // All data in this read should be skipped
                            tracing::trace!("Skipping {} bytes at offset {}", n, old_offset);
                            // Continue reading in the loop instead of recursive call
                            continue;
                        } else {
                            // Partial skip: some data should be returned
                            let data_start_in_buffer = bytes_to_skip_in_this_read;
                            let available_data = n - data_start_in_buffer;
                            let bytes_to_return = std::cmp::min(
                                available_data,
                                std::cmp::min(buf.remaining(), self.target_length - self.bytes_returned),
                            );

                            if bytes_to_return > 0 {
                                let data_slice =
                                    &temp_read_buf.filled()[data_start_in_buffer..data_start_in_buffer + bytes_to_return];
                                buf.put_slice(data_slice);
                                self.bytes_returned += bytes_to_return;

                                tracing::trace!(
                                    "Skipped {} bytes, returned {} bytes at offset {}",
                                    bytes_to_skip_in_this_read,
                                    bytes_to_return,
                                    old_offset
                                );
                            }
                            return Poll::Ready(Ok(()));
                        }
                    } else {
                        // We're in the data return phase
                        let bytes_to_return =
                            std::cmp::min(n, std::cmp::min(buf.remaining(), self.target_length - self.bytes_returned));

                        if bytes_to_return > 0 {
                            buf.put_slice(&temp_read_buf.filled()[..bytes_to_return]);
                            self.bytes_returned += bytes_to_return;

                            tracing::trace!("Returned {} bytes at offset {}", bytes_to_return, old_offset);
                        }
                        return Poll::Ready(Ok(()));
                    }
                }
            }
        }
    }
}

/// A wrapper that ensures the inner stream is fully consumed even if the outer reader stops early.
/// This prevents broken pipe errors in erasure coding scenarios where the writer expects
/// the full stream to be consumed.
pub struct StreamConsumer<R: AsyncRead + Unpin + Send + 'static> {
    inner: Option<R>,
    consumer_task: Option<tokio::task::JoinHandle<()>>,
}

impl<R: AsyncRead + Unpin + Send + 'static> StreamConsumer<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner: Some(inner),
            consumer_task: None,
        }
    }

    fn ensure_consumer_started(&mut self) {
        if self.consumer_task.is_none() && self.inner.is_some() {
            let mut inner = self.inner.take().unwrap();
            let task = tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                loop {
                    match inner.read(&mut buf).await {
                        Ok(0) => break,    // EOF
                        Ok(_) => continue, // Keep consuming
                        Err(_) => break,   // Error, stop consuming
                    }
                }
            });
            self.consumer_task = Some(task);
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> AsyncRead for StreamConsumer<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::pin::Pin;
        use std::task::Poll;

        if let Some(ref mut inner) = self.inner {
            Pin::new(inner).poll_read(cx, buf)
        } else {
            Poll::Ready(Ok(())) // EOF
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> Drop for StreamConsumer<R> {
    fn drop(&mut self) {
        if self.consumer_task.is_none() && self.inner.is_some() {
            let mut inner = self.inner.take().unwrap();
            let task = tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                loop {
                    match inner.read(&mut buf).await {
                        Ok(0) => break,    // EOF
                        Ok(_) => continue, // Keep consuming
                        Err(_) => break,   // Error, stop consuming
                    }
                }
            });
            self.consumer_task = Some(task);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_ranged_decompress_reader() {
        // Create test data
        let original_data = b"Hello, World! This is a test for range requests on compressed data.";

        // For this test, we'll simulate using the original data directly as "decompressed"
        let cursor = Cursor::new(original_data.to_vec());

        // Test reading a range from the middle
        let mut ranged_reader = RangedDecompressReader::new(cursor, 7, 5, original_data.len()).unwrap();

        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();

        // Should read "World" (5 bytes starting from position 7)
        assert_eq!(result, b"World");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_from_start() {
        let original_data = b"Hello, World! This is a test.";
        let cursor = Cursor::new(original_data.to_vec());

        let mut ranged_reader = RangedDecompressReader::new(cursor, 0, 5, original_data.len()).unwrap();

        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();

        // Should read "Hello" (5 bytes from the start)
        assert_eq!(result, b"Hello");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_to_end() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());

        let mut ranged_reader = RangedDecompressReader::new(cursor, 7, 6, original_data.len()).unwrap();

        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();

        // Should read "World!" (6 bytes starting from position 7)
        assert_eq!(result, b"World!");
    }

    #[tokio::test]
    async fn test_http_range_spec_with_compressed_data() {
        // Test that HTTPRangeSpec::get_offset_length works correctly
        let range_spec = HTTPRangeSpec {
            is_suffix_length: false,
            start: 5,
            end: 14, // inclusive
        };

        let total_size = 100i64;
        let (offset, length) = range_spec.get_offset_length(total_size).unwrap();

        assert_eq!(offset, 5);
        assert_eq!(length, 10); // end - start + 1 = 14 - 5 + 1 = 10
    }

    #[test]
    fn test_http_range_spec_suffix_positive_start() {
        let range_spec = HTTPRangeSpec {
            is_suffix_length: true,
            start: 5,
            end: -1,
        };

        let (offset, length) = range_spec.get_offset_length(20).unwrap();
        assert_eq!(offset, 15);
        assert_eq!(length, 5);
    }

    #[test]
    fn test_http_range_spec_suffix_negative_start() {
        let range_spec = HTTPRangeSpec {
            is_suffix_length: true,
            start: -5,
            end: -1,
        };

        let (offset, length) = range_spec.get_offset_length(20).unwrap();
        assert_eq!(offset, 15);
        assert_eq!(length, 5);
    }

    #[test]
    fn test_http_range_spec_suffix_exceeds_object() {
        let range_spec = HTTPRangeSpec {
            is_suffix_length: true,
            start: 50,
            end: -1,
        };

        let (offset, length) = range_spec.get_offset_length(20).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(length, 20);
    }

    #[test]
    fn test_http_range_spec_from_object_info_valid_and_invalid_parts() {
        let object_info = ObjectInfo {
            size: 300,
            parts: vec![
                ObjectPartInfo {
                    etag: String::new(),
                    number: 1,
                    size: 100,
                    actual_size: 100,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 2,
                    size: 100,
                    actual_size: 100,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 3,
                    size: 100,
                    actual_size: 100,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let spec = HTTPRangeSpec::from_object_info(&object_info, 2).unwrap();
        assert_eq!(spec.start, 100);
        assert_eq!(spec.end, 199);

        assert!(HTTPRangeSpec::from_object_info(&object_info, 0).is_none());
        assert!(HTTPRangeSpec::from_object_info(&object_info, 4).is_none());
    }

    #[test]
    fn test_http_range_spec_from_object_info_uses_actual_size() {
        let object_info = ObjectInfo {
            size: 90,
            parts: vec![
                ObjectPartInfo {
                    etag: String::new(),
                    number: 1,
                    size: 20,
                    actual_size: 30,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 2,
                    size: 30,
                    actual_size: 40,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 3,
                    size: 40,
                    actual_size: 50,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let spec = HTTPRangeSpec::from_object_info(&object_info, 2).unwrap();
        assert_eq!(spec.start, 30);
        assert_eq!(spec.end, 69);
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_zero_length() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());
        let mut ranged_reader = RangedDecompressReader::new(cursor, 5, 0, original_data.len()).unwrap();
        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();
        // Should read nothing
        assert_eq!(result, b"");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_skip_entire_data() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());
        // Skip to end of data with length 0 - this should read nothing
        let mut ranged_reader = RangedDecompressReader::new(cursor, original_data.len() - 1, 0, original_data.len()).unwrap();
        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, b"");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_out_of_bounds_offset() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());
        // Offset beyond EOF should return error in constructor
        let result = RangedDecompressReader::new(cursor, original_data.len() + 10, 5, original_data.len());
        assert!(result.is_err());
        // Use pattern matching to avoid requiring Debug on the error type
        if let Err(e) = result {
            assert!(e.to_string().contains("Range offset exceeds file size"));
        }
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_partial_read() {
        let original_data = b"abcdef";
        let cursor = Cursor::new(original_data.to_vec());
        let mut ranged_reader = RangedDecompressReader::new(cursor, 2, 3, original_data.len()).unwrap();
        let mut buf = [0u8; 2];
        let n = ranged_reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf, b"cd");
        let mut buf2 = [0u8; 2];
        let n2 = ranged_reader.read(&mut buf2).await.unwrap();
        assert_eq!(n2, 1);
        assert_eq!(&buf2[..1], b"e");
    }

    #[test]
    fn test_get_object_reader_rejects_encrypted_range_without_decryption_support() {
        let object_info = ObjectInfo {
            size: 10,
            user_defined: HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                ("x-amz-server-side-encryption-customer-original-size".to_string(), "20".to_string()),
            ]),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 8,
            end: -1,
        };

        let result = GetObjectReader::new(
            Box::new(Cursor::new(b"0123456789".to_vec())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &HeaderMap::new(),
        );

        match result {
            Ok(_) => panic!("encrypted range read should not fall back to plain object semantics"),
            Err(err) => assert!(err.to_string().contains("encrypted object reads are not implemented")),
        }
    }

    #[test]
    fn test_get_object_reader_restore_request_bypasses_encryption_range_rewrite() {
        let object_info = ObjectInfo {
            size: 10,
            user_defined: HashMap::from([
                ("x-rustfs-encryption-key".to_string(), "encrypted-key".to_string()),
                ("x-rustfs-encryption-original-size".to_string(), "20".to_string()),
            ]),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: true,
            start: 4,
            end: -1,
        };

        let mut opts = ObjectOptions::default();
        opts.transition.restore_request.days = Some(1);

        let (_, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(b"0123456789".to_vec())),
            Some(range),
            &object_info,
            &opts,
            &HeaderMap::new(),
        )
        .unwrap();

        assert_eq!(offset, 6);
        assert_eq!(length, 4);
    }

    #[test]
    fn test_get_object_reader_compressed_range_returns_physical_offset_from_index() {
        let mut index = rustfs_rio::Index::new();
        index.add(0, 0).unwrap();
        index.add(1_048_576, 2_097_152).unwrap();

        let object_info = ObjectInfo {
            size: 3_000_000,
            parts: vec![ObjectPartInfo {
                etag: String::new(),
                number: 1,
                size: 3_000_000,
                actual_size: 4_194_304,
                index: Some(index.into_vec()),
                ..Default::default()
            }],
            user_defined: HashMap::from([
                ("x-minio-internal-compression".to_string(), "gzip".to_string()),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ]),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 2_097_152,
            end: 2_097_161,
        };

        let (reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(Vec::<u8>::new())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &HeaderMap::new(),
        )
        .unwrap();

        assert!(offset > 0);
        assert!(offset < 2_097_152);
        assert_eq!(length, object_info.size - offset as i64);
        assert_eq!(reader.object_info.size, 10);
    }
}
