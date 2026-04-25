use super::*;
use crate::data_movement::decode_part_index;

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
    pub read_plan: ObjectReadPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaintextRange {
    pub start: usize,
    pub length: i64,
    pub total_size: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadTransform {
    Decrypt,
    Decompress { algorithm: CompressionAlgorithm },
    Slice { offset: usize, length: i64, total_size: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectReadPlan {
    pub physical_offset: usize,
    pub physical_length: i64,
    pub plaintext_size: i64,
    pub response_length: i64,
    pub plaintext_range: Option<PlaintextRange>,
    pub transforms: Vec<ReadTransform>,
}

impl ObjectReadPlan {
    pub fn build(oi: &ObjectInfo, rs: Option<HTTPRangeSpec>, opts: &ObjectOptions) -> Result<Self> {
        let mut rs = rs;
        if let Some(part_number) = opts.part_number
            && rs.is_none()
        {
            rs = plaintext_range_for_part(oi, part_number);
        }

        let plaintext_size = oi.get_actual_size()?;
        let plaintext_range = if let Some(rs) = &rs {
            let (start, length) = rs.get_offset_length(plaintext_size)?;
            Some(PlaintextRange {
                start,
                length,
                total_size: plaintext_size,
            })
        } else {
            None
        };

        let (algorithm, is_compressed) = oi.is_compressed_ok()?;
        let is_encrypted = oi.is_encrypted();

        let (physical_offset, physical_length) = build_physical_range(oi, plaintext_range.as_ref(), is_compressed, is_encrypted);

        let mut transforms = Vec::new();
        if is_encrypted {
            transforms.push(ReadTransform::Decrypt);
        }
        if is_compressed {
            transforms.push(ReadTransform::Decompress { algorithm });
        }
        if let Some(range) = &plaintext_range
            && (is_encrypted || is_compressed)
        {
            transforms.push(ReadTransform::Slice {
                offset: range.start,
                length: range.length,
                total_size: range.total_size,
            });
        }

        Ok(Self {
            physical_offset,
            physical_length,
            plaintext_size,
            response_length: plaintext_range.as_ref().map(|range| range.length).unwrap_or(plaintext_size),
            plaintext_range,
            transforms,
        })
    }

    pub fn content_range(&self) -> Option<String> {
        self.plaintext_range
            .as_ref()
            .map(|range| format!("bytes {}-{}/{}", range.start, range.start as i64 + range.length - 1, range.total_size))
    }
}

fn plaintext_range_for_part(oi: &ObjectInfo, part_number: usize) -> Option<HTTPRangeSpec> {
    if oi.parts.is_empty() || part_number == 0 {
        return None;
    }

    let mut start = 0_i64;
    for (index, part) in oi.parts.iter().enumerate() {
        let part_plaintext_size = if part.actual_size > 0 {
            part.actual_size
        } else {
            part.size as i64
        };
        if part.number == part_number || index + 1 == part_number {
            return Some(HTTPRangeSpec {
                is_suffix_length: false,
                start: start.max(0),
                end: start + part_plaintext_size - 1,
            });
        }
        start += part_plaintext_size;
    }

    None
}

fn build_physical_range(
    oi: &ObjectInfo,
    plaintext_range: Option<&PlaintextRange>,
    is_compressed: bool,
    is_encrypted: bool,
) -> (usize, i64) {
    if is_encrypted {
        return (0, oi.size);
    }

    if is_compressed && let Some(range) = plaintext_range {
        if let Some(index) = oi.parts.first().and_then(|part| decode_part_index(part.index.as_ref()))
            && let Ok((compressed_offset, _)) = index.find(range.start as i64)
        {
            let compressed_offset = compressed_offset.max(0) as usize;
            return (compressed_offset, oi.size - compressed_offset as i64);
        }

        return (0, oi.size);
    }

    if let Some(range) = plaintext_range {
        return (range.start, range.length);
    }

    (0, oi.size)
}

impl GetObjectReader {
    #[tracing::instrument(level = "debug", skip(reader, rs, opts, _h))]
    pub fn new(
        reader: Box<dyn AsyncRead + Unpin + Send + Sync>,
        rs: Option<HTTPRangeSpec>,
        oi: &ObjectInfo,
        opts: &ObjectOptions,
        _h: &HeaderMap<HeaderValue>,
    ) -> Result<Self> {
        let read_plan = ObjectReadPlan::build(oi, rs, opts)?;

        Ok(GetObjectReader {
            stream: reader,
            object_info: oi.clone(),
            read_plan,
        })
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
            end = start + (part.size as i64) - 1;
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

#[derive(Debug)]
pub struct RangedReader<R> {
    inner: R,
    target_offset: usize,
    target_length: usize,
    total_size: usize,
    current_offset: usize,
    bytes_returned: usize,
}

impl<R: AsyncRead + Unpin + Send + Sync> RangedReader<R> {
    pub fn new(inner: R, offset: usize, length: i64, total_size: usize) -> Result<Self> {
        if offset > total_size {
            tracing::debug!("Range offset {} exceeds total size {}", offset, total_size);
            return Err(Error::InvalidRangeSpec("Range offset exceeds file size".to_string()));
        }

        let actual_length = std::cmp::min(length.max(0) as usize, total_size.saturating_sub(offset));

        Ok(Self {
            inner,
            target_offset: offset,
            target_length: actual_length,
            total_size,
            current_offset: 0,
            bytes_returned: 0,
        })
    }
}

impl<R: AsyncRead + Unpin + Send + Sync> AsyncRead for RangedReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::pin::Pin;
        use std::task::Poll;
        use tokio::io::ReadBuf;

        loop {
            if self.bytes_returned >= self.target_length {
                return Poll::Ready(Ok(()));
            }

            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            let mut temp_buf = vec![0u8; std::cmp::min(buf.remaining(), 8192)];
            let mut temp_read_buf = ReadBuf::new(&mut temp_buf);

            match Pin::new(&mut self.inner).poll_read(cx, &mut temp_read_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    let n = temp_read_buf.filled().len();
                    if n == 0 {
                        if self.current_offset < self.target_offset {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!(
                                    "Unexpected EOF: only read {} bytes, target offset is {}, total size is {}",
                                    self.current_offset, self.target_offset, self.total_size
                                ),
                            )));
                        }
                        return Poll::Ready(Ok(()));
                    }

                    let old_offset = self.current_offset;
                    self.current_offset += n;

                    if old_offset < self.target_offset {
                        let skip_end = std::cmp::min(self.current_offset, self.target_offset);
                        let skipped = skip_end - old_offset;

                        if self.current_offset <= self.target_offset {
                            continue;
                        }

                        let data_start = skipped;
                        let available = n - data_start;
                        let bytes_to_return =
                            std::cmp::min(available, std::cmp::min(buf.remaining(), self.target_length - self.bytes_returned));

                        if bytes_to_return > 0 {
                            let data_slice = &temp_read_buf.filled()[data_start..data_start + bytes_to_return];
                            buf.put_slice(data_slice);
                            self.bytes_returned += bytes_to_return;
                        }
                        return Poll::Ready(Ok(()));
                    }

                    let bytes_to_return =
                        std::cmp::min(n, std::cmp::min(buf.remaining(), self.target_length - self.bytes_returned));
                    if bytes_to_return > 0 {
                        buf.put_slice(&temp_read_buf.filled()[..bytes_to_return]);
                        self.bytes_returned += bytes_to_return;
                    }
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

impl<R> rustfs_rio::EtagResolvable for RangedReader<R>
where
    R: rustfs_rio::EtagResolvable,
{
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.inner.try_resolve_etag()
    }
}

impl<R> rustfs_rio::HashReaderDetector for RangedReader<R>
where
    R: rustfs_rio::HashReaderDetector,
{
    fn is_hash_reader(&self) -> bool {
        self.inner.is_hash_reader()
    }
}

impl<R> rustfs_rio::TryGetIndex for RangedReader<R>
where
    R: rustfs_rio::TryGetIndex,
{
    fn try_get_index(&self) -> Option<&rustfs_rio::Index> {
        self.inner.try_get_index()
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

    #[tokio::test]
    async fn test_ranged_reader_returns_requested_slice() {
        let original_data = b"abcdefghijklmnopqrstuvwxyz";
        let cursor = Cursor::new(original_data.to_vec());
        let mut ranged_reader = RangedReader::new(cursor, 5, 4, original_data.len()).unwrap();
        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, b"fghi");
    }

    #[test]
    fn test_get_object_reader_range_uses_plaintext_size_for_encrypted_metadata() {
        let object_info = ObjectInfo {
            size: 10,
            user_defined: HashMap::from([("x-amz-server-side-encryption-customer-original-size".to_string(), "20".to_string())]),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 8,
            end: -1,
        };

        let reader = GetObjectReader::new(
            Box::new(Cursor::new(b"0123456789".to_vec())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &HeaderMap::new(),
        )
        .unwrap();

        assert_eq!(reader.read_plan.physical_offset, 0);
        assert_eq!(reader.read_plan.physical_length, 10);
    }

    #[test]
    fn test_get_object_reader_suffix_range_uses_plaintext_size_for_encrypted_metadata() {
        let object_info = ObjectInfo {
            size: 10,
            user_defined: HashMap::from([("x-rustfs-encryption-original-size".to_string(), "20".to_string())]),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: true,
            start: 4,
            end: -1,
        };

        let reader = GetObjectReader::new(
            Box::new(Cursor::new(b"0123456789".to_vec())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &HeaderMap::new(),
        )
        .unwrap();

        assert_eq!(reader.read_plan.physical_offset, 0);
        assert_eq!(reader.read_plan.physical_length, 10);
    }

    #[test]
    fn test_build_read_plan_uses_plaintext_part_sizes_for_transformed_part_number() {
        let mut user_defined = HashMap::new();
        rustfs_utils::http::insert_str(&mut user_defined, rustfs_utils::http::SUFFIX_COMPRESSION, "gzip".to_string());
        rustfs_utils::http::insert_str(&mut user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "180".to_string());
        user_defined.insert("x-rustfs-encryption-original-size".to_string(), "180".to_string());

        let object_info = ObjectInfo {
            size: 220,
            user_defined,
            parts: vec![
                ObjectPartInfo {
                    number: 1,
                    size: 120,
                    actual_size: 100,
                    ..Default::default()
                },
                ObjectPartInfo {
                    number: 2,
                    size: 100,
                    actual_size: 80,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let plan = ObjectReadPlan::build(
            &object_info,
            None,
            &ObjectOptions {
                part_number: Some(2),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(plan.plaintext_size, 180);
        assert_eq!(plan.response_length, 80);
        assert_eq!(
            plan.plaintext_range,
            Some(PlaintextRange {
                start: 100,
                length: 80,
                total_size: 180,
            })
        );
    }

    #[test]
    fn test_build_read_plan_orders_decrypt_before_decompress_for_compressed_encrypted_range() {
        let mut user_defined = HashMap::new();
        rustfs_utils::http::insert_str(&mut user_defined, rustfs_utils::http::SUFFIX_COMPRESSION, "gzip".to_string());
        rustfs_utils::http::insert_str(&mut user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "64".to_string());
        user_defined.insert("x-rustfs-encryption-original-size".to_string(), "64".to_string());

        let object_info = ObjectInfo {
            size: 100,
            user_defined,
            ..Default::default()
        };

        let plan = ObjectReadPlan::build(
            &object_info,
            Some(HTTPRangeSpec {
                is_suffix_length: false,
                start: 8,
                end: 15,
            }),
            &ObjectOptions::default(),
        )
        .unwrap();

        assert_eq!(plan.physical_offset, 0);
        assert_eq!(plan.physical_length, 100);
        assert_eq!(
            plan.transforms,
            vec![
                ReadTransform::Decrypt,
                ReadTransform::Decompress {
                    algorithm: CompressionAlgorithm::Gzip,
                },
                ReadTransform::Slice {
                    offset: 8,
                    length: 8,
                    total_size: 64,
                },
            ]
        );
    }

    #[test]
    fn test_build_read_plan_returns_plaintext_content_range_for_suffix_request() {
        let object_info = ObjectInfo {
            size: 10,
            user_defined: HashMap::from([("x-rustfs-encryption-original-size".to_string(), "20".to_string())]),
            ..Default::default()
        };

        let plan = ObjectReadPlan::build(
            &object_info,
            Some(HTTPRangeSpec {
                is_suffix_length: true,
                start: 4,
                end: -1,
            }),
            &ObjectOptions::default(),
        )
        .unwrap();

        assert_eq!(
            plan.plaintext_range,
            Some(PlaintextRange {
                start: 16,
                length: 4,
                total_size: 20,
            })
        );
        assert_eq!(plan.content_range(), Some("bytes 16-19/20".to_string()));
    }
}
