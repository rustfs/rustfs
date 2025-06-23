#![allow(unused_imports)]

use bytes::Bytes;
use s3s::StdError;
use std::any::Any;
use std::io::Read;
use std::{collections::VecDeque, io::Cursor};

use std::pin::Pin;
use std::task::Poll;

use crate::{
    error::ReaderError,
    hasher::{HashType, Uuid},
};

// use futures::stream::Stream;
use super::hasher::{Hasher, MD5, Sha256};
use futures::Stream;
use std::io::{Error, Result};

pin_project_lite::pin_project! {
    #[derive(Default)]
    pub struct EtagReader<S> {
        #[pin]
        inner: S,
        md5: HashType,
        checksum:Option<String>,
        bytes_read:usize,
    }
}

impl<S> EtagReader<S> {
    pub fn new(inner: S, etag: Option<String>, force_md5: Option<String>) -> Self {
        let md5 = {
            if let Some(m) = force_md5 {
                HashType::Uuid(Uuid::new(m))
            } else {
                HashType::Md5(MD5::new())
            }
        };
        Self {
            inner,
            md5,
            checksum: etag,
            bytes_read: 0,
        }
    }

    pub fn etag(&mut self) -> String {
        self.md5.sum()
    }
}

impl<S> Stream for EtagReader<S>
where
    S: Stream<Item = std::result::Result<Bytes, StdError>>,
{
    type Item = std::result::Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let poll = this.inner.poll_next(cx);

        if let Poll::Ready(ref res) = poll {
            match res {
                Some(Ok(bytes)) => {
                    *this.bytes_read += bytes.len();
                    this.md5.write(bytes);
                }
                Some(Err(err)) => {
                    return Poll::Ready(Some(Err(Box::new(ReaderError::StreamInput(err.to_string())))));
                }
                None => {
                    if let Some(etag) = this.checksum {
                        let got = this.md5.sum();
                        if got.as_str() != etag.as_str() {
                            return Poll::Ready(Some(Err(Box::new(ReaderError::VerifyError(etag.to_owned(), got)))));
                        }
                    }
                }
            }
        }

        poll
    }
}

pin_project_lite::pin_project! {
    #[derive(Default)]
    pub struct HashReader<S> {
        #[pin]
        inner: S,
        sha256: Option<Sha256>,
        md5: Option<MD5>,
        md5_hex:Option<String>,
        sha256_hex:Option<String>,
        size:usize,
        actual_size: usize,
        bytes_read:usize,
    }
}

impl<S> HashReader<S> {
    pub fn new(inner: S, size: usize, md5_hex: Option<String>, sha256_hex: Option<String>, actual_size: usize) -> Self {
        let md5 = { if md5_hex.is_some() { Some(MD5::new()) } else { None } };
        let sha256 = { if sha256_hex.is_some() { Some(Sha256::new()) } else { None } };
        Self {
            inner,
            size,
            actual_size,
            md5_hex,
            sha256_hex,
            bytes_read: 0,
            md5,
            sha256,
        }
    }
}

impl<S> Stream for HashReader<S>
where
    S: Stream<Item = std::result::Result<Bytes, StdError>>,
{
    type Item = std::result::Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let poll = this.inner.poll_next(cx);

        if let Poll::Ready(ref res) = poll {
            match res {
                Some(Ok(bytes)) => {
                    *this.bytes_read += bytes.len();
                    if let Some(sha) = this.sha256 {
                        sha.write(bytes);
                    }

                    if let Some(md5) = this.md5 {
                        md5.write(bytes);
                    }
                }
                Some(Err(err)) => {
                    return Poll::Ready(Some(Err(Box::new(ReaderError::StreamInput(err.to_string())))));
                }
                None => {
                    if let Some(hash) = this.sha256 {
                        if let Some(hex) = this.sha256_hex {
                            let got = hash.sum();
                            let src = hex.as_str();
                            if src != got.as_str() {
                                println!("sha256 err src:{},got:{}", src, got);
                                return Poll::Ready(Some(Err(Box::new(ReaderError::SHA256Mismatch(src.to_string(), got)))));
                            }
                        }
                    }

                    if let Some(hash) = this.md5 {
                        if let Some(hex) = this.md5_hex {
                            let got = hash.sum();
                            let src = hex.as_str();
                            if src != got.as_str() {
                                // TODO: ERR
                                println!("md5 err src:{},got:{}", src, got);
                                return Poll::Ready(Some(Err(Box::new(ReaderError::ChecksumMismatch(src.to_string(), got)))));
                            }
                        }
                    }
                }
            }
        }

        // println!("poll {:?}", poll);

        poll
    }
}

#[async_trait::async_trait]
pub trait Reader {
    async fn read_at(&mut self, offset: usize, buf: &mut [u8]) -> Result<usize>;
    async fn seek(&mut self, offset: usize) -> Result<()>;
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize>;
    async fn read_all(&mut self) -> Result<Vec<u8>> {
        let data = Vec::new();

        Ok(data)
    }
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug)]
pub struct BufferReader {
    pub inner: Cursor<Vec<u8>>,
    pos: usize,
}

impl BufferReader {
    pub fn new(inner: Vec<u8>) -> Self {
        Self {
            inner: Cursor::new(inner),
            pos: 0,
        }
    }
}

#[async_trait::async_trait]
impl Reader for BufferReader {
    #[tracing::instrument(level = "debug", skip(self, buf))]
    async fn read_at(&mut self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        self.seek(offset).await?;
        self.read_exact(buf).await
    }
    #[tracing::instrument(level = "debug", skip(self))]
    async fn seek(&mut self, offset: usize) -> Result<()> {
        if self.pos != offset {
            self.inner.set_position(offset as u64);
        }

        Ok(())
    }
    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize> {
        let _bytes_read = self.inner.read_exact(buf)?;
        self.pos += buf.len();
        //Ok(bytes_read)
        Ok(0)
    }

    async fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        self.inner.read_to_end(&mut data)?;

        Ok(data)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pin_project_lite::pin_project! {
    pub struct ChunkedStream<S> {
        #[pin]
        inner: S,
        chuck_size: usize,
        streams: VecDeque<Bytes>,
        remaining:Vec<u8>,
    }
}

impl<S> ChunkedStream<S> {
    pub fn new(inner: S, chuck_size: usize) -> Self {
        Self {
            inner,
            chuck_size,
            streams: VecDeque::new(),
            remaining: Vec::new(),
        }
    }
}

impl<S> Stream for ChunkedStream<S>
where
    S: Stream<Item = std::result::Result<Bytes, StdError>> + Send + Sync,
    // E: std::error::Error + Send + Sync,
{
    type Item = std::result::Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let (items, op_items) = self.inner.size_hint();
        let this = self.project();

        if let Some(b) = this.streams.pop_front() {
            return Poll::Ready(Some(Ok(b)));
        }

        let poll = this.inner.poll_next(cx);

        match poll {
            Poll::Ready(res_op) => match res_op {
                Some(res) => match res {
                    Ok(bytes) => {
                        let chuck_size = *this.chuck_size;
                        let mut bytes = bytes;

                        // println!("get len {}", bytes.len());
                        // 如果有剩余
                        if !this.remaining.is_empty() {
                            let need_size = chuck_size - this.remaining.len();
                            // 传入的数据大小需要补齐的大小，使用传入数据补齐
                            if bytes.len() >= need_size {
                                let add_bytes = bytes.split_to(need_size);
                                this.remaining.extend_from_slice(&add_bytes);
                                this.streams.push_back(Bytes::from(this.remaining.clone()));
                                this.remaining.clear();
                            } else {
                                // 不够，直接追加
                                let need_size = bytes.len();
                                let add_bytes = bytes.split_to(need_size);
                                this.remaining.extend_from_slice(&add_bytes);
                            }
                        }

                        loop {
                            if bytes.len() < chuck_size {
                                break;
                            }
                            let chuck = bytes.split_to(chuck_size);
                            this.streams.push_back(chuck);
                        }

                        if !bytes.is_empty() {
                            this.remaining.extend_from_slice(&bytes);
                        }

                        if let Some(b) = this.streams.pop_front() {
                            return Poll::Ready(Some(Ok(b)));
                        }

                        if items > 0 || op_items.is_some() {
                            return Poll::Pending;
                        }

                        if !this.remaining.is_empty() {
                            let b = this.remaining.clone();
                            this.remaining.clear();
                            return Poll::Ready(Some(Ok(Bytes::from(b))));
                        }
                        Poll::Ready(None)
                    }
                    Err(err) => Poll::Ready(Some(Err(err))),
                },
                None => {
                    // println!("get empty");
                    if let Some(b) = this.streams.pop_front() {
                        return Poll::Ready(Some(Ok(b)));
                    }
                    if !this.remaining.is_empty() {
                        let b = this.remaining.clone();
                        this.remaining.clear();
                        return Poll::Ready(Some(Ok(Bytes::from(b))));
                    }
                    Poll::Ready(None)
                }
            },
            Poll::Pending => {
                // println!("get Pending");
                Poll::Pending
            }
        }

        // if let Poll::Ready(Some(res)) = poll {
        //     warn!("poll res ...");
        //     match res {
        //         Ok(bytes) => {
        //             let chuck_size = *this.chuck_size;
        //             let mut bytes = bytes;
        //             if this.remaining.len() > 0 {
        //                 let need_size = chuck_size - this.remaining.len();
        //                 let add_bytes = bytes.split_to(need_size);
        //                 this.remaining.extend_from_slice(&add_bytes);
        //                 warn!("poll push_back remaining ...1");
        //                 this.streams.push_back(Bytes::from(this.remaining.clone()));
        //                 this.remaining.clear();
        //             }

        //             loop {
        //                 if bytes.len() < chuck_size {
        //                     break;
        //                 }
        //                 let chuck = bytes.split_to(chuck_size);
        //                 warn!("poll push_back ...1");
        //                 this.streams.push_back(chuck);
        //             }

        //             warn!("poll remaining extend_from_slice...1");
        //             this.remaining.extend_from_slice(&bytes);
        //         }
        //         Err(err) => return Poll::Ready(Some(Err(err))),
        //     }
        // }

        // if let Some(b) = this.streams.pop_front() {
        //     warn!("poll pop_front ...");
        //     return Poll::Ready(Some(Ok(b)));
        // }

        // if this.remaining.len() > 0 {
        //     let b = this.remaining.clone();
        //     this.remaining.clear();

        //     warn!("poll remaining ...1");
        //     return Poll::Ready(Some(Ok(Bytes::from(b))));
        // }
        // Poll::Pending
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut items = self.streams.len();
        if !self.remaining.is_empty() {
            items += 1;
        }
        (items, Some(items))
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_etag_reader() {
        let data1 = vec![1u8; 60]; // 65536
        let data2 = vec![0u8; 32]; // 65536
        let chunk1 = Bytes::from(data1);
        let chunk2 = Bytes::from(data2);

        let chunk_results: Vec<std::result::Result<Bytes, StdError>> = vec![Ok(chunk1), Ok(chunk2)];

        let mut stream = futures::stream::iter(chunk_results);

        let mut hash_reader = EtagReader::new(&mut stream, None, None);

        // let chunk_size = 8;

        // let mut chunked_stream = ChunkStream::new(&mut hash_reader, chunk_size);

        loop {
            match hash_reader.next().await {
                Some(res) => match res {
                    Ok(bytes) => {
                        println!("bytes: {}, {:?}", bytes.len(), bytes);
                    }
                    Err(err) => {
                        println!("err:{:?}", err);
                        break;
                    }
                },
                None => {
                    println!("next none");
                    break;
                }
            }
        }

        println!("etag:{}", hash_reader.etag());

        // 9a7dfa2fcd7b69c89a30cfd3a9be11ab58cb6172628bd7e967fad1e187456d45
        // println!("md5: {:?}", hash_reader.hex());
    }

    #[tokio::test]
    async fn test_hash_reader() {
        let data1 = vec![1u8; 60]; // 65536
        let data2 = vec![0u8; 32]; // 65536
        let size = data1.len() + data2.len();
        let chunk1 = Bytes::from(data1);
        let chunk2 = Bytes::from(data2);

        let chunk_results: Vec<std::result::Result<Bytes, StdError>> = vec![Ok(chunk1), Ok(chunk2)];

        let mut stream = futures::stream::iter(chunk_results);

        let mut hash_reader = HashReader::new(
            &mut stream,
            size,
            Some("d94c485610a7a00a574df55e45d3cc0c".to_string()),
            Some("9a7dfa2fcd7b69c89a30cfd3a9be11ab58cb6172628bd7e967fad1e187456d45".to_string()),
            0,
        );

        // let chunk_size = 8;

        // let mut chunked_stream = ChunkStream::new(&mut hash_reader, chunk_size);

        loop {
            match hash_reader.next().await {
                Some(res) => match res {
                    Ok(bytes) => {
                        println!("bytes: {}, {:?}", bytes.len(), bytes);
                    }
                    Err(err) => {
                        println!("err:{:?}", err);
                        break;
                    }
                },
                None => {
                    println!("next none");
                    break;
                }
            }
        }

        // BUG: borrow of moved value: `md5_stream`

        // 9a7dfa2fcd7b69c89a30cfd3a9be11ab58cb6172628bd7e967fad1e187456d45
        // println!("md5: {:?}", hash_reader.hex());
    }

    #[tokio::test]
    async fn test_chunked_stream() {
        let data1 = vec![1u8; 60]; // 65536
        let data2 = vec![0u8; 33]; // 65536
        let data3 = vec![4u8; 5]; // 65536
        let chunk1 = Bytes::from(data1);
        let chunk2 = Bytes::from(data2);
        let chunk3 = Bytes::from(data3);

        let chunk_results: Vec<std::result::Result<Bytes, StdError>> = vec![Ok(chunk1), Ok(chunk2), Ok(chunk3)];

        let mut stream = futures::stream::iter(chunk_results);
        // let mut hash_reader = HashReader::new(
        //     &mut stream,
        //     size,
        //     Some("d94c485610a7a00a574df55e45d3cc0c".to_string()),
        //     Some("9a7dfa2fcd7b69c89a30cfd3a9be11ab58cb6172628bd7e967fad1e187456d45".to_string()),
        //     0,
        // );

        let chunk_size = 8;

        let mut etag_reader = EtagReader::new(&mut stream, None, None);

        let mut chunked_stream = ChunkedStream::new(&mut etag_reader, chunk_size);

        loop {
            match chunked_stream.next().await {
                Some(res) => match res {
                    Ok(bytes) => {
                        println!("bytes: {}, {:?}", bytes.len(), bytes);
                    }
                    Err(err) => {
                        println!("err:{:?}", err);
                        break;
                    }
                },
                None => {
                    println!("next none");
                    break;
                }
            }
        }

        println!("etag:{}", etag_reader.etag());
    }
}
