use bytes::Bytes;
use futures::pin_mut;
use futures::stream::{Stream, StreamExt};
use std::future::Future;
use std::ops::Not;
use std::pin::Pin;
use std::task::{Context, Poll};
use transform_stream::AsyncTryStream;

use crate::error::StdError;

pub type SyncBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 'a>>;

pub struct ChunkedStream {
    /// inner
    inner: AsyncTryStream<
        Bytes,
        ChunkedStreamError,
        SyncBoxFuture<'static, Result<(), ChunkedStreamError>>,
    >,

    remaining_length: usize,
}

impl ChunkedStream {
    pub fn new<S>(body: S, chunk_size: usize, content_length: usize) -> Self
    where
        S: Stream<Item = Result<Bytes, StdError>> + Send + Sync + 'static,
    {
        let inner =
            AsyncTryStream::<_, _, SyncBoxFuture<'static, Result<(), ChunkedStreamError>>>::new(
                |mut y| {
                    #[allow(clippy::shadow_same)] // necessary for `pin_mut!`
                    Box::pin(async move {
                        pin_mut!(body);
                        // 上一次没用完的数据
                        let mut prev_bytes = Bytes::new();
                        let mut readed_size = 0;

                        'outer: {
                            loop {
                                let data: Vec<Bytes> = {
                                    // 读固定大小的数据
                                    match Self::read_data(body.as_mut(), prev_bytes, chunk_size)
                                        .await
                                    {
                                        None => break 'outer,
                                        Some(Err(e)) => return Err(e),
                                        Some(Ok((data, remaining_bytes))) => {
                                            prev_bytes = remaining_bytes;
                                            data
                                        }
                                    }
                                };

                                for bytes in data {
                                    readed_size += bytes.len();

                                    //  没读完
                                    if readed_size <= content_length {
                                        if bytes.len() < chunk_size {
                                            prev_bytes = bytes;
                                        } else {
                                            y.yield_ok(bytes).await;
                                        }
                                    } else {
                                        // 读完了
                                        break 'outer;
                                    }
                                }
                            }
                        };

                        Ok(())
                    })
                },
            );
        Self {
            inner,
            remaining_length: content_length,
        }
    }
    /// read data and return remaining bytes
    async fn read_data<S>(
        mut body: Pin<&mut S>,
        prev_bytes: Bytes,
        data_size: usize,
    ) -> Option<Result<(Vec<Bytes>, Bytes), ChunkedStreamError>>
    where
        S: Stream<Item = Result<Bytes, StdError>> + Send + 'static,
    {
        let mut data_size = data_size;

        let mut bytes_buffer = Vec::new();

        let mut push_data_bytes = |mut bytes: Bytes| {
            println!("bytes_buffer.len: {}", bytes_buffer.len());

            if data_size == 0 {
                return Some(bytes);
            }
            // 取到的数据比需要的块大，从bytes中截取需要的块大小
            if data_size <= bytes.len() {
                let data = bytes.split_to(data_size);

                println!("bytes_buffer.push: {}", data.len());
                bytes_buffer.push(data);
                data_size = 0;
                Some(bytes)
            } else {
                // 不够
                data_size = data_size.wrapping_sub(bytes.len());

                if bytes.is_empty() {
                    return None;
                }

                println!("bytes_buffer.push 2: {}, need:{}", bytes.len(), data_size);

                bytes_buffer.push(bytes);
                None
            }
        };

        // 剩余数据
        let remaining_bytes = 'outer: {
            // 如果上一次数据足够，跳出
            if let Some(remaining_bytes) = push_data_bytes(prev_bytes) {
                println!("从剩下的取");
                break 'outer remaining_bytes;
            }

            loop {
                match body.next().await? {
                    Err(e) => return Some(Err(ChunkedStreamError::Underlying(e))),
                    Ok(bytes) => {
                        println!("从body 取: {}", bytes.len());
                        if let Some(remaining_bytes) = push_data_bytes(bytes) {
                            break 'outer remaining_bytes;
                        }
                    }
                }
            }
        };

        // println!(
        //     "bytes_buffer:{},remaining_bytes:{}",
        //     bytes_buffer.len(),
        //     remaining_bytes.len()
        // );

        Some(Ok((bytes_buffer, remaining_bytes)))
    }

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, ChunkedStreamError>>> {
        let ans = Pin::new(&mut self.inner).poll_next(cx);
        if let Poll::Ready(Some(Ok(ref bytes))) = ans {
            self.remaining_length = self.remaining_length.saturating_sub(bytes.len());
            println!(
                "这次读取长度：{}， 还需要：{}",
                bytes.len(),
                self.remaining_length
            );
        }
        ans
    }

    // pub fn exact_remaining_length(&self) -> usize {
    //     self.remaining_length
    // }
}

impl Stream for ChunkedStream {
    type Item = Result<Bytes, ChunkedStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChunkedStreamError {
    /// Underlying error
    #[error("ChunkedStreamError: Underlying: {}",.0)]
    Underlying(StdError),
    /// Format error
    #[error("ChunkedStreamError: FormatError")]
    FormatError,
    /// Incomplete stream
    #[error("ChunkedStreamError: Incomplete")]
    Incomplete,
}

#[cfg(test)]
mod test {

    use super::*;

    #[tokio::test]
    async fn test_chunked_stream() {
        let chunk_size = 1024;

        let data1 = vec![b'a'; 7777]; // 65536
        let data2 = vec![b'a'; 7777]; // 65536

        let content_length = data1.len() + data2.len();

        let chunk1 = Bytes::from(data1);
        let chunk2 = Bytes::from(data2);

        let chunk_results: Vec<Result<Bytes, _>> = vec![Ok(chunk1), Ok(chunk2)];

        let stream = futures::stream::iter(chunk_results);

        let mut chunked_stream = ChunkedStream::new(stream, chunk_size, content_length);

        loop {
            let ans1 = chunked_stream.next().await;
            if ans1.is_none() {
                break;
            }

            assert!(ans1.unwrap().unwrap().len() == chunk_size)
        }

        // assert_eq!(ans1.unwrap(), chunk1_data.as_slice());
    }
}
