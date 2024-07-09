use bytes::Bytes;
use futures::pin_mut;
use futures::stream::{Stream, StreamExt};
use s3s::StdError;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::debug;
use transform_stream::AsyncTryStream;

pub type SyncBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 'a>>;

pub struct ChunkedStream {
    /// inner
    inner: AsyncTryStream<Bytes, StdError, SyncBoxFuture<'static, Result<(), StdError>>>,

    remaining_length: usize,
}

impl ChunkedStream {
    pub fn new<S>(body: S, content_length: usize, chunk_size: usize, need_padding: bool) -> Self
    where
        S: Stream<Item = Result<Bytes, StdError>> + Send + Sync + 'static,
    {
        let inner = AsyncTryStream::<_, _, SyncBoxFuture<'static, Result<(), StdError>>>::new(|mut y| {
            #[allow(clippy::shadow_same)] // necessary for `pin_mut!`
            Box::pin(async move {
                pin_mut!(body);
                // 上一次没用完的数据
                let mut prev_bytes = Bytes::new();
                let mut readed_size = 0;

                loop {
                    let data: Vec<Bytes> = {
                        // 读固定大小的数据
                        match Self::read_data(body.as_mut(), prev_bytes, chunk_size).await {
                            None => break,
                            Some(Err(e)) => return Err(e),
                            Some(Ok((data, remaining_bytes))) => {
                                debug!(
                                    "content_length:{},readed_size:{}, read_data data:{}, remaining_bytes: {} ",
                                    content_length,
                                    readed_size,
                                    data.len(),
                                    remaining_bytes.len()
                                );

                                prev_bytes = remaining_bytes;
                                data
                            }
                        }
                    };

                    for bytes in data {
                        readed_size += bytes.len();
                        // println!(
                        //     "readed_size {}, content_length {}",
                        //     readed_size, content_length,
                        // );
                        y.yield_ok(bytes).await;
                    }

                    if readed_size + prev_bytes.len() >= content_length {
                        // println!(
                        //     "读完了 readed_size:{} + prev_bytes.len({}) == content_length {}",
                        //     readed_size,
                        //     prev_bytes.len(),
                        //     content_length,
                        // );

                        // 填充0？
                        if !need_padding {
                            y.yield_ok(prev_bytes).await;
                            break;
                        }

                        let mut bytes = vec![0u8; chunk_size];
                        let (left, _) = bytes.split_at_mut(prev_bytes.len());
                        left.copy_from_slice(&prev_bytes);

                        y.yield_ok(Bytes::from(bytes)).await;

                        break;
                    }
                }

                debug!("chunked stream exit");

                Ok(())
            })
        });
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
    ) -> Option<Result<(Vec<Bytes>, Bytes), StdError>>
    where
        S: Stream<Item = Result<Bytes, StdError>> + Send + 'static,
    {
        let mut bytes_buffer = Vec::new();

        // 只执行一次
        let mut push_data_bytes = |mut bytes: Bytes| {
            debug!("read from body {} split per {}, prev_bytes: {}", bytes.len(), data_size, prev_bytes.len());

            if bytes.is_empty() {
                return None;
            }

            if data_size == 0 {
                return Some(bytes);
            }

            // 合并上一次数据
            if !prev_bytes.is_empty() {
                let need_size = data_size.wrapping_sub(prev_bytes.len());
                // println!(
                //     " 上一次有剩余{},从这一次中取{},共：{}",
                //     prev_bytes.len(),
                //     need_size,
                //     prev_bytes.len() + need_size
                // );
                if bytes.len() >= need_size {
                    let data = bytes.split_to(need_size);
                    let mut combined = Vec::new();
                    combined.extend_from_slice(&prev_bytes);
                    combined.extend_from_slice(&data);

                    debug!(
                        "取到的长度大于所需，取出需要的长度：{},与上一次合并得到：{}，bytes剩余：{}",
                        need_size,
                        combined.len(),
                        bytes.len(),
                    );

                    bytes_buffer.push(Bytes::from(combined));
                } else {
                    let mut combined = Vec::new();
                    combined.extend_from_slice(&prev_bytes);
                    combined.extend_from_slice(&bytes);

                    debug!(
                        "取到的长度小于所需，取出需要的长度：{},与上一次合并得到：{}，bytes剩余：{}，直接返回",
                        need_size,
                        combined.len(),
                        bytes.len(),
                    );

                    return Some(Bytes::from(combined));
                }
            }

            // 取到的数据比需要的块大，从bytes中截取需要的块大小
            if data_size <= bytes.len() {
                let n = bytes.len() / data_size;

                for _ in 0..n {
                    let data = bytes.split_to(data_size);

                    // println!("bytes_buffer.push: {}， 剩余：{}", data.len(), bytes.len());
                    bytes_buffer.push(data);
                }

                Some(bytes)
            } else {
                // 不够
                Some(bytes)
            }
        };

        // 剩余数据
        let remaining_bytes = 'outer: {
            // // 如果上一次数据足够，跳出
            // if let Some(remaining_bytes) = push_data_bytes(prev_bytes) {
            //     println!("从剩下的取");
            //     break 'outer remaining_bytes;
            // }

            loop {
                match body.next().await? {
                    Err(e) => return Some(Err(e)),
                    Ok(bytes) => {
                        if let Some(remaining_bytes) = push_data_bytes(bytes) {
                            break 'outer remaining_bytes;
                        }
                    }
                }
            }
        };

        Some(Ok((bytes_buffer, remaining_bytes)))
    }

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes, StdError>>> {
        let ans = Pin::new(&mut self.inner).poll_next(cx);
        if let Poll::Ready(Some(Ok(ref bytes))) = ans {
            self.remaining_length = self.remaining_length.saturating_sub(bytes.len());
        }
        ans
    }

    // pub fn exact_remaining_length(&self) -> usize {
    //     self.remaining_length
    // }
}

impl Stream for ChunkedStream {
    type Item = Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[tokio::test]
    async fn test_chunked_stream() {
        let chunk_size = 4;

        let data1 = vec![1u8; 7777]; // 65536
        let data2 = vec![1u8; 7777]; // 65536

        let content_length = data1.len() + data2.len();

        let chunk1 = Bytes::from(data1);
        let chunk2 = Bytes::from(data2);

        let chunk_results: Vec<Result<Bytes, _>> = vec![Ok(chunk1), Ok(chunk2)];

        let stream = futures::stream::iter(chunk_results);

        let mut chunked_stream = ChunkedStream::new(stream, content_length, chunk_size, true);

        loop {
            let ans1 = chunked_stream.next().await;
            if ans1.is_none() {
                break;
            }

            let bytes = ans1.unwrap().unwrap();
            assert!(bytes.len() == chunk_size)
        }

        // assert_eq!(ans1.unwrap(), chunk1_data.as_slice());
    }
}
