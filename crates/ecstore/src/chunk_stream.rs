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

// use crate::error::StdError;
// use bytes::Bytes;
// use futures::pin_mut;
// use futures::stream::{Stream, StreamExt};
// use std::future::Future;
// use std::pin::Pin;
// use std::task::{Context, Poll};
// use transform_stream::AsyncTryStream;

// pub type SyncBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 'a>>;

// pub struct ChunkedStream<'a> {
//     /// inner
//     inner: AsyncTryStream<Bytes, StdError, SyncBoxFuture<'a, Result<(), StdError>>>,

//     remaining_length: usize,
// }

// impl<'a> ChunkedStream<'a> {
//     pub fn new<S>(body: S, content_length: usize, chunk_size: usize, need_padding: bool) -> Self
//     where
//         S: Stream<Item = Result<Bytes, StdError>> + Send + Sync + 'a,
//     {
//         let inner = AsyncTryStream::<_, _, SyncBoxFuture<'a, Result<(), StdError>>>::new(|mut y| {
//             #[allow(clippy::shadow_same)] // necessary for `pin_mut!`
//             Box::pin(async move {
//                 pin_mut!(body);
//                 // Data left over from the previous call
//                 let mut prev_bytes = Bytes::new();
//                 let mut read_size = 0;

//                 loop {
//                     let data: Vec<Bytes> = {
//                         // Read a fixed-size chunk
//                         match Self::read_data(body.as_mut(), prev_bytes, chunk_size).await {
//                             None => break,
//                             Some(Err(e)) => return Err(e),
//                             Some(Ok((data, remaining_bytes))) => {
//                                 // debug!(
//                                 //     "content_length:{},read_size:{}, read_data data:{}, remaining_bytes: {} ",
//                                 //     content_length,
//                                 //     read_size,
//                                 //     data.len(),
//                                 //     remaining_bytes.len()
//                                 // );

//                                 prev_bytes = remaining_bytes;
//                                 data
//                             }
//                         }
//                     };

//                     for bytes in data {
//                         read_size += bytes.len();
//                         // debug!("read_size {}, content_length {}", read_size, content_length,);
//                         y.yield_ok(bytes).await;
//                     }

//                     if read_size + prev_bytes.len() >= content_length {
//                         // debug!(
//                         //     "Finished reading: read_size:{} + prev_bytes.len({}) == content_length {}",
//                         //     read_size,
//                         //     prev_bytes.len(),
//                         //     content_length,
//                         // );

//                         // Pad with zeros?
//                         if !need_padding {
//                             y.yield_ok(prev_bytes).await;
//                             break;
//                         }

//                         let mut bytes = vec![0u8; chunk_size];
//                         let (left, _) = bytes.split_at_mut(prev_bytes.len());
//                         left.copy_from_slice(&prev_bytes);

//                         y.yield_ok(Bytes::from(bytes)).await;

//                         break;
//                     }
//                 }

//                 // debug!("chunked stream exit");

//                 Ok(())
//             })
//         });
//         Self {
//             inner,
//             remaining_length: content_length,
//         }
//     }
//     /// read data and return remaining bytes
//     async fn read_data<S>(
//         mut body: Pin<&mut S>,
//         prev_bytes: Bytes,
//         data_size: usize,
//     ) -> Option<Result<(Vec<Bytes>, Bytes), StdError>>
//     where
//         S: Stream<Item = Result<Bytes, StdError>> + Send,
//     {
//         let mut bytes_buffer = Vec::new();

//         // Run only once
//         let mut push_data_bytes = |mut bytes: Bytes| {
//             // debug!("read from body {} split per {}, prev_bytes: {}", bytes.len(), data_size, prev_bytes.len());

//             if bytes.is_empty() {
//                 return None;
//             }

//             if data_size == 0 {
//                 return Some(bytes);
//             }

//             // Merge with the previous data
//             if !prev_bytes.is_empty() {
//                 let need_size = data_size.wrapping_sub(prev_bytes.len());
//                 // debug!(
//                 //     "Previous leftover {}, take {} now, total: {}",
//                 //     prev_bytes.len(),
//                 //     need_size,
//                 //     prev_bytes.len() + need_size
//                 // );
//                 if bytes.len() >= need_size {
//                     let data = bytes.split_to(need_size);
//                     let mut combined = Vec::new();
//                     combined.extend_from_slice(&prev_bytes);
//                     combined.extend_from_slice(&data);

//                     // debug!(
//                     //     "Fetched more bytes than needed: {}, merged result {}, remaining bytes {}",
//                     //     need_size,
//                     //     combined.len(),
//                     //     bytes.len(),
//                     // );

//                     bytes_buffer.push(Bytes::from(combined));
//                 } else {
//                     let mut combined = Vec::new();
//                     combined.extend_from_slice(&prev_bytes);
//                     combined.extend_from_slice(&bytes);

//                     // debug!(
//                     //     "Fetched fewer bytes than needed: {}, merged result {}, remaining bytes {}, return immediately",
//                     //     need_size,
//                     //     combined.len(),
//                     //     bytes.len(),
//                     // );

//                     return Some(Bytes::from(combined));
//                 }
//             }

//             // If the fetched data exceeds the chunk, slice the required size
//             if data_size <= bytes.len() {
//                 let n = bytes.len() / data_size;

//                 for _ in 0..n {
//                     let data = bytes.split_to(data_size);

//                     // println!("bytes_buffer.push: {}, remaining: {}", data.len(), bytes.len());
//                     bytes_buffer.push(data);
//                 }

//                 Some(bytes)
//             } else {
//                 // Insufficient data
//                 Some(bytes)
//             }
//         };

//         // Remaining data
//         let remaining_bytes = 'outer: {
//             // // Exit if the previous data was sufficient
//             // if let Some(remaining_bytes) = push_data_bytes(prev_bytes) {
//             //     println!("Consuming leftovers");
//             //     break 'outer remaining_bytes;
//             // }

//             loop {
//                 match body.next().await? {
//                     Err(e) => return Some(Err(e)),
//                     Ok(bytes) => {
//                         if let Some(remaining_bytes) = push_data_bytes(bytes) {
//                             break 'outer remaining_bytes;
//                         }
//                     }
//                 }
//             }
//         };

//         Some(Ok((bytes_buffer, remaining_bytes)))
//     }

//     fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes, StdError>>> {
//         let ans = Pin::new(&mut self.inner).poll_next(cx);
//         if let Poll::Ready(Some(Ok(ref bytes))) = ans {
//             self.remaining_length = self.remaining_length.saturating_sub(bytes.len());
//         }
//         ans
//     }

//     // pub fn exact_remaining_length(&self) -> usize {
//     //     self.remaining_length
//     // }
// }

// impl Stream for ChunkedStream<'_> {
//     type Item = Result<Bytes, StdError>;

//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         self.poll(cx)
//     }

//     fn size_hint(&self) -> (usize, Option<usize>) {
//         (0, None)
//     }
// }

// #[cfg(test)]
// mod test {

//     use super::*;

//     #[tokio::test]
//     async fn test_chunked_stream() {
//         let chunk_size = 4;

//         let data1 = vec![1u8; 7777]; // 65536
//         let data2 = vec![1u8; 7777]; // 65536

//         let content_length = data1.len() + data2.len();

//         let chunk1 = Bytes::from(data1);
//         let chunk2 = Bytes::from(data2);

//         let chunk_results: Vec<Result<Bytes, _>> = vec![Ok(chunk1), Ok(chunk2)];

//         let stream = futures::stream::iter(chunk_results);

//         let mut chunked_stream = ChunkedStream::new(stream, content_length, chunk_size, true);

//         loop {
//             let ans1 = chunked_stream.next().await;
//             if ans1.is_none() {
//                 break;
//             }

//             let bytes = ans1.unwrap().unwrap();
//             assert!(bytes.len() == chunk_size)
//         }

//         // assert_eq!(ans1.unwrap(), chunk1_data.as_slice());
//     }
// }
