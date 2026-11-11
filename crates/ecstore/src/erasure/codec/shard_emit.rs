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

//! Zero-copy emit: a consumer that can send bytes straight out of a shard
//! buffer (an RDMA write from registered arena memory) registers a
//! [`ShardEmitter`] for the decode task; the emit stage then hands it whole
//! data shards instead of copying them into the byte stream.

use super::shard_buf::ShardBuf;
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::ops::Range;
use std::task::{Context, Poll};

/// Takes ownership of data shards at the point where the decoder would write
/// them into the byte stream. The bytes `range` of the shard are the next
/// bytes of the stream, in order; the emitter keeps the buffer until it no
/// longer needs the memory (for an RDMA write, until the DMA completes).
pub trait ShardEmitter: Send {
    /// Returns `Ready(Ok(true))` once the shard was taken (`shard` is then
    /// `None`), `Ready(Ok(false))` when this shard cannot be taken (the
    /// caller then writes the bytes through the stream instead), and
    /// `Pending` while the emitter has no room yet.
    fn poll_emit_shard(
        &mut self,
        cx: &mut Context<'_>,
        shard: &mut Option<ShardBuf>,
        range: Range<usize>,
    ) -> Poll<io::Result<bool>>;

    /// Called at a stripe boundary: an emitter that batches shards submits
    /// what it holds. `Pending` while it waits for room.
    fn poll_flush_shards(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

tokio::task_local! {
    static SHARD_EMITTER: RefCell<Option<Box<dyn ShardEmitter>>>;
}

/// Runs `future` with `emitter` offered to the decode loop's emit stage.
pub async fn with_shard_emitter<F: Future>(emitter: Box<dyn ShardEmitter>, future: F) -> F::Output {
    SHARD_EMITTER.scope(RefCell::new(Some(emitter)), future).await
}

/// Whether the current task offers a shard emitter.
pub fn shard_emitter_offered() -> bool {
    SHARD_EMITTER.try_with(|cell| cell.borrow().is_some()).unwrap_or(false)
}

/// Offers `shard[range]` to the task's emitter. Resolves to `true` when the
/// emitter took the shard, `false` when there is no emitter or it declined.
pub async fn emit_shard(shard: &mut Option<ShardBuf>, range: Range<usize>) -> io::Result<bool> {
    if !shard_emitter_offered() {
        return Ok(false);
    }
    std::future::poll_fn(|cx| {
        SHARD_EMITTER
            .try_with(|cell| match cell.borrow_mut().as_mut() {
                Some(emitter) => emitter.poll_emit_shard(cx, shard, range.clone()),
                None => Poll::Ready(Ok(false)),
            })
            .unwrap_or(Poll::Ready(Ok(false)))
    })
    .await
}

/// Tells the task's emitter (if any) that a stripe's data shards are complete.
pub async fn flush_shards() -> io::Result<()> {
    if !shard_emitter_offered() {
        return Ok(());
    }
    std::future::poll_fn(|cx| {
        SHARD_EMITTER
            .try_with(|cell| match cell.borrow_mut().as_mut() {
                Some(emitter) => emitter.poll_flush_shards(cx),
                None => Poll::Ready(Ok(())),
            })
            .unwrap_or(Poll::Ready(Ok(())))
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    type Taken = Arc<Mutex<Vec<(usize, Range<usize>)>>>;

    struct Recording {
        taken: Taken,
        accept: bool,
    }

    struct Flushing {
        flushes: Arc<Mutex<usize>>,
    }

    impl ShardEmitter for Flushing {
        fn poll_emit_shard(
            &mut self,
            _cx: &mut Context<'_>,
            shard: &mut Option<ShardBuf>,
            _range: Range<usize>,
        ) -> Poll<io::Result<bool>> {
            shard.take();
            Poll::Ready(Ok(true))
        }

        fn poll_flush_shards(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            *self.flushes.lock().unwrap() += 1;
            Poll::Ready(Ok(()))
        }
    }

    impl ShardEmitter for Recording {
        fn poll_emit_shard(
            &mut self,
            _cx: &mut Context<'_>,
            shard: &mut Option<ShardBuf>,
            range: Range<usize>,
        ) -> Poll<io::Result<bool>> {
            if !self.accept {
                return Poll::Ready(Ok(false));
            }
            let taken = shard.take().expect("shard offered");
            self.taken.lock().unwrap().push((taken.len(), range));
            Poll::Ready(Ok(true))
        }
    }

    #[tokio::test]
    async fn emit_without_an_emitter_declines_and_keeps_the_shard() {
        let mut shard = Some(ShardBuf::from(vec![1u8; 8]));
        assert!(!emit_shard(&mut shard, 0..8).await.unwrap());
        assert!(shard.is_some());
        assert!(!shard_emitter_offered());
    }

    #[tokio::test]
    async fn emitter_takes_the_shard_inside_the_scope_only() {
        let taken = Arc::new(Mutex::new(Vec::new()));
        let emitter = Box::new(Recording {
            taken: Arc::clone(&taken),
            accept: true,
        });
        with_shard_emitter(emitter, async {
            assert!(shard_emitter_offered());
            let mut shard = Some(ShardBuf::from(vec![7u8; 16]));
            assert!(emit_shard(&mut shard, 4..16).await.unwrap());
            assert!(shard.is_none());
        })
        .await;
        assert_eq!(taken.lock().unwrap().as_slice(), &[(16, 4..16)]);
        assert!(!shard_emitter_offered());
    }

    #[tokio::test]
    async fn flush_reaches_the_emitter_only_inside_the_scope() {
        assert!(flush_shards().await.is_ok());
        let flushes = Arc::new(Mutex::new(0));
        let emitter = Box::new(Flushing {
            flushes: Arc::clone(&flushes),
        });
        with_shard_emitter(emitter, async {
            let mut shard = Some(ShardBuf::from(vec![1u8; 4]));
            assert!(emit_shard(&mut shard, 0..4).await.unwrap());
            flush_shards().await.unwrap();
            flush_shards().await.unwrap();
        })
        .await;
        assert_eq!(*flushes.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn declining_emitter_leaves_the_shard_to_the_stream() {
        let emitter = Box::new(Recording {
            taken: Arc::new(Mutex::new(Vec::new())),
            accept: false,
        });
        with_shard_emitter(emitter, async {
            let mut shard = Some(ShardBuf::from(vec![7u8; 16]));
            assert!(!emit_shard(&mut shard, 0..16).await.unwrap());
            assert!(shard.is_some());
        })
        .await;
    }
}
