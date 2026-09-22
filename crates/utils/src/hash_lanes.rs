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

//! Multi-stream MD5 port (Port C).
//!
//! [`hash_stream::Md5Stream`](crate::hash_stream::Md5Stream) is the one-stream-at-a-time
//! interface. This module is its many-streams-at-a-time sibling: a component that holds many
//! in-flight MD5 streams (the ETag lane server) advances them together through
//! [`Md5LaneEngine`] and never names a concrete kernel.
//!
//! The trait is the intersection of what any engine can offer, same rule as `Md5Stream`:
//! an opaque per-stream state, "feed these slices to these states", and a consuming
//! `finalize`. It asks for no `Clone`/`Copy`, no contiguous state array, no alignment and no
//! equal lengths, so [`InlineLaneEngine`] (a plain loop over `Md5Stream`) is a complete
//! implementation and the always-available fallback.
//!
//! | engine | cargo feature | what it does |
//! |---|---|---|
//! | [`InlineLaneEngine`] | `hash` | each stream through `Md5Stream`, no batching |
//! | [`SimdLaneEngine`]   | `hash-md5-simd` | `md5-simd` incremental `update_many`: 4/8/16 streams per SIMD register |

use crate::hash_stream::Md5Stream;

/// Advances many independent MD5 streams at once.
///
/// Callers own the states and may keep them anywhere (a slab, a map, a per-connection struct);
/// the engine only borrows them for the duration of one [`update_many`](Self::update_many).
pub trait Md5LaneEngine: Send + Sync + 'static {
    /// One in-flight stream. Opaque to the caller.
    type State: Send + 'static;

    /// Diagnostic name (startup logs, metrics labels); never part of any format.
    fn name(&self) -> &'static str;

    /// How many streams make a batch that runs at peak throughput. A scheduler gathers up to
    /// this many before calling [`update_many`](Self::update_many); any other count is still
    /// correct and never slower than feeding the streams one by one.
    fn preferred_batch(&self) -> usize;

    /// A new, empty stream.
    fn start(&self) -> Self::State;

    /// Append each slice to its stream. Slices may differ in length and may be empty.
    fn update_many<'a, I>(&self, streams: I)
    where
        I: IntoIterator<Item = (&'a mut Self::State, &'a [u8])>;

    /// Consume a stream and return its digest.
    fn finalize(&self, state: Self::State) -> [u8; 16];
}

/// No batching: every stream goes through [`Md5Stream`]. Behaviourally identical to hashing in
/// place, which makes it both the fallback and the reference for differential tests.
#[derive(Clone, Copy, Debug, Default)]
pub struct InlineLaneEngine;

impl Md5LaneEngine for InlineLaneEngine {
    type State = Md5Stream;

    fn name(&self) -> &'static str {
        Md5Stream::ACTIVE_BACKEND
    }

    fn preferred_batch(&self) -> usize {
        1
    }

    fn start(&self) -> Md5Stream {
        Md5Stream::new()
    }

    fn update_many<'a, I>(&self, streams: I)
    where
        I: IntoIterator<Item = (&'a mut Md5Stream, &'a [u8])>,
    {
        for (state, data) in streams {
            state.update(data);
        }
    }

    fn finalize(&self, state: Md5Stream) -> [u8; 16] {
        state.finalize()
    }
}

#[cfg(feature = "hash-md5-simd")]
pub use simd::SimdLaneEngine;

#[cfg(feature = "hash-md5-simd")]
mod simd {
    use super::Md5LaneEngine;
    use md5_simd::{Md5Engine, Md5State};

    /// `md5-simd` behind the port. The backing crate's types never leave this module.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct SimdLaneEngine(Md5Engine);

    /// Opaque stream state of [`SimdLaneEngine`].
    pub struct SimdLaneState(Md5State);

    /// `Md5Engine::update_many` wants contiguous states; the port hands out scattered ones.
    /// States are small and `Copy`, so each window is copied in, advanced, and copied back.
    const WINDOW: usize = 64;

    impl SimdLaneEngine {
        /// The best engine for the running CPU.
        pub fn new() -> Self {
            Self(Md5Engine::new())
        }

        fn flush(&self, slots: &mut [Option<&mut SimdLaneState>], inputs: &[&[u8]]) {
            let mut states = [Md5State::new(); WINDOW];
            for (state, slot) in states.iter_mut().zip(slots.iter()) {
                if let Some(slot) = slot {
                    *state = slot.0;
                }
            }
            self.0.update_many(&mut states[..slots.len()], inputs);
            for (state, slot) in states.iter().zip(slots.iter_mut()) {
                if let Some(slot) = slot.take() {
                    slot.0 = *state;
                }
            }
        }
    }

    impl Md5LaneEngine for SimdLaneEngine {
        type State = SimdLaneState;

        fn name(&self) -> &'static str {
            self.0.simd_name()
        }

        fn preferred_batch(&self) -> usize {
            self.0.lanes().max(1)
        }

        fn start(&self) -> SimdLaneState {
            SimdLaneState(Md5State::new())
        }

        fn update_many<'a, I>(&self, streams: I)
        where
            I: IntoIterator<Item = (&'a mut SimdLaneState, &'a [u8])>,
        {
            let mut slots: [Option<&'a mut SimdLaneState>; WINDOW] = [const { None }; WINDOW];
            let mut inputs: [&[u8]; WINDOW] = [&[]; WINDOW];
            let mut n = 0;
            for (state, data) in streams {
                slots[n] = Some(state);
                inputs[n] = data;
                n += 1;
                if n == WINDOW {
                    self.flush(&mut slots, &inputs);
                    n = 0;
                }
            }
            self.flush(&mut slots[..n], &inputs[..n]);
        }

        fn finalize(&self, state: SimdLaneState) -> [u8; 16] {
            state.0.finalize()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn bytes(&mut self, len: usize) -> Vec<u8> {
            (0..len).map(|_| self.next() as u8).collect()
        }
    }

    /// The shape the lane server has: streams live in a map keyed by id, arrive and finish at
    /// different times, get a block-aligned chunk most ticks, nothing on some ticks, and an odd
    /// tail at the end. Every digest must equal the one-stream interface.
    fn lane_server_shape<E: Md5LaneEngine>(engine: &E) {
        let mut rng = Rng(0x5eed_1234_abcd_ef01);
        let objects: Vec<Vec<u8>> = (0..45)
            .map(|i| {
                let len = match i % 5 {
                    0 => 0,
                    1 => 777,
                    _ => 300_000 + (rng.next() % 200_000) as usize,
                };
                rng.bytes(len)
            })
            .collect();
        let mut in_flight: HashMap<usize, (E::State, usize)> = HashMap::new();
        let mut digests: HashMap<usize, [u8; 16]> = HashMap::new();
        let mut next_object = 0;
        for tick in 0.. {
            // admit a few new streams per tick
            for _ in 0..(1 + rng.next() % 6) {
                if next_object < objects.len() {
                    in_flight.insert(next_object, (engine.start(), 0));
                    next_object += 1;
                }
            }
            if in_flight.is_empty() {
                break;
            }
            // this tick's chunk per stream: 64 KiB, or nothing for a stalled client
            let plan: HashMap<usize, (usize, usize)> = in_flight
                .iter()
                .map(|(&id, &(_, off))| {
                    let stalled = (id + tick) % 7 == 0;
                    let take = if stalled { 0 } else { (objects[id].len() - off).min(65536) };
                    (id, (off, take))
                })
                .collect();
            engine.update_many(in_flight.iter_mut().map(|(id, (state, _))| {
                let (off, take) = plan[id];
                (state, &objects[*id][off..off + take])
            }));
            for (id, (off, take)) in plan {
                in_flight.get_mut(&id).unwrap().1 = off + take;
                if off + take == objects[id].len() && take < 65536 && !((id + tick) % 7 == 0 && !objects[id].is_empty()) {
                    let (state, _) = in_flight.remove(&id).unwrap();
                    digests.insert(id, engine.finalize(state));
                }
            }
        }
        assert_eq!(digests.len(), objects.len());
        for (id, object) in objects.iter().enumerate() {
            assert_eq!(
                digests[&id],
                Md5Stream::digest(object),
                "engine {} object {id} len {}",
                engine.name(),
                object.len()
            );
        }
    }

    #[test]
    fn inline_engine_matches_md5_stream() {
        assert_eq!(InlineLaneEngine.preferred_batch(), 1);
        lane_server_shape(&InlineLaneEngine);
    }

    #[cfg(feature = "hash-md5-simd")]
    #[test]
    fn simd_engine_matches_md5_stream() {
        let engine = SimdLaneEngine::new();
        assert!(engine.preferred_batch() >= 1);
        assert!(!engine.name().is_empty());
        lane_server_shape(&engine);
    }

    /// Throughput through the port, 32 streams x 1 MiB chunks. `cargo test -p rustfs-utils
    /// --release --features hash-md5-simd -- --ignored --nocapture lanes_throughput`.
    #[test]
    #[ignore = "benchmark, not a correctness test"]
    fn lanes_throughput() {
        fn run<E: Md5LaneEngine>(engine: &E, data: &[Vec<u8>]) -> f64 {
            let mut best = 0f64;
            for _ in 0..3 {
                let mut states: Vec<E::State> = data.iter().map(|_| engine.start()).collect();
                let t = std::time::Instant::now();
                for off in (0..data[0].len()).step_by(1 << 20) {
                    engine.update_many(states.iter_mut().zip(data).map(|(s, d)| (s, &d[off..off + (1 << 20)])));
                }
                let digests: Vec<[u8; 16]> = states.into_iter().map(|s| engine.finalize(s)).collect();
                let secs = t.elapsed().as_secs_f64();
                assert_eq!(digests[7], Md5Stream::digest(&data[7]));
                best = best.max((data.len() * data[0].len()) as f64 / secs / 1048576.0);
            }
            best
        }
        let mut rng = Rng(42);
        let data: Vec<Vec<u8>> = (0..32).map(|_| rng.bytes(8 << 20)).collect();
        println!("{:<20} {:>7.0} MiB/s", InlineLaneEngine.name(), run(&InlineLaneEngine, &data));
        #[cfg(feature = "hash-md5-simd")]
        {
            let engine = SimdLaneEngine::new();
            println!(
                "md5-simd/{:<11} {:>7.0} MiB/s (batch {})",
                engine.name(),
                run(&engine, &data),
                engine.preferred_batch()
            );
        }
    }
}
