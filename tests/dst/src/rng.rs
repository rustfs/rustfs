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

//! Seeded xoshiro256** used for every campaign choice.
//!
//! The 64-bit seed is expanded with splitmix64. The same seed always yields
//! the same schedule on every platform.

#[derive(Clone, Debug)]
pub struct Rng {
    state: [u64; 4],
}

impl Rng {
    pub fn from_seed(seed: u64) -> Self {
        let mut mixer = seed;
        let mut state = [0; 4];
        for word in &mut state {
            *word = splitmix64(&mut mixer);
        }
        Self { state }
    }

    pub fn next_u64(&mut self) -> u64 {
        let s = &mut self.state;
        let result = s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }

    pub fn gen_range(&mut self, exclusive_end: u64) -> u64 {
        if exclusive_end == 0 {
            return 0;
        }
        self.next_u64() % exclusive_end
    }

    pub fn chance(&mut self, numer: u32, denom: u32) -> bool {
        if denom == 0 || numer == 0 {
            return false;
        }
        (self.next_u64() % u64::from(denom)) < u64::from(numer)
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut mixed = *state;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

#[cfg(test)]
mod tests {
    use super::Rng;

    #[test]
    fn seed_one_matches_xoshiro256starstar_vector() {
        let mut rng = Rng::from_seed(1);
        let observed = [rng.next_u64(), rng.next_u64(), rng.next_u64(), rng.next_u64()];
        assert_eq!(
            observed,
            [
                0xb3f2_af6d_0fc7_10c5,
                0x853b_5596_4736_4cea,
                0x92f8_9756_082a_4514,
                0x642e_1c7b_c266_a3a7,
            ]
        );
    }

    #[test]
    fn distinct_seeds_diverge() {
        let mut left = Rng::from_seed(1);
        let mut right = Rng::from_seed(2);
        assert_ne!(left.next_u64(), right.next_u64());
    }
}
