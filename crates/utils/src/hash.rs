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

use blake2::Blake2b512;
use highway::{HighwayHash, HighwayHasher, Key};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::hint::black_box;

const MD5_INITIAL_STATE: [u32; 4] = [0x6745_2301, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476];

const MD5_RC: [u32; 64] = [
    0xd76a_a478,
    0xe8c7_b756,
    0x2420_70db,
    0xc1bd_ceee,
    0xf57c_0faf,
    0x4787_c62a,
    0xa830_4613,
    0xfd46_9501,
    0x6980_98d8,
    0x8b44_f7af,
    0xffff_5bb1,
    0x895c_d7be,
    0x6b90_1122,
    0xfd98_7193,
    0xa679_438e,
    0x49b4_0821,
    0xf61e_2562,
    0xc040_b340,
    0x265e_5a51,
    0xe9b6_c7aa,
    0xd62f_105d,
    0x0244_1453,
    0xd8a1_e681,
    0xe7d3_fbc8,
    0x21e1_cde6,
    0xc337_07d6,
    0xf4d5_0d87,
    0x455a_14ed,
    0xa9e3_e905,
    0xfcef_a3f8,
    0x676f_02d9,
    0x8d2a_4c8a,
    0xfffa_3942,
    0x8771_f681,
    0x6d9d_6122,
    0xfde5_380c,
    0xa4be_ea44,
    0x4bde_cfa9,
    0xf6bb_4b60,
    0xbebf_bc70,
    0x289b_7ec6,
    0xeaa1_27fa,
    0xd4ef_3085,
    0x0488_1d05,
    0xd9d4_d039,
    0xe6db_99e5,
    0x1fa2_7cf8,
    0xc4ac_5665,
    0xf429_2244,
    0x432a_ff97,
    0xab94_23a7,
    0xfc93_a039,
    0x655b_59c3,
    0x8f0c_cc92,
    0xffef_f47d,
    0x8584_5dd1,
    0x6fa8_7e4f,
    0xfe2c_e6e0,
    0xa301_4314,
    0x4e08_11a1,
    0xf753_7e82,
    0xbd3a_f235,
    0x2ad7_d2bb,
    0xeb86_d391,
];

#[inline(always)]
fn md5_op_f(w: u32, x: u32, y: u32, z: u32, m: u32, c: u32, s: u32) -> u32 {
    // Keep `w + m + c` off the `x` critical dependency path from the previous round.
    let base = black_box(w.wrapping_add(m).wrapping_add(c));
    let f = z ^ (x & (y ^ z));
    base.wrapping_add(f).rotate_left(s).wrapping_add(x)
}

#[inline(always)]
fn md5_op_g(w: u32, x: u32, y: u32, z: u32, m: u32, c: u32, s: u32) -> u32 {
    let base = black_box(w.wrapping_add(m).wrapping_add(c)).wrapping_add(y & !z);
    base.wrapping_add(x & z).rotate_left(s).wrapping_add(x)
}

#[inline(always)]
fn md5_op_h(w: u32, x: u32, y: u32, z: u32, m: u32, c: u32, s: u32) -> u32 {
    let base = black_box(w.wrapping_add(m).wrapping_add(c));
    base.wrapping_add(x ^ (y ^ z)).rotate_left(s).wrapping_add(x)
}

#[inline(always)]
fn md5_op_i(w: u32, x: u32, y: u32, z: u32, m: u32, c: u32, s: u32) -> u32 {
    let base = black_box(w.wrapping_add(m).wrapping_add(c));
    base.wrapping_add(y ^ (x | !z)).rotate_left(s).wrapping_add(x)
}

#[inline(always)]
fn md5_word(words: &[[u8; 4]], idx: usize) -> u32 {
    u32::from_le_bytes(words[idx])
}

#[inline]
pub fn compress_md5_blocks(state: &mut [u32; 4], blocks: &[[u8; 64]]) {
    let [mut a, mut b, mut c, mut d] = *state;
    for block in blocks {
        let words = block.as_chunks::<4>().0;
        let aa = a;
        let bb = b;
        let cc = c;
        let dd = d;

        a = md5_op_f(a, b, c, d, md5_word(words, 0), MD5_RC[0], 7);
        d = md5_op_f(d, a, b, c, md5_word(words, 1), MD5_RC[1], 12);
        c = md5_op_f(c, d, a, b, md5_word(words, 2), MD5_RC[2], 17);
        b = md5_op_f(b, c, d, a, md5_word(words, 3), MD5_RC[3], 22);
        a = md5_op_f(a, b, c, d, md5_word(words, 4), MD5_RC[4], 7);
        d = md5_op_f(d, a, b, c, md5_word(words, 5), MD5_RC[5], 12);
        c = md5_op_f(c, d, a, b, md5_word(words, 6), MD5_RC[6], 17);
        b = md5_op_f(b, c, d, a, md5_word(words, 7), MD5_RC[7], 22);
        a = md5_op_f(a, b, c, d, md5_word(words, 8), MD5_RC[8], 7);
        d = md5_op_f(d, a, b, c, md5_word(words, 9), MD5_RC[9], 12);
        c = md5_op_f(c, d, a, b, md5_word(words, 10), MD5_RC[10], 17);
        b = md5_op_f(b, c, d, a, md5_word(words, 11), MD5_RC[11], 22);
        a = md5_op_f(a, b, c, d, md5_word(words, 12), MD5_RC[12], 7);
        d = md5_op_f(d, a, b, c, md5_word(words, 13), MD5_RC[13], 12);
        c = md5_op_f(c, d, a, b, md5_word(words, 14), MD5_RC[14], 17);
        b = md5_op_f(b, c, d, a, md5_word(words, 15), MD5_RC[15], 22);

        a = md5_op_g(a, b, c, d, md5_word(words, 1), MD5_RC[16], 5);
        d = md5_op_g(d, a, b, c, md5_word(words, 6), MD5_RC[17], 9);
        c = md5_op_g(c, d, a, b, md5_word(words, 11), MD5_RC[18], 14);
        b = md5_op_g(b, c, d, a, md5_word(words, 0), MD5_RC[19], 20);
        a = md5_op_g(a, b, c, d, md5_word(words, 5), MD5_RC[20], 5);
        d = md5_op_g(d, a, b, c, md5_word(words, 10), MD5_RC[21], 9);
        c = md5_op_g(c, d, a, b, md5_word(words, 15), MD5_RC[22], 14);
        b = md5_op_g(b, c, d, a, md5_word(words, 4), MD5_RC[23], 20);
        a = md5_op_g(a, b, c, d, md5_word(words, 9), MD5_RC[24], 5);
        d = md5_op_g(d, a, b, c, md5_word(words, 14), MD5_RC[25], 9);
        c = md5_op_g(c, d, a, b, md5_word(words, 3), MD5_RC[26], 14);
        b = md5_op_g(b, c, d, a, md5_word(words, 8), MD5_RC[27], 20);
        a = md5_op_g(a, b, c, d, md5_word(words, 13), MD5_RC[28], 5);
        d = md5_op_g(d, a, b, c, md5_word(words, 2), MD5_RC[29], 9);
        c = md5_op_g(c, d, a, b, md5_word(words, 7), MD5_RC[30], 14);
        b = md5_op_g(b, c, d, a, md5_word(words, 12), MD5_RC[31], 20);

        a = md5_op_h(a, b, c, d, md5_word(words, 5), MD5_RC[32], 4);
        d = md5_op_h(d, a, b, c, md5_word(words, 8), MD5_RC[33], 11);
        c = md5_op_h(c, d, a, b, md5_word(words, 11), MD5_RC[34], 16);
        b = md5_op_h(b, c, d, a, md5_word(words, 14), MD5_RC[35], 23);
        a = md5_op_h(a, b, c, d, md5_word(words, 1), MD5_RC[36], 4);
        d = md5_op_h(d, a, b, c, md5_word(words, 4), MD5_RC[37], 11);
        c = md5_op_h(c, d, a, b, md5_word(words, 7), MD5_RC[38], 16);
        b = md5_op_h(b, c, d, a, md5_word(words, 10), MD5_RC[39], 23);
        a = md5_op_h(a, b, c, d, md5_word(words, 13), MD5_RC[40], 4);
        d = md5_op_h(d, a, b, c, md5_word(words, 0), MD5_RC[41], 11);
        c = md5_op_h(c, d, a, b, md5_word(words, 3), MD5_RC[42], 16);
        b = md5_op_h(b, c, d, a, md5_word(words, 6), MD5_RC[43], 23);
        a = md5_op_h(a, b, c, d, md5_word(words, 9), MD5_RC[44], 4);
        d = md5_op_h(d, a, b, c, md5_word(words, 12), MD5_RC[45], 11);
        c = md5_op_h(c, d, a, b, md5_word(words, 15), MD5_RC[46], 16);
        b = md5_op_h(b, c, d, a, md5_word(words, 2), MD5_RC[47], 23);

        a = md5_op_i(a, b, c, d, md5_word(words, 0), MD5_RC[48], 6);
        d = md5_op_i(d, a, b, c, md5_word(words, 7), MD5_RC[49], 10);
        c = md5_op_i(c, d, a, b, md5_word(words, 14), MD5_RC[50], 15);
        b = md5_op_i(b, c, d, a, md5_word(words, 5), MD5_RC[51], 21);
        a = md5_op_i(a, b, c, d, md5_word(words, 12), MD5_RC[52], 6);
        d = md5_op_i(d, a, b, c, md5_word(words, 3), MD5_RC[53], 10);
        c = md5_op_i(c, d, a, b, md5_word(words, 10), MD5_RC[54], 15);
        b = md5_op_i(b, c, d, a, md5_word(words, 1), MD5_RC[55], 21);
        a = md5_op_i(a, b, c, d, md5_word(words, 8), MD5_RC[56], 6);
        d = md5_op_i(d, a, b, c, md5_word(words, 15), MD5_RC[57], 10);
        c = md5_op_i(c, d, a, b, md5_word(words, 6), MD5_RC[58], 15);
        b = md5_op_i(b, c, d, a, md5_word(words, 13), MD5_RC[59], 21);
        a = md5_op_i(a, b, c, d, md5_word(words, 4), MD5_RC[60], 6);
        d = md5_op_i(d, a, b, c, md5_word(words, 11), MD5_RC[61], 10);
        c = md5_op_i(c, d, a, b, md5_word(words, 2), MD5_RC[62], 15);
        b = md5_op_i(b, c, d, a, md5_word(words, 9), MD5_RC[63], 21);

        a = a.wrapping_add(aa);
        b = b.wrapping_add(bb);
        c = c.wrapping_add(cc);
        d = d.wrapping_add(dd);
    }
    *state = [a, b, c, d];
}

#[inline]
fn finalize_md5_state(mut state: [u32; 4], tail: &[u8], total_bytes: u64) -> [u8; 16] {
    debug_assert!(tail.len() < 64);
    let mut pad = [0u8; 128];
    pad[..tail.len()].copy_from_slice(tail);
    pad[tail.len()] = 0x80;
    let bit_len = total_bytes.wrapping_mul(8).to_le_bytes();
    if tail.len() < 56 {
        pad[56..64].copy_from_slice(&bit_len);
        compress_md5_blocks(&mut state, pad[..64].as_chunks::<64>().0);
    } else {
        pad[120..128].copy_from_slice(&bit_len);
        compress_md5_blocks(&mut state, pad.as_chunks::<64>().0);
    }
    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&state[0].to_le_bytes());
    out[4..8].copy_from_slice(&state[1].to_le_bytes());
    out[8..12].copy_from_slice(&state[2].to_le_bytes());
    out[12..16].copy_from_slice(&state[3].to_le_bytes());
    out
}

/// Streaming MD5 hasher using critical-path ILP reassociation and direct slice chunking.
#[derive(Clone, Debug)]
pub struct FastMd5 {
    state: [u32; 4],
    buf: [u8; 64],
    buf_len: usize,
    total_bytes: u64,
}

impl Default for FastMd5 {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl FastMd5 {
    #[inline]
    pub const fn new() -> Self {
        Self {
            state: MD5_INITIAL_STATE,
            buf: [0u8; 64],
            buf_len: 0,
            total_bytes: 0,
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.state = MD5_INITIAL_STATE;
        self.buf_len = 0;
        self.total_bytes = 0;
    }

    #[inline]
    pub fn update(&mut self, mut data: &[u8]) {
        if data.is_empty() {
            return;
        }
        self.total_bytes = self.total_bytes.wrapping_add(data.len() as u64);
        if self.buf_len > 0 {
            let need = 64 - self.buf_len;
            if data.len() < need {
                self.buf[self.buf_len..self.buf_len + data.len()].copy_from_slice(data);
                self.buf_len += data.len();
                return;
            }
            self.buf[self.buf_len..64].copy_from_slice(&data[..need]);
            compress_md5_blocks(&mut self.state, std::slice::from_ref(&self.buf));
            self.buf_len = 0;
            data = &data[need..];
        }
        let (blocks, remainder) = data.as_chunks::<64>();
        if !blocks.is_empty() {
            compress_md5_blocks(&mut self.state, blocks);
        }
        if !remainder.is_empty() {
            self.buf[..remainder.len()].copy_from_slice(remainder);
            self.buf_len = remainder.len();
        }
    }

    #[inline]
    pub fn finalize_bytes(&self) -> [u8; 16] {
        finalize_md5_state(self.state, &self.buf[..self.buf_len], self.total_bytes)
    }

    #[inline]
    pub fn finalize(self) -> [u8; 16] {
        self.finalize_bytes()
    }

    #[inline]
    pub fn digest(data: &[u8]) -> [u8; 16] {
        let mut state = MD5_INITIAL_STATE;
        let (blocks, remainder) = data.as_chunks::<64>();
        if !blocks.is_empty() {
            compress_md5_blocks(&mut state, blocks);
        }
        finalize_md5_state(state, remainder, data.len() as u64)
    }
}

/// Magic HH-256 key: HH-256 hash of first 100 decimals of π as utf-8 with zero key.
const MAGIC_HIGHWAY_HASH256_KEY: [u8; 32] = [
    0x4b, 0xe7, 0x34, 0xfa, 0x8e, 0x23, 0x8a, 0xcd, 0x26, 0x3e, 0x83, 0xe6, 0xbb, 0x96, 0x85, 0x52, 0x04, 0x0f, 0x93, 0x5d, 0xa3,
    0x9f, 0x44, 0x14, 0x97, 0xe0, 0x9d, 0x13, 0x22, 0xde, 0x36, 0xa0,
];

/// Legacy HH-256 key (main branch): fixed [3,4,2,1] as u64 LE.
const LEGACY_HIGHWAY_HASH256_KEY: [u8; 32] = [
    3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
];

const fn highway_key_from_bytes(bytes: &[u8; 32]) -> [u64; 4] {
    [
        u64::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]),
        u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]),
        u64::from_le_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
        ]),
        u64::from_le_bytes([
            bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30], bytes[31],
        ]),
    ]
}

const MAGIC_HIGHWAY_HASH256_PARSED_KEY: Key = Key(highway_key_from_bytes(&MAGIC_HIGHWAY_HASH256_KEY));
const LEGACY_HIGHWAY_HASH256_PARSED_KEY: Key = Key(highway_key_from_bytes(&LEGACY_HIGHWAY_HASH256_KEY));

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone, Eq, Hash)]
/// Supported hash algorithms for bitrot protection.
pub enum HashAlgorithm {
    // SHA256 represents the SHA-256 hash function
    SHA256,
    // HighwayHash256 represents the HighwayHash-256 hash function
    HighwayHash256,
    // HighwayHash256S represents the Streaming HighwayHash-256 hash function
    #[default]
    HighwayHash256S,
    /// Legacy HighwayHash256S (main branch) with fixed key [3,4,2,1]
    HighwayHash256SLegacy,
    // BLAKE2b512 represents the BLAKE2b-512 hash function
    BLAKE2b512,
    /// MD5 (128-bit)
    Md5,
    /// No hash (for testing or unprotected data)
    None,
}

enum HashEncoded {
    Md5([u8; 16]),
    Sha256([u8; 32]),
    HighwayHash256([u8; 32]),
    HighwayHash256S([u8; 32]),
    HighwayHash256SLegacy([u8; 32]),
    Blake2b512([u8; 64]),
    None,
}

impl AsRef<[u8]> for HashEncoded {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        match self {
            HashEncoded::Md5(hash) => hash.as_ref(),
            HashEncoded::Sha256(hash) => hash.as_ref(),
            HashEncoded::HighwayHash256(hash) => hash.as_ref(),
            HashEncoded::HighwayHash256S(hash) => hash.as_ref(),
            HashEncoded::HighwayHash256SLegacy(hash) => hash.as_ref(),
            HashEncoded::Blake2b512(hash) => hash.as_ref(),
            HashEncoded::None => &[],
        }
    }
}

#[inline]
fn u8x32_from_u64x4(input: [u64; 4]) -> [u8; 32] {
    let mut output = [0u8; 32];
    for (i, &n) in input.iter().enumerate() {
        output[i * 8..(i + 1) * 8].copy_from_slice(&n.to_le_bytes());
    }
    output
}

impl HashAlgorithm {
    /// Hash the input data and return the hash result as Vec<u8>.
    ///
    /// # Arguments
    /// * `data` - A byte slice representing the data to be hashed
    ///
    /// # Returns
    /// A byte slice containing the hash of the input data
    ///
    #[inline]
    pub fn hash_encode(&self, data: &[u8]) -> impl AsRef<[u8]> {
        match self {
            HashAlgorithm::Md5 => HashEncoded::Md5(FastMd5::digest(data)),
            HashAlgorithm::HighwayHash256 => {
                let mut hasher = HighwayHasher::new(MAGIC_HIGHWAY_HASH256_PARSED_KEY);
                hasher.append(data);
                HashEncoded::HighwayHash256(u8x32_from_u64x4(hasher.finalize256()))
            }
            HashAlgorithm::SHA256 => HashEncoded::Sha256(Sha256::digest(data).into()),
            HashAlgorithm::HighwayHash256S => {
                let mut hasher = HighwayHasher::new(MAGIC_HIGHWAY_HASH256_PARSED_KEY);
                hasher.append(data);
                HashEncoded::HighwayHash256S(u8x32_from_u64x4(hasher.finalize256()))
            }
            HashAlgorithm::HighwayHash256SLegacy => {
                let mut hasher = HighwayHasher::new(LEGACY_HIGHWAY_HASH256_PARSED_KEY);
                hasher.append(data);
                HashEncoded::HighwayHash256SLegacy(u8x32_from_u64x4(hasher.finalize256()))
            }
            HashAlgorithm::BLAKE2b512 => {
                let hash = Blake2b512::digest(data);
                let mut out = [0u8; 64];
                out.copy_from_slice(hash.as_ref());
                HashEncoded::Blake2b512(out)
            }
            HashAlgorithm::None => HashEncoded::None,
        }
    }

    /// Hash byte slices as one logical byte stream without concatenating them.
    #[inline]
    pub fn hash_encode_slices<'a, I>(&self, slices: I) -> impl AsRef<[u8]>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        match self {
            HashAlgorithm::Md5 => {
                let mut hasher = FastMd5::new();
                for slice in slices {
                    hasher.update(slice);
                }
                HashEncoded::Md5(hasher.finalize_bytes())
            }
            HashAlgorithm::HighwayHash256 => {
                let mut hasher = HighwayHasher::new(MAGIC_HIGHWAY_HASH256_PARSED_KEY);
                for slice in slices {
                    hasher.append(slice);
                }
                HashEncoded::HighwayHash256(u8x32_from_u64x4(hasher.finalize256()))
            }
            HashAlgorithm::SHA256 => {
                let mut hasher = Sha256::new();
                for slice in slices {
                    hasher.update(slice);
                }
                HashEncoded::Sha256(hasher.finalize().into())
            }
            HashAlgorithm::HighwayHash256S => {
                let mut hasher = HighwayHasher::new(MAGIC_HIGHWAY_HASH256_PARSED_KEY);
                for slice in slices {
                    hasher.append(slice);
                }
                HashEncoded::HighwayHash256S(u8x32_from_u64x4(hasher.finalize256()))
            }
            HashAlgorithm::HighwayHash256SLegacy => {
                let mut hasher = HighwayHasher::new(LEGACY_HIGHWAY_HASH256_PARSED_KEY);
                for slice in slices {
                    hasher.append(slice);
                }
                HashEncoded::HighwayHash256SLegacy(u8x32_from_u64x4(hasher.finalize256()))
            }
            HashAlgorithm::BLAKE2b512 => {
                let mut hasher = Blake2b512::new();
                for slice in slices {
                    hasher.update(slice);
                }
                let hash = hasher.finalize();
                let mut out = [0u8; 64];
                out.copy_from_slice(hash.as_ref());
                HashEncoded::Blake2b512(out)
            }
            HashAlgorithm::None => HashEncoded::None,
        }
    }

    /// Return the output size in bytes for the hash algorithm.
    ///
    /// # Returns
    /// The size in bytes of the hash output
    ///
    pub fn size(&self) -> usize {
        match self {
            HashAlgorithm::SHA256 => 32,
            HashAlgorithm::HighwayHash256 => 32,
            HashAlgorithm::HighwayHash256S => 32,
            HashAlgorithm::HighwayHash256SLegacy => 32,
            HashAlgorithm::BLAKE2b512 => 64,
            HashAlgorithm::Md5 => 16,
            HashAlgorithm::None => 0,
        }
    }
}

use siphasher::sip::SipHasher;

pub const EMPTY_STRING_SHA256_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

pub const DEFAULT_SIP_HASH_KEY: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

/// SipHash function to hash a string key into a bucket index.
///
/// # Arguments
/// * `key` - The input string to be hashed
/// * `cardinality` - The number of buckets
/// * `id` - A 16-byte array used as the SipHash key
///
/// # Returns
/// A usize representing the bucket index
///
pub fn sip_hash(key: &str, cardinality: usize, id: &[u8; 16]) -> usize {
    // Your key, must be 16 bytes

    // Calculate SipHash value of the string
    let result = SipHasher::new_with_key(id).hash(key.as_bytes());

    (result as usize) % cardinality
}

/// CRC32 hash function to hash a string key into a bucket index.
///
/// # Arguments
/// * `key` - The input string to be hashed
/// * `cardinality` - The number of buckets
///
/// # Returns
/// A usize representing the bucket index
///
pub fn crc_hash(key: &str, cardinality: usize) -> usize {
    let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
    hasher.update(key.as_bytes());
    let checksum = hasher.finalize() as u32;

    checksum as usize % cardinality
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_algorithm_sizes() {
        assert_eq!(HashAlgorithm::Md5.size(), 16);
        assert_eq!(HashAlgorithm::HighwayHash256.size(), 32);
        assert_eq!(HashAlgorithm::HighwayHash256S.size(), 32);
        assert_eq!(HashAlgorithm::SHA256.size(), 32);
        assert_eq!(HashAlgorithm::BLAKE2b512.size(), 64);
        assert_eq!(HashAlgorithm::None.size(), 0);
    }

    #[test]
    fn test_hash_encode_none() {
        let data = b"test data";
        let hash = HashAlgorithm::None.hash_encode(data);
        let hash = hash.as_ref();
        assert_eq!(hash.len(), 0);
    }

    #[test]
    fn hash_encode_slices_matches_contiguous_for_all_algorithms() {
        let data = b"fragmented bitrot hash input";
        let slices = [&data[..3], &data[3..11], &data[11..], &[]];

        for algo in [
            HashAlgorithm::Md5,
            HashAlgorithm::SHA256,
            HashAlgorithm::HighwayHash256,
            HashAlgorithm::HighwayHash256S,
            HashAlgorithm::HighwayHash256SLegacy,
            HashAlgorithm::BLAKE2b512,
            HashAlgorithm::None,
        ] {
            assert_eq!(
                algo.hash_encode_slices(slices).as_ref(),
                algo.hash_encode(data).as_ref(),
                "fragmented hash must match contiguous hash for {algo:?}"
            );
        }
    }

    #[test]
    fn test_hash_encode_md5() {
        let data = b"test data";
        let hash = HashAlgorithm::Md5.hash_encode(data);
        let hash = hash.as_ref();
        assert_eq!(hash.len(), 16);
        // MD5 should be deterministic
        let hash2 = HashAlgorithm::Md5.hash_encode(data);
        let hash2 = hash2.as_ref();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_encode_highway() {
        let data = b"test data";
        let hash = HashAlgorithm::HighwayHash256.hash_encode(data);
        let hash = hash.as_ref();
        assert_eq!(hash.len(), 32);
        // HighwayHash should be deterministic
        let hash2 = HashAlgorithm::HighwayHash256.hash_encode(data);
        let hash2 = hash2.as_ref();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_encode_sha256() {
        let data = b"test data";
        let hash = HashAlgorithm::SHA256.hash_encode(data);
        let hash = hash.as_ref();
        assert_eq!(hash.len(), 32);
        // SHA256 should be deterministic
        let hash2 = HashAlgorithm::SHA256.hash_encode(data);
        let hash2 = hash2.as_ref();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_encode_blake2b512() {
        let data = b"test data";
        let hash = HashAlgorithm::BLAKE2b512.hash_encode(data);
        let hash = hash.as_ref();
        assert_eq!(hash.len(), 64);
        // BLAKE2b512 should be deterministic
        let hash2 = HashAlgorithm::BLAKE2b512.hash_encode(data);
        let hash2 = hash2.as_ref();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_bitrot_selftest() {
        let checksums: [(HashAlgorithm, &str); 5] = [
            (HashAlgorithm::SHA256, "a7677ff19e0182e4d52e3a3db727804abc82a5818749336369552e54b838b004"),
            (
                HashAlgorithm::BLAKE2b512,
                "e519b7d84b1c3c917985f544773a35cf265dcab10948be3550320d156bab612124a5ae2ae5a8c73c0eea360f68b0e28136f26e858756dbfe7375a7389f26c669",
            ),
            (
                HashAlgorithm::HighwayHash256,
                "39c0407ed3f01b18d22c85db4aeff11e060ca5f43131b0126731ca197cd42313",
            ),
            (
                HashAlgorithm::HighwayHash256S,
                "39c0407ed3f01b18d22c85db4aeff11e060ca5f43131b0126731ca197cd42313",
            ),
            (
                HashAlgorithm::HighwayHash256SLegacy,
                "a5592a831588836b0f61bff43da4bd957c376d9b6412a9ecbbd144a3ecf34649",
            ),
        ];
        for (algo, expected_hex) in checksums {
            let block_size = match algo {
                HashAlgorithm::SHA256 => 64,
                HashAlgorithm::BLAKE2b512 => 128,
                HashAlgorithm::HighwayHash256 | HashAlgorithm::HighwayHash256S | HashAlgorithm::HighwayHash256SLegacy => 32,
                _ => continue,
            };
            let mut msg = Vec::new();
            let mut sum = Vec::new();
            for _ in 0..block_size {
                sum = algo.hash_encode(&msg).as_ref().to_vec();
                msg.extend_from_slice(&sum);
            }
            let got = hex_simd::encode_to_string(&sum, hex_simd::AsciiCase::Lower);
            assert_eq!(got, expected_hex, "{:?} selftest mismatch: got {} want {}", algo, got, expected_hex);
        }
    }

    /// Generates 7557 bytes
    /// Pattern: (i*7+13)%256 for each byte.
    fn generate_compat_test_data(size: usize) -> Vec<u8> {
        (0..size).map(|i| ((i * 7 + 13) % 256) as u8).collect()
    }

    /// Run: cargo test -p rustfs-utils test_highwayhash_compat
    #[test]
    fn test_highwayhash_compat() {
        let data = generate_compat_test_data(7557);
        let hash = HashAlgorithm::HighwayHash256S.hash_encode(&data);
        let got = hex_simd::encode_to_string(hash.as_ref(), hex_simd::AsciiCase::Lower);
        let expected = "06543bf1c637e67386922a43b71cca08e5faa0f9131105a2bf96dec880529551";
        assert_eq!(got, expected, "HighwayHash256S must match: got {} want {}", got, expected);
    }

    #[test]
    fn test_different_data_different_hashes() {
        let data1 = b"test data 1";
        let data2 = b"test data 2";

        let md5_hash1 = HashAlgorithm::Md5.hash_encode(data1);
        let md5_hash2 = HashAlgorithm::Md5.hash_encode(data2);
        assert_ne!(md5_hash1.as_ref(), md5_hash2.as_ref());

        let highway_hash1 = HashAlgorithm::HighwayHash256.hash_encode(data1);
        let highway_hash2 = HashAlgorithm::HighwayHash256.hash_encode(data2);
        assert_ne!(highway_hash1.as_ref(), highway_hash2.as_ref());

        let sha256_hash1 = HashAlgorithm::SHA256.hash_encode(data1);
        let sha256_hash2 = HashAlgorithm::SHA256.hash_encode(data2);
        assert_ne!(sha256_hash1.as_ref(), sha256_hash2.as_ref());

        let blake_hash1 = HashAlgorithm::BLAKE2b512.hash_encode(data1);
        let blake_hash2 = HashAlgorithm::BLAKE2b512.hash_encode(data2);
        assert_ne!(blake_hash1.as_ref(), blake_hash2.as_ref());
    }

    #[test]
    fn fast_md5_matches_reference_across_boundaries_and_chunkings() {
        use md5::{Digest as _, Md5};

        for len in (0..=260).chain([511, 512, 513, 1024, 4096, 8193]) {
            let data = generate_compat_test_data(len);
            let expected: [u8; 16] = Md5::digest(&data).into();
            assert_eq!(FastMd5::digest(&data), expected, "FastMd5::digest mismatch at len={len}");

            for step in [1usize, 3, 7, 16, 31, 55, 56, 63, 64, 65, 128] {
                let mut fast = FastMd5::new();
                for chunk in data.chunks(step) {
                    fast.update(chunk);
                }
                assert_eq!(fast.finalize_bytes(), expected, "FastMd5 streaming mismatch at len={len}, step={step}");
                assert_eq!(fast.finalize(), expected, "FastMd5 repeated finalize mismatch at len={len}, step={step}");
            }
        }
    }
}
