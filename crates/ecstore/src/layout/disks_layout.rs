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

use rustfs_utils::string::{ArgPattern, find_ellipses_patterns, has_ellipses};
use serde::Deserialize;
use std::collections::HashSet;
use std::env;
use std::io::{Error, Result};
use tracing::debug;

/// Supported set sizes this is used to find the optimal
/// single set size.
const SET_SIZES: [usize; 15] = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
const ENV_RUSTFS_ERASURE_SET_DRIVE_COUNT: &str = "RUSTFS_ERASURE_SET_DRIVE_COUNT";

#[derive(Deserialize, Debug, Default)]
pub struct PoolDisksLayout {
    cmd_line: String,
    layout: Vec<Vec<String>>,
}

impl PoolDisksLayout {
    fn new(args: impl Into<String>, layout: Vec<Vec<String>>) -> Self {
        PoolDisksLayout {
            cmd_line: args.into(),
            layout,
        }
    }

    fn count(&self) -> usize {
        self.layout.len()
    }

    fn get_cmd_line(&self) -> &str {
        &self.cmd_line
    }

    pub fn iter(&self) -> impl Iterator<Item = &Vec<String>> {
        self.layout.iter()
    }
}

#[derive(Deserialize, Debug, Default)]
pub struct DisksLayout {
    pub legacy: bool,
    pub pools: Vec<PoolDisksLayout>,
}

// impl<T: AsRef<str>> TryFrom<&[T]> for DisksLayout {
//     type Error = Error;

//     fn try_from(args: &[T]) -> Result<Self, Self::Error> {
//         if args.is_empty() {
//             return Err(Error::from_string("Invalid argument"));
//         }

//         let is_ellipses = args.iter().any(|v| has_ellipses(&[v]));

//         // None of the args have ellipses use the old style.
//         if !is_ellipses {
//             let set_args = get_all_sets(is_ellipses, args)?;

//             return Ok(DisksLayout {
//                 legacy: true,
//                 pools: vec![PoolDisksLayout::new(
//                     args.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join(" "),
//                     set_args,
//                 )],
//             });
//         }

//         let mut layout = Vec::with_capacity(args.len());
//         for arg in args.iter() {
//             if !has_ellipses(&[arg]) && args.len() > 1 {
//                 return Err(Error::from_string(
//                     "all args must have ellipses for pool expansion (Invalid arguments specified)",
//                 ));
//             }

//             let set_args = get_all_sets(is_ellipses, &[arg])?;

//             layout.push(PoolDisksLayout::new(arg.as_ref(), set_args));
//         }

//         Ok(DisksLayout {
//             legacy: false,
//             pools: layout,
//         })
//     }
// }

impl DisksLayout {
    pub fn from_volumes<T: AsRef<str>>(args: &[T]) -> Result<Self> {
        if args.is_empty() {
            return Err(Error::other("Invalid argument"));
        }

        let is_ellipses = args.iter().any(|v| has_ellipses(&[v]));

        let set_drive_count_env = env::var(ENV_RUSTFS_ERASURE_SET_DRIVE_COUNT).unwrap_or_else(|err| {
            debug!("{} not set use default:0, {:?}", ENV_RUSTFS_ERASURE_SET_DRIVE_COUNT, err);
            "0".to_string()
        });
        let set_drive_count: usize = set_drive_count_env.parse().map_err(Error::other)?;

        // None of the args have ellipses use the old style.
        if !is_ellipses {
            let set_args = get_all_sets(set_drive_count, is_ellipses, args)?;

            return Ok(DisksLayout {
                legacy: true,
                pools: vec![PoolDisksLayout::new(
                    args.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join(" "),
                    set_args,
                )],
            });
        }

        let mut layout = Vec::with_capacity(args.len());
        for arg in args.iter() {
            if !has_ellipses(&[arg]) && args.len() > 1 {
                return Err(Error::other(
                    "all args must have ellipses for pool expansion (Invalid arguments specified)",
                ));
            }

            let set_args = get_all_sets(set_drive_count, is_ellipses, &[arg])?;

            layout.push(PoolDisksLayout::new(arg.as_ref(), set_args));
        }

        Ok(DisksLayout {
            legacy: false,
            pools: layout,
        })
    }

    pub fn is_empty_layout(&self) -> bool {
        self.pools.is_empty()
            || self.pools[0].layout.is_empty()
            || self.pools[0].layout[0].is_empty()
            || self.pools[0].layout[0][0].is_empty()
    }

    pub fn is_single_drive_layout(&self) -> bool {
        self.pools.len() == 1 && self.pools[0].layout.len() == 1 && self.pools[0].layout[0].len() == 1
    }

    pub fn get_single_drive_layout(&self) -> &str {
        &self.pools[0].layout[0][0]
    }

    /// returns the total number of sets in the layout.
    pub fn get_set_count(&self, i: usize) -> usize {
        self.pools.get(i).map_or(0, |v| v.count())
    }

    /// returns the total number of drives in the layout.
    pub fn get_drives_per_set(&self, i: usize) -> usize {
        self.pools.get(i).map_or(0, |v| v.layout.first().map_or(0, |v| v.len()))
    }

    /// returns the command line for the given index.
    pub fn get_cmd_line(&self, i: usize) -> String {
        self.pools.get(i).map_or(String::new(), |v| v.get_cmd_line().to_owned())
    }
}

/// parses all ellipses input arguments, expands them into
/// corresponding list of endpoints chunked evenly in accordance with a
/// specific set size.
///
/// For example: {1...64} is divided into 4 sets each of size 16.
/// This applies to even distributed setup syntax as well.
fn get_all_sets<T: AsRef<str>>(set_drive_count: usize, is_ellipses: bool, args: &[T]) -> Result<Vec<Vec<String>>> {
    let endpoint_set = if is_ellipses {
        EndpointSet::from_volumes(args, set_drive_count)?
    } else {
        let set_indexes = if args.len() > 1 {
            get_set_indexes(args, &[args.len()], set_drive_count, &[])?
        } else {
            vec![vec![args.len()]]
        };
        let endpoints = args.iter().map(|v| v.as_ref().to_string()).collect();

        EndpointSet::new(endpoints, set_indexes)
    };

    let set_args = endpoint_set.get();

    let mut unique_args = HashSet::with_capacity(set_args.len());
    for args in set_args.iter() {
        for arg in args {
            if unique_args.contains(arg) {
                return Err(Error::other(format!("Input args {arg} has duplicate ellipses")));
            }
            unique_args.insert(arg);
        }
    }

    Ok(set_args)
}

/// represents parsed ellipses values, also provides
/// methods to get the sets of endpoints.
#[derive(Debug, Default)]
struct EndpointSet {
    _arg_patterns: Vec<ArgPattern>,
    endpoints: Vec<String>,
    set_indexes: Vec<Vec<usize>>,
}

// impl<T: AsRef<str>> TryFrom<&[T]> for EndpointSet {
//     type Error = Error;

//     fn try_from(args: &[T]) -> Result<Self, Self::Error> {
//         let mut arg_patterns = Vec::with_capacity(args.len());
//         for arg in args {
//             arg_patterns.push(find_ellipses_patterns(arg.as_ref())?);
//         }

//         let total_sizes = get_total_sizes(&arg_patterns);
//         let set_indexes = get_set_indexes(args, &total_sizes, &arg_patterns)?;

//         let mut endpoints = Vec::new();
//         for ap in arg_patterns.iter() {
//             let aps = ap.expand();
//             for bs in aps {
//                 endpoints.push(bs.join(""));
//             }
//         }

//         Ok(EndpointSet {
//             set_indexes,
//             _arg_patterns: arg_patterns,
//             endpoints,
//         })
//     }
// }

impl EndpointSet {
    /// Create a new EndpointSet with the given endpoints and set indexes.
    pub fn new(endpoints: Vec<String>, set_indexes: Vec<Vec<usize>>) -> Self {
        Self {
            endpoints,
            set_indexes,
            ..Default::default()
        }
    }

    pub fn from_volumes<T: AsRef<str>>(args: &[T], set_drive_count: usize) -> Result<Self> {
        let mut arg_patterns = Vec::with_capacity(args.len());
        for arg in args {
            arg_patterns.push(find_ellipses_patterns(arg.as_ref())?);
        }

        let total_sizes = get_total_sizes(&arg_patterns);
        let set_indexes = get_set_indexes(args, &total_sizes, set_drive_count, &arg_patterns)?;

        let mut endpoints = Vec::new();
        for ap in arg_patterns.iter() {
            let aps = ap.expand();
            for bs in aps {
                endpoints.push(bs.join(""));
            }
        }

        Ok(EndpointSet {
            set_indexes,
            _arg_patterns: arg_patterns,
            endpoints,
        })
    }

    /// returns the sets representation of the endpoints
    /// this function also intelligently decides on what will
    /// be the right set size etc.
    pub fn get(&self) -> Vec<Vec<String>> {
        let mut sets: Vec<Vec<String>> = Vec::new();

        let mut start = 0;
        for set_idx in self.set_indexes.iter() {
            for idx in set_idx {
                let end = idx + start;
                sets.push(self.endpoints[start..end].to_vec());
                start = end;
            }
        }
        sets
    }
}

/// returns the greatest common divisor of all the ellipses sizes.
fn get_divisible_size(total_sizes: &[usize]) -> usize {
    fn gcd(mut x: usize, mut y: usize) -> usize {
        while y != 0 {
            // be equivalent to: x, y = y, x%y
            std::mem::swap(&mut x, &mut y);
            y %= x;
        }
        x
    }

    total_sizes.iter().skip(1).fold(total_sizes[0], |acc, &y| gcd(acc, y))
}

fn possible_set_counts(set_size: usize) -> Vec<usize> {
    let mut ss = Vec::new();
    for s in SET_SIZES {
        if set_size.is_multiple_of(s) {
            ss.push(s);
        }
    }
    ss
}

/// checks whether given count is a valid set size for erasure coding.
fn is_valid_set_size(count: usize) -> bool {
    count >= SET_SIZES[0] && count <= SET_SIZES[SET_SIZES.len() - 1]
}

/// Final set size with all the symmetry accounted for.
fn common_set_drive_count(divisible_size: usize, set_counts: &[usize]) -> usize {
    // prefers set_counts to be sorted for optimal behavior.
    if divisible_size < set_counts[set_counts.len() - 1] {
        return divisible_size;
    }

    let mut prev_d = divisible_size / set_counts[0];
    let mut set_size = 0;
    for &cnt in set_counts {
        if divisible_size.is_multiple_of(cnt) {
            let d = divisible_size / cnt;
            if d <= prev_d {
                prev_d = d;
                set_size = cnt;
            }
        }
    }
    set_size
}

/// returns symmetrical setCounts based on the input argument patterns,
/// the symmetry calculation is to ensure that we also use uniform number
/// of drives common across all ellipses patterns.
fn possible_set_counts_with_symmetry(set_counts: &[usize], arg_patterns: &[ArgPattern]) -> Vec<usize> {
    let mut new_set_counts: HashSet<usize> = HashSet::new();

    for &ss in set_counts {
        let mut symmetry = false;
        for arg_pattern in arg_patterns {
            for p in arg_pattern.as_ref().iter() {
                if p.len() > ss {
                    symmetry = (p.len() % ss) == 0;
                } else {
                    symmetry = (ss % p.len()) == 0;
                }
            }
        }

        if !new_set_counts.contains(&ss) && (symmetry || arg_patterns.is_empty()) {
            new_set_counts.insert(ss);
        }
    }

    let mut set_counts: Vec<usize> = new_set_counts.into_iter().collect();
    set_counts.sort_unstable();

    set_counts
}

/// returns list of indexes which provides the set size
/// on each index, this function also determines the final set size
/// The final set size has the affinity towards choosing smaller
/// indexes (total sets)
fn get_set_indexes<T: AsRef<str>>(
    args: &[T],
    total_sizes: &[usize],
    set_drive_count: usize,
    arg_patterns: &[ArgPattern],
) -> Result<Vec<Vec<usize>>> {
    if args.is_empty() || total_sizes.is_empty() {
        return Err(Error::other("Invalid argument"));
    }

    for &size in total_sizes {
        // Check if total_sizes has minimum range upto set_size
        if size < SET_SIZES[0] || size < set_drive_count {
            return Err(Error::other(format!("Incorrect number of endpoints provided, size {size}")));
        }
    }

    let common_size = get_divisible_size(total_sizes);
    let mut set_counts = possible_set_counts(common_size);
    if set_counts.is_empty() {
        return Err(Error::other(format!(
            "Incorrect number of endpoints provided, number of drives {} is not divisible by any supported erasure set sizes {}",
            common_size, 0
        )));
    }

    // Returns possible set counts with symmetry.
    set_counts = possible_set_counts_with_symmetry(&set_counts, arg_patterns);
    if set_counts.is_empty() {
        return Err(Error::other("No symmetric distribution detected with input endpoints provided"));
    }

    let set_size = {
        if set_drive_count > 0 {
            let has_set_drive_count = set_counts.contains(&set_drive_count);

            if !has_set_drive_count {
                return Err(Error::other(format!(
                    "Invalid set drive count {}. Acceptable values for {:?} number drives are {:?}",
                    set_drive_count, common_size, &set_counts
                )));
            }
            set_drive_count
        } else {
            set_counts = possible_set_counts_with_symmetry(&set_counts, arg_patterns);
            if set_counts.is_empty() {
                return Err(Error::other(format!(
                    "No symmetric distribution detected with input endpoints , drives {} cannot be spread symmetrically by any supported erasure set sizes {:?}",
                    common_size, &set_counts
                )));
            }
            // Final set size with all the symmetry accounted for.
            common_set_drive_count(common_size, &set_counts)
        }
    };

    if !is_valid_set_size(set_size) {
        return Err(Error::other("Incorrect number of endpoints provided3"));
    }

    Ok(total_sizes
        .iter()
        .map(|&size| (0..(size / set_size)).map(|_| set_size).collect())
        .collect())
}

/// Return the total size for each argument patterns.
fn get_total_sizes(arg_patterns: &[ArgPattern]) -> Vec<usize> {
    arg_patterns.iter().map(|v| v.total_sizes()).collect()
}

#[cfg(test)]
mod test {
    use rustfs_utils::string::Pattern;

    use super::*;

    impl PartialEq for EndpointSet {
        fn eq(&self, other: &Self) -> bool {
            self._arg_patterns == other._arg_patterns && self.set_indexes == other.set_indexes
        }
    }

    #[test]
    fn test_get_divisible_size() {
        struct TestCase {
            total_sizes: Vec<usize>,
            result: usize,
        }

        let test_cases = [
            TestCase {
                total_sizes: vec![24, 32, 16],
                result: 8,
            },
            TestCase {
                total_sizes: vec![32, 8, 4],
                result: 4,
            },
            TestCase {
                total_sizes: vec![8, 8, 8],
                result: 8,
            },
            TestCase {
                total_sizes: vec![24],
                result: 24,
            },
        ];

        for (i, test_case) in test_cases.iter().enumerate() {
            let ret = get_divisible_size(&test_case.total_sizes);
            assert_eq!(ret, test_case.result, "Test{}: Expected {}, got {}", i + 1, test_case.result, ret);
        }
    }

    #[test]
    fn test_get_set_indexes() {
        #[derive(Default)]
        struct TestCase<'a> {
            num: usize,
            args: Vec<&'a str>,
            total_sizes: Vec<usize>,
            indexes: Vec<Vec<usize>>,
            success: bool,
        }

        let test_cases = [
            TestCase {
                num: 1,
                args: vec!["data{1...17}/export{1...52}"],
                total_sizes: vec![14144],
                ..Default::default()
            },
            TestCase {
                num: 2,
                args: vec!["data{1...3}"],
                total_sizes: vec![3],
                indexes: vec![vec![3]],
                success: true,
            },
            TestCase {
                num: 3,
                args: vec!["data/controller1/export{1...2}, data/controller2/export{1...4}, data/controller3/export{1...8}"],
                total_sizes: vec![2, 4, 8],
                indexes: vec![vec![2], vec![2, 2], vec![2, 2, 2, 2]],
                success: true,
            },
            TestCase {
                num: 4,
                args: vec!["data{1...27}"],
                total_sizes: vec![27],
                indexes: vec![vec![9, 9, 9]],
                success: true,
            },
            TestCase {
                num: 5,
                args: vec!["http://host{1...3}/data{1...180}"],
                total_sizes: vec![540],
                indexes: vec![vec![
                    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
                    15, 15, 15, 15, 15, 15, 15, 15, 15,
                ]],
                success: true,
            },
            TestCase {
                num: 6,
                args: vec!["http://host{1...2}.rack{1...4}/data{1...180}"],
                total_sizes: vec![1440],
                indexes: vec![vec![
                    16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
                    16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
                    16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
                    16, 16, 16, 16, 16, 16, 16, 16, 16,
                ]],
                success: true,
            },
            TestCase {
                num: 7,
                args: vec!["http://host{1...2}/data{1...180}"],
                total_sizes: vec![360],
                indexes: vec![vec![
                    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
                    12, 12, 12,
                ]],
                success: true,
            },
            TestCase {
                num: 8,
                args: vec!["data/controller1/export{1...4}, data/controller2/export{1...8}, data/controller3/export{1...12}"],
                total_sizes: vec![4, 8, 12],
                indexes: vec![vec![4], vec![4, 4], vec![4, 4, 4]],
                success: true,
            },
            TestCase {
                num: 9,
                args: vec!["data{1...64}"],
                total_sizes: vec![64],
                indexes: vec![vec![16, 16, 16, 16]],
                success: true,
            },
            TestCase {
                num: 10,
                args: vec!["data{1...24}"],
                total_sizes: vec![24],
                indexes: vec![vec![12, 12]],
                success: true,
            },
            TestCase {
                num: 11,
                args: vec!["data/controller{1...11}/export{1...8}"],
                total_sizes: vec![88],
                indexes: vec![vec![11, 11, 11, 11, 11, 11, 11, 11]],
                success: true,
            },
            TestCase {
                num: 12,
                args: vec!["data{1...4}"],
                total_sizes: vec![4],
                indexes: vec![vec![4]],
                success: true,
            },
            TestCase {
                num: 13,
                args: vec!["data/controller1/export{1...10}, data/controller2/export{1...10}, data/controller3/export{1...10}"],
                total_sizes: vec![10, 10, 10],
                indexes: vec![vec![10], vec![10], vec![10]],
                success: true,
            },
            TestCase {
                num: 14,
                args: vec!["data{1...16}/export{1...52}"],
                total_sizes: vec![832],
                indexes: vec![vec![
                    16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
                    16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
                ]],
                success: true,
            },
            TestCase {
                num: 15,
                args: vec!["https://node{1...3}.example.net/mnt/drive{1...8}"],
                total_sizes: vec![24],
                indexes: vec![vec![12, 12]],
                success: true,
            },
        ];

        for test_case in test_cases {
            let mut arg_patterns = Vec::new();
            for v in test_case.args.iter() {
                match find_ellipses_patterns(v) {
                    Ok(patterns) => {
                        arg_patterns.push(patterns);
                    }
                    Err(err) => {
                        panic!("Test{}: Unexpected failure {:?}", test_case.num, err);
                    }
                }
            }

            match get_set_indexes(test_case.args.as_slice(), test_case.total_sizes.as_slice(), 0, arg_patterns.as_slice()) {
                Ok(got_indexes) => {
                    if !test_case.success {
                        panic!("Test{}: Expected failure but passed instead", test_case.num);
                    }

                    assert_eq!(
                        test_case.indexes, got_indexes,
                        "Test{}: Expected {:?}, got {:?}",
                        test_case.num, test_case.indexes, got_indexes
                    )
                }
                Err(err) => {
                    if test_case.success {
                        panic!("Test{}: Expected success but failed instead {:?}", test_case.num, err);
                    }
                }
            }
        }
    }

    fn get_sequences(start: usize, number: usize, padding_len: usize) -> Vec<String> {
        let mut seq = Vec::new();
        for i in start..=number {
            if padding_len == 0 {
                seq.push(format!("{i}"));
            } else {
                seq.push(format!("{i:0padding_len$}"));
            }
        }
        seq
    }

    #[test]
    fn test_into_endpoint_set() {
        #[derive(Default)]
        struct TestCase<'a> {
            num: usize,
            arg: &'a str,
            es: EndpointSet,
            success: bool,
        }

        let test_cases = [
            // Tests invalid inputs.
            TestCase {
                num: 1,
                arg: "...",
                ..Default::default()
            },
            // No range specified.
            TestCase {
                num: 2,
                arg: "{...}",
                ..Default::default()
            },
            // Invalid range.
            TestCase {
                num: 3,
                arg: "http://rustfs{2...3}/export/set{1...0}",
                ..Default::default()
            },
            // Range cannot be smaller than 4 minimum.
            TestCase {
                num: 4,
                arg: "/export{1..2}",
                ..Default::default()
            },
            // Unsupported characters.
            TestCase {
                num: 5,
                arg: "/export/test{1...2O}",
                ..Default::default()
            },
            // Tests valid inputs.
            TestCase {
                num: 6,
                arg: "{1...27}",
                es: EndpointSet {
                    _arg_patterns: vec![ArgPattern::new(vec![Pattern {
                        seq: get_sequences(1, 27, 0),
                        ..Default::default()
                    }])],
                    set_indexes: vec![vec![9, 9, 9]],
                    ..Default::default()
                },
                success: true,
            },
            TestCase {
                num: 7,
                arg: "/export/set{1...64}",
                es: EndpointSet {
                    _arg_patterns: vec![ArgPattern::new(vec![Pattern {
                        seq: get_sequences(1, 64, 0),
                        prefix: "/export/set".to_owned(),
                        ..Default::default()
                    }])],
                    set_indexes: vec![vec![16, 16, 16, 16]],
                    ..Default::default()
                },
                success: true,
            },
            // Valid input for distributed setup.
            TestCase {
                num: 8,
                arg: "http://rustfs{2...3}/export/set{1...64}",
                es: EndpointSet {
                    _arg_patterns: vec![ArgPattern::new(vec![
                        Pattern {
                            seq: get_sequences(1, 64, 0),
                            ..Default::default()
                        },
                        Pattern {
                            seq: get_sequences(2, 3, 0),
                            prefix: "http://rustfs".to_owned(),
                            suffix: "/export/set".to_owned(),
                        },
                    ])],
                    set_indexes: vec![vec![16, 16, 16, 16, 16, 16, 16, 16]],
                    ..Default::default()
                },
                success: true,
            },
            // Supporting some advanced cases.
            TestCase {
                num: 9,
                arg: "http://rustfs{1...64}.mydomain.net/data",
                es: EndpointSet {
                    _arg_patterns: vec![ArgPattern::new(vec![Pattern {
                        seq: get_sequences(1, 64, 0),
                        prefix: "http://rustfs".to_owned(),
                        suffix: ".mydomain.net/data".to_owned(),
                    }])],
                    set_indexes: vec![vec![16, 16, 16, 16]],
                    ..Default::default()
                },
                success: true,
            },
            TestCase {
                num: 10,
                arg: "http://rack{1...4}.mydomain.rustfs{1...16}/data",
                es: EndpointSet {
                    _arg_patterns: vec![ArgPattern::new(vec![
                        Pattern {
                            seq: get_sequences(1, 16, 0),
                            suffix: "/data".to_owned(),
                            ..Default::default()
                        },
                        Pattern {
                            seq: get_sequences(1, 4, 0),
                            prefix: "http://rack".to_owned(),
                            suffix: ".mydomain.rustfs".to_owned(),
                        },
                    ])],
                    set_indexes: vec![vec![16, 16, 16, 16]],
                    ..Default::default()
                },
                success: true,
            },
            // Supporting kubernetes cases.
            TestCase {
                num: 11,
                arg: "http://rustfs{0...15}.mydomain.net/data{0...1}",
                es: EndpointSet {
                    _arg_patterns: vec![ArgPattern::new(vec![
                        Pattern {
                            seq: get_sequences(0, 1, 0),
                            ..Default::default()
                        },
                        Pattern {
                            seq: get_sequences(0, 15, 0),
                            prefix: "http://rustfs".to_owned(),
                            suffix: ".mydomain.net/data".to_owned(),
                        },
                    ])],
                    set_indexes: vec![vec![16, 16]],
                    ..Default::default()
                },
                success: true,
            },
            // No host regex, just disks.
            TestCase {
                num: 12,
                arg: "http://server1/data{1...32}",
                es: EndpointSet {
                    _arg_patterns: vec![ArgPattern::new(vec![Pattern {
                        seq: get_sequences(1, 32, 0),
                        prefix: "http://server1/data".to_owned(),
                        ..Default::default()
                    }])],
                    set_indexes: vec![vec![16, 16]],
                    ..Default::default()
                },
                success: true,
            },
            // No host regex, just disks with two position numerics.
            TestCase {
                num: 13,
                arg: "http://server1/data{01...32}",
                es: EndpointSet {
                    _arg_patterns: vec![ArgPattern::new(vec![Pattern {
                        seq: get_sequences(1, 32, 2),
                        prefix: "http://server1/data".to_owned(),
                        ..Default::default()
                    }])],
                    set_indexes: vec![vec![16, 16]],
                    ..Default::default()
                },
                success: true,
            },
            // More than 2 ellipses are supported as well.
            TestCase {
                num: 14,
                arg: "http://rustfs{2...3}/export/set{1...64}/test{1...2}",
                es: EndpointSet {
                    _arg_patterns: vec![ArgPattern::new(vec![
                        Pattern {
                            seq: get_sequences(1, 2, 0),
                            ..Default::default()
                        },
                        Pattern {
                            seq: get_sequences(1, 64, 0),
                            suffix: "/test".to_owned(),
                            ..Default::default()
                        },
                        Pattern {
                            seq: get_sequences(2, 3, 0),
                            prefix: "http://rustfs".to_owned(),
                            suffix: "/export/set".to_owned(),
                        },
                    ])],
                    set_indexes: vec![vec![16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16]],
                    ..Default::default()
                },
                success: true,
            },
            // More than an ellipse per argument for standalone setup.
            TestCase {
                num: 15,
                arg: "/export{1...10}/disk{1...10}",
                es: EndpointSet {
                    _arg_patterns: vec![ArgPattern::new(vec![
                        Pattern {
                            seq: get_sequences(1, 10, 0),
                            ..Default::default()
                        },
                        Pattern {
                            seq: get_sequences(1, 10, 0),
                            prefix: "/export".to_owned(),
                            suffix: "/disk".to_owned(),
                        },
                    ])],
                    set_indexes: vec![vec![10, 10, 10, 10, 10, 10, 10, 10, 10, 10]],
                    ..Default::default()
                },
                success: true,
            },
        ];

        for test_case in test_cases {
            match EndpointSet::from_volumes([test_case.arg].as_slice(), 0) {
                Ok(got_es) => {
                    if !test_case.success {
                        panic!("Test{}: Expected failure but passed instead", test_case.num);
                    }

                    assert_eq!(
                        test_case.es, got_es,
                        "Test{}: Expected {:?}, got {:?}",
                        test_case.num, test_case.es, got_es
                    )
                }
                Err(err) => {
                    if test_case.success {
                        panic!("Test{}: Expected success but failed instead {:?}", test_case.num, err);
                    }
                }
            }
        }
    }
}
