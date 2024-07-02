use super::ellipses::*;
use anyhow::{Error, Result};
use serde::Deserialize;
use std::collections::HashSet;

/// Supported set sizes this is used to find the optimal
/// single set size.
const SET_SIZES: [usize; 15] = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

#[derive(Deserialize, Debug, Default)]
pub struct PoolDisksLayout {
    pub cmd_line: String,
    pub layout: Vec<Vec<String>>,
}

impl PoolDisksLayout {
    pub fn new(args: impl Into<String>, layout: Vec<Vec<String>>) -> Self {
        PoolDisksLayout {
            cmd_line: args.into(),
            layout,
        }
    }
}

#[derive(Deserialize, Debug, Default)]
pub struct DisksLayout {
    pub legacy: bool,
    pub pools: Vec<PoolDisksLayout>,
}

impl<T: AsRef<str>> TryFrom<&[T]> for DisksLayout {
    type Error = Error;

    fn try_from(args: &[T]) -> Result<Self, Self::Error> {
        if args.is_empty() {
            return Err(Error::msg("Invalid argument"));
        }

        let is_ellipses = args.iter().any(|v| has_ellipses(&[v]));

        // None of the args have ellipses use the old style.
        if !is_ellipses {
            let set_args = get_all_sets(is_ellipses, args)?;

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
                return Err(Error::msg("all args must have ellipses for pool expansion (Invalid arguments specified)"));
            }

            let set_args = get_all_sets(is_ellipses, &[arg])?;

            layout.push(PoolDisksLayout::new(arg.as_ref(), set_args));
        }

        Ok(DisksLayout {
            legacy: false,
            pools: layout,
        })
    }
}

/// parses all ellipses input arguments, expands them into
/// corresponding list of endpoints chunked evenly in accordance with a
/// specific set size.
///
/// For example: {1...64} is divided into 4 sets each of size 16.
/// This applies to even distributed setup syntax as well.
fn get_all_sets<T: AsRef<str>>(is_ellipses: bool, args: &[T]) -> Result<Vec<Vec<String>>> {
    let endpoint_set = if is_ellipses {
        EndpointSet::try_from(args)?
    } else {
        let set_indexes = if args.len() > 1 {
            get_set_indexes(args, &[args.len()], &[])?
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
                return Err(Error::msg(format!("Input args {} has duplicate ellipses", arg)));
            }
            unique_args.insert(arg);
        }
    }

    Ok(set_args)
}

/// represents parsed ellipses values, also provides
/// methods to get the sets of endpoints.
#[derive(Debug, Default)]
pub struct EndpointSet {
    pub arg_patterns: Vec<ArgPattern>,
    pub endpoints: Vec<String>,
    pub set_indexes: Vec<Vec<usize>>,
}

impl<T: AsRef<str>> TryFrom<&[T]> for EndpointSet {
    type Error = Error;

    fn try_from(args: &[T]) -> Result<Self, Self::Error> {
        let mut arg_patterns = Vec::with_capacity(args.len());
        for arg in args {
            arg_patterns.push(find_ellipses_patterns(arg.as_ref())?);
        }

        let total_sizes = get_total_sizes(&arg_patterns);
        let set_indexes = get_set_indexes(args, &total_sizes, &arg_patterns)?;

        let mut endpoints = Vec::new();
        for ap in arg_patterns.iter() {
            let aps = ap.expand();
            for bs in aps {
                endpoints.push(bs.join(""));
            }
        }

        Ok(EndpointSet {
            set_indexes,
            arg_patterns,
            endpoints,
        })
    }
}

impl EndpointSet {
    /// Create a new EndpointSet with the given endpoints and set indexes.
    pub fn new(endpoints: Vec<String>, set_indexes: Vec<Vec<usize>>) -> Self {
        Self {
            endpoints,
            set_indexes,
            ..Default::default()
        }
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

/// returns a greatest common divisor of all the ellipses sizes.
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
        if set_size % s == 0 {
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
        if divisible_size % cnt == 0 {
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
            for p in arg_pattern.inner.iter() {
                if p.seq.len() > ss {
                    symmetry = (p.seq.len() % ss) == 0;
                } else {
                    symmetry = (ss % p.seq.len()) == 0;
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
fn get_set_indexes<T: AsRef<str>>(args: &[T], total_sizes: &[usize], arg_patterns: &[ArgPattern]) -> Result<Vec<Vec<usize>>> {
    if args.is_empty() || total_sizes.is_empty() {
        return Err(Error::msg("Invalid argument"));
    }

    for &size in total_sizes {
        // Check if total_sizes has minimum range upto set_size
        if size < SET_SIZES[0] {
            return Err(Error::msg(format!("Incorrect number of endpoints provided, size {}", size)));
        }
    }

    let common_size = get_divisible_size(total_sizes);
    let mut set_counts = possible_set_counts(common_size);
    if set_counts.is_empty() {
        return Err(Error::msg(format!(
            "Incorrect number of endpoints provided, number of drives {} is not divisible by any supported erasure set sizes {}",
            common_size, 0
        )));
    }

    // TODO Add custom set drive count

    // Returns possible set counts with symmetry.
    set_counts = possible_set_counts_with_symmetry(&set_counts, arg_patterns);
    if set_counts.is_empty() {
        return Err(Error::msg("No symmetric distribution detected with input endpoints provided"));
    }

    // Final set size with all the symmetry accounted for.
    let set_size = common_set_drive_count(common_size, &set_counts);
    if !is_valid_set_size(set_size) {
        return Err(Error::msg("Incorrect number of endpoints provided3"));
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
    use super::*;

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
    fn test_parse_disks_layout_from_env_args() {
        // let pattern = String::from("http://[2001:3984:3989::{001...002}]/disk{1...4}");
        // let pattern = String::from("/export{1...10}/disk{1...10}");
        let pattern = String::from("http://minio{1...2}:9000/mnt/disk{1...16}");

        let mut args = Vec::new();
        args.push(pattern);
        match DisksLayout::try_from(args.as_slice()) {
            Ok(set) => {
                for pool in set.pools {
                    println!("cmd: {:?}", pool.cmd_line);

                    for (i, set) in pool.layout.iter().enumerate() {
                        for (j, v) in set.iter().enumerate() {
                            println!("{:?}.{}: {:?}", i, j, v);
                        }
                    }
                }
            }
            Err(err) => println!("{err:?}"),
        }
    }
}
