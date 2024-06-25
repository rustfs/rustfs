use std::collections::{HashMap, HashSet};

use anyhow::Error;
use serde::Deserialize;

use super::ellipses::*;

#[derive(Deserialize, Debug, Default)]
pub struct PoolDisksLayout {
    pub cmdline: String,
    pub layout: Vec<Vec<String>>,
}

#[derive(Deserialize, Debug, Default)]
pub struct DisksLayout {
    pub legacy: bool,
    pub pools: Vec<PoolDisksLayout>,
}

impl DisksLayout {
    pub fn new(args: Vec<String>) -> Result<DisksLayout, Error> {
        if args.is_empty() {
            return Err(Error::msg("Invalid argument"));
        }

        let mut ok = true;
        for arg in args.iter() {
            ok = ok && !has_ellipses(&vec![arg.to_string()])
        }

        // TODO: from env
        let set_drive_count: usize = 0;

        if ok {
            let set_args = get_all_sets(set_drive_count, &args)?;

            return Ok(DisksLayout {
                legacy: true,
                pools: vec![PoolDisksLayout {
                    layout: set_args,
                    cmdline: args.join(" "),
                }],
            });
        }

        let mut ret = DisksLayout {
            pools: Vec::new(),
            ..Default::default()
        };

        for arg in args.iter() {
            let varg = vec![arg.to_string()];

            if !has_ellipses(&varg) && args.len() > 1 {
                return Err(Error::msg("所有参数必须包含省略号以用于池扩展"));
            }

            let set_args = get_all_sets(set_drive_count, &varg)?;

            ret.pools.push(PoolDisksLayout {
                layout: set_args,
                cmdline: arg.clone(),
            })
        }

        Ok(ret)
    }
}

fn get_all_sets(set_drive_count: usize, args: &Vec<String>) -> Result<Vec<Vec<String>>, Error> {
    let set_args;
    if !has_ellipses(args) {
        let set_indexes: Vec<Vec<usize>>;
        if args.len() > 1 {
            let totalsizes = vec![args.len()];
            set_indexes = get_set_indexes(args, &totalsizes, set_drive_count, &Vec::new())?;
        } else {
            set_indexes = vec![vec![args.len()]];
        }

        let mut s = EndpointSet {
            endpoints: args.clone(),
            set_indexes,
            ..Default::default()
        };

        set_args = s.get();
    } else {
        let mut s = EndpointSet::new(args, set_drive_count)?;
        set_args = s.get();
    }

    let mut seen = HashSet::with_capacity(set_args.len());
    for args in set_args.iter() {
        for arg in args {
            if seen.contains(arg) {
                return Err(Error::msg(format!(
                    "Input args {} has duplicate ellipses",
                    arg
                )));
            }
            seen.insert(arg);
        }
    }

    Ok(set_args)
}

#[derive(Debug, Default)]
pub struct EndpointSet {
    pub arg_patterns: Vec<ArgPattern>,
    pub endpoints: Vec<String>,
    pub set_indexes: Vec<Vec<usize>>,
}

impl EndpointSet {
    pub fn new(args: &Vec<String>, set_div_count: usize) -> Result<EndpointSet, Error> {
        let mut arg_patterns = Vec::with_capacity(args.len());
        for arg in args.iter() {
            arg_patterns.push(find_ellipses_patterns(arg.as_str())?);
        }

        let totalsizes = get_total_sizes(&arg_patterns);
        let set_indexes = get_set_indexes(args, &totalsizes, set_div_count, &arg_patterns)?;

        Ok(EndpointSet {
            set_indexes,
            arg_patterns,
            ..Default::default()
        })
    }

    pub fn get(&mut self) -> Vec<Vec<String>> {
        let mut sets: Vec<Vec<String>> = Vec::new();
        let eps = self.get_endpoints();

        let mut start = 0;
        for sidx in self.set_indexes.iter() {
            for idx in sidx {
                let end = idx + start;
                sets.push(eps[start..end].to_vec());
                start = end;
            }
        }
        sets
    }

    fn get_endpoints(&mut self) -> Vec<String> {
        if !self.endpoints.is_empty() {
            return self.endpoints.clone();
        }

        let mut endpoints = Vec::new();
        for ap in self.arg_patterns.iter() {
            let aps = ap.expand();
            for bs in aps {
                endpoints.push(bs.join(""));
            }
        }

        self.endpoints = endpoints;

        self.endpoints.clone()
    }
}

// fn parse_endpoint_set(set_div_count: usize, args: &Vec<String>) -> Result<EndpointSet, Error> {
//     let mut arg_patterns = Vec::with_capacity(args.len());
//     for arg in args.iter() {
//         arg_patterns.push(find_ellipses_patterns(arg.as_str())?);
//     }

//     let totalsizes = get_total_sizes(&arg_patterns);
//     let set_indexes = get_set_indexes(args, &totalsizes, set_div_count, &arg_patterns)?;

//     Ok(EndpointSet {
//         set_indexes,
//         arg_patterns,
//         ..Default::default()
//     })
// }

static SET_SIZES: [usize; 15] = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

fn gcd(mut x: usize, mut y: usize) -> usize {
    while y != 0 {
        let t = y;
        y = x % y;
        x = t;
    }
    x
}

fn get_divisible_size(totalsizes: &Vec<usize>) -> usize {
    let mut ret = totalsizes[0];
    for s in totalsizes.iter() {
        ret = gcd(ret, *s)
    }
    ret
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

fn is_valid_set_size(count: usize) -> bool {
    &count >= SET_SIZES.first().unwrap() && &count <= SET_SIZES.last().unwrap()
}

fn common_set_drive_count(divisible_size: usize, set_counts: Vec<usize>) -> usize {
    // prefers set_counts to be sorted for optimal behavior.
    if &divisible_size < set_counts.last().unwrap_or(&0) {
        return divisible_size;
    }

    let mut prev_d = divisible_size / set_counts[0];
    let mut set_size = 0;
    for cnt in set_counts {
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

fn possible_set_counts_with_symmetry(
    set_counts: Vec<usize>,
    arg_patterns: &Vec<ArgPattern>,
) -> Vec<usize> {
    let mut new_set_counts: HashMap<usize, ()> = HashMap::new();

    for ss in set_counts {
        let mut symmetry = false;
        for arg_pattern in arg_patterns {
            for p in arg_pattern.inner.iter() {
                if p.seq.len() > ss {
                    symmetry = p.seq.len() % ss == 0;
                } else {
                    symmetry = ss % p.seq.len() == 0;
                }
            }
        }

        if !new_set_counts.contains_key(&ss) && (symmetry || arg_patterns.is_empty()) {
            new_set_counts.insert(ss, ());
        }
    }

    let mut set_counts: Vec<usize> = Vec::from_iter(new_set_counts.keys().cloned());
    set_counts.sort_unstable();

    set_counts
}

fn get_set_indexes(
    args: &Vec<String>,
    totalsizes: &Vec<usize>,
    set_div_count: usize,
    arg_patterns: &Vec<ArgPattern>,
) -> Result<Vec<Vec<usize>>, Error> {
    if args.is_empty() || totalsizes.is_empty() {
        return Err(Error::msg("Invalid argument"));
    }

    for size in totalsizes.iter() {
        if size.lt(&SET_SIZES[0]) || size < &set_div_count {
            return Err(Error::msg(format!(
                "Incorrect number of endpoints provided,size {}",
                size
            )));
        }
    }

    let common_size = get_divisible_size(totalsizes);
    let mut set_counts = possible_set_counts(common_size);
    if set_counts.is_empty() {
        return Err(Error::msg("Incorrect number of endpoints provided2"));
    }

    let set_size;

    if set_div_count > 0 {
        let mut found = false;
        for ss in set_counts {
            if ss == set_div_count {
                found = true
            }
        }

        if !found {
            return Err(Error::msg("Invalid set drive count."));
        }

        set_size = set_div_count
        // TODO globalCustomErasureDriveCount = true
    } else {
        set_counts = possible_set_counts_with_symmetry(set_counts, arg_patterns);

        if set_counts.is_empty() {
            return Err(Error::msg(
                "No symmetric distribution detected with input endpoints provided",
            ));
        }

        set_size = common_set_drive_count(common_size, set_counts);
    }

    if !is_valid_set_size(set_size) {
        return Err(Error::msg("Incorrect number of endpoints provided3"));
    }

    let mut set_indexs = Vec::with_capacity(totalsizes.len());

    for size in totalsizes.iter() {
        let mut sizes = Vec::with_capacity(size / set_size);
        for _ in 0..size / set_size {
            sizes.push(set_size);
        }

        set_indexs.push(sizes)
    }

    Ok(set_indexs)
}

fn get_total_sizes(arg_patterns: &Vec<ArgPattern>) -> Vec<usize> {
    let mut sizes = Vec::with_capacity(arg_patterns.len());
    for ap in arg_patterns {
        let mut size = 1;
        for p in ap.inner.iter() {
            size *= p.seq.len()
        }

        sizes.push(size)
    }
    sizes
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_disks_layout_from_env_args() {
        // let pattern = String::from("http://[2001:3984:3989::{001...002}]/disk{1...4}");
        // let pattern = String::from("/export{1...10}/disk{1...10}");
        let pattern = String::from("http://minio{1...2}:9000/mnt/disk{1...16}");

        let mut args = Vec::new();
        args.push(pattern);
        match DisksLayout::new(args) {
            Ok(set) => {
                for pool in set.pools {
                    println!("cmd: {:?}", pool.cmdline);

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
