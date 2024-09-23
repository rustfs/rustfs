use crate::{disk::error::DiskError, error::Error};
use std::{collections::HashMap, fmt::Debug};

// pub type CheckErrorFn = fn(e: &Error) -> bool;

pub trait CheckErrorFn: Debug + Send + Sync + 'static {
    fn is(&self, e: &Error) -> bool;
}

#[derive(Debug, thiserror::Error)]
enum QuorumError {
    #[error("Read quorum not met")]
    Read,
    #[error("disk not found")]
    Write,
}

pub fn is_file_not_found(e: &Error) -> bool {
    DiskError::FileNotFound.is(e)
}

pub fn base_ignored_errs() -> Vec<Box<dyn CheckErrorFn>> {
    vec![
        Box::new(DiskError::DiskNotFound),
        Box::new(DiskError::FaultyDisk),
        Box::new(DiskError::FaultyRemoteDisk),
    ]
}

pub fn object_ignored_errs() -> Vec<Box<dyn CheckErrorFn>> {
    vec![Box::new(DiskError::FileNotFound)]
}

// 用于检查错误是否被忽略的函数
fn is_err_ignored(err: &Error, ignored_errs: &Vec<Box<dyn CheckErrorFn>>) -> bool {
    ignored_errs.iter().any(|ignored_err| ignored_err.is(err))
}

// 减少错误数量并返回出现次数最多的错误
fn reduce_errs(errs: &Vec<Option<Error>>, ignored_errs: &Vec<Box<dyn CheckErrorFn>>) -> (usize, Option<usize>) {
    let mut error_counts: HashMap<String, usize> = HashMap::new();
    let mut error_map: HashMap<String, usize> = HashMap::new(); // 存err位置
    let nil = "nil".to_string();
    for (i, operr) in errs.iter().enumerate() {
        if operr.is_none() {
            *error_counts.entry(nil.clone()).or_insert(0) += 1;
            let _ = *error_map.entry(nil.clone()).or_insert(i);
            continue;
        }

        let err = operr.as_ref().unwrap();

        if is_err_ignored(err, &ignored_errs) {
            continue;
        }

        let errstr = err.to_string();

        let _ = *error_map.entry(errstr.clone()).or_insert(i);
        *error_counts.entry(errstr.clone()).or_insert(0) += 1;
    }

    let mut max = 0;
    let mut max_err = nil.clone();
    for (&ref err, &count) in error_counts.iter() {
        if count > max || (count == max && *err == nil) {
            max = count;
            max_err = err.clone();
        }
    }

    if let Some(&c) = error_counts.get(&max_err) {
        if let Some(&err) = error_map.get(&max_err) {
            return (c, Some(err));
        }

        return (c, None);
    }

    (0, None)
}

// 根据quorum验证错误数量
fn reduce_quorum_errs(errs: &Vec<Option<Error>>, ignored_errs: &Vec<Box<dyn CheckErrorFn>>, quorum: usize) -> Option<usize> {
    let (max_count, max_err) = reduce_errs(errs, ignored_errs);
    if max_count >= quorum {
        max_err
    } else {
        None
    }
}

// 根据读quorum验证错误数量
pub fn reduce_read_quorum_errs(
    errs: &mut Vec<Option<Error>>,
    ignored_errs: &Vec<Box<dyn CheckErrorFn>>,
    read_quorum: usize,
) -> Result<usize, Error> {
    let idx = reduce_quorum_errs(errs, ignored_errs, read_quorum);
    if idx.is_none() {
        return Err(Error::new(QuorumError::Read));
    }

    Ok(idx.unwrap())
}

// 根据写quorum验证错误数量
pub fn reduce_write_quorum_errs(
    errs: &Vec<Option<Error>>,
    ignored_errs: &Vec<Box<dyn CheckErrorFn>>,
    write_quorum: usize,
) -> Result<usize, Error> {
    let idx = reduce_quorum_errs(errs, ignored_errs, write_quorum);

    if idx.is_none() {
        return Err(Error::new(QuorumError::Write));
    }

    Ok(idx.unwrap())
}
