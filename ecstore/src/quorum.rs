use crate::{disk::error::DiskError, error::Error};
use std::collections::HashMap;

type CheckErrorFn = fn(e: &Error) -> bool;

#[derive(Debug, thiserror::Error)]
enum QuorumError {
    #[error("Read quorum not met")]
    Read,
    #[error("disk not found")]
    Write,
}

fn is_file_not_found(e: &Error) -> bool {
    DiskError::FileNotFound.is(e)
}

// 用于检查错误是否被忽略的函数
fn is_err_ignored(err: &Error, ignored_errs: &[CheckErrorFn]) -> bool {
    ignored_errs.iter().any(|&ignored_err| ignored_err(err))
}

// 减少错误数量并返回出现次数最多的错误
fn reduce_errs(errs: &Vec<Option<Error>>, ignored_errs: &[CheckErrorFn]) -> (usize, Option<Error>) {
    let mut error_counts: HashMap<String, usize> = HashMap::new();
    let mut error_map: HashMap<String, &Error> = HashMap::new();
    let nil = "nil".to_string();
    for operr in errs.iter() {
        if operr.is_none() {
            *error_counts.entry(nil.clone()).or_insert(0) += 1;
            continue;
        }

        let err = operr.as_ref().unwrap();

        if is_err_ignored(err, &ignored_errs) {
            continue;
        }

        let errstr = err.to_string();

        *error_map.entry(errstr.clone()).or_insert(err);
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

    if let Some(c) = error_counts.get(&max_err) {
        if let Some(&err) = error_map.get(&max_err) {
            // return (*c, Some(err.clone()));
            return (*c, None);
        }

        return (*c, None);
    }

    (0, None)
}

// 根据quorum验证错误数量
fn reduce_quorum_errs<'a>(
    errs: &'a Vec<Option<Error>>,
    ignored_errs: &[CheckErrorFn],
    quorum: usize,
    quorum_err: Error,
) -> Option<Error> {
    let (max_count, max_err) = reduce_errs(errs, ignored_errs);
    if max_count >= quorum {
        max_err
    } else {
        Some(quorum_err)
    }
}

// 根据读quorum验证错误数量
fn reduce_read_quorum_errs(errs: &Vec<Option<Error>>, ignored_errs: &[CheckErrorFn], read_quorum: usize) -> Option<Error> {
    reduce_quorum_errs(errs, ignored_errs, read_quorum, Error::new(QuorumError::Read))
}

// 根据写quorum验证错误数量
fn reduce_write_quorum_errs(errs: &Vec<Option<Error>>, ignored_errs: &[CheckErrorFn], write_quorum: usize) -> Option<Error> {
    reduce_quorum_errs(errs, ignored_errs, write_quorum, Error::new(QuorumError::Write))
}
