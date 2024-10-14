use crate::{disk::error::DiskError, error::Error};
use std::{collections::HashMap, fmt::Debug};

// pub type CheckErrorFn = fn(e: &Error) -> bool;

pub trait CheckErrorFn: Debug + Send + Sync + 'static {
    fn is(&self, e: &Error) -> bool;
}

#[derive(Debug, thiserror::Error)]
pub enum QuorumError {
    #[error("Read quorum not met")]
    Read,
    #[error("disk not found")]
    Write,
}

pub fn base_ignored_errs() -> Vec<Box<dyn CheckErrorFn>> {
    vec![
        Box::new(DiskError::DiskNotFound),
        Box::new(DiskError::FaultyDisk),
        Box::new(DiskError::FaultyRemoteDisk),
    ]
}

// object_op_ignored_errs
pub fn object_op_ignored_errs() -> Vec<Box<dyn CheckErrorFn>> {
    let mut base = base_ignored_errs();

    let ext: Vec<Box<dyn CheckErrorFn>> = vec![
        // Box::new(DiskError::DiskNotFound),
        // Box::new(DiskError::FaultyDisk),
        // Box::new(DiskError::FaultyRemoteDisk),
        Box::new(DiskError::DiskAccessDenied),
        Box::new(DiskError::UnformattedDisk),
        Box::new(DiskError::DiskOngoingReq),
    ];

    base.extend(ext);
    base
}

// 用于检查错误是否被忽略的函数
fn is_err_ignored(err: &Error, ignored_errs: &[Box<dyn CheckErrorFn>]) -> bool {
    ignored_errs.iter().any(|ignored_err| ignored_err.is(err))
}

// 减少错误数量并返回出现次数最多的错误
fn reduce_errs(errs: &[Option<Error>], ignored_errs: &[Box<dyn CheckErrorFn>]) -> (usize, Option<Error>) {
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

        if is_err_ignored(err, ignored_errs) {
            continue;
        }

        let errstr = err.to_string();

        let _ = *error_map.entry(errstr.clone()).or_insert(i);
        *error_counts.entry(errstr.clone()).or_insert(0) += 1;
    }

    let mut max = 0;
    let mut max_err = nil.clone();
    for (err, &count) in error_counts.iter() {
        if count > max || (count == max && *err == nil) {
            max = count;
            max_err.clone_from(err);
        }
    }

    if let Some(&c) = error_counts.get(&max_err) {
        if let Some(&err_idx) = error_map.get(&max_err) {
            let err = errs[err_idx].clone();

            return (c, err);
        }

        return (c, None);
    }

    (0, None)
}

// 根据quorum验证错误数量
fn reduce_quorum_errs(
    errs: &[Option<Error>],
    ignored_errs: &[Box<dyn CheckErrorFn>],
    quorum: usize,
    quorum_err: QuorumError,
) -> Option<Error> {
    let (max_count, max_err) = reduce_errs(errs, ignored_errs);
    if max_count >= quorum {
        max_err
    } else {
        Some(Error::new(quorum_err))
    }
}

// 根据读quorum验证错误数量
// 返回最大错误数量的下标，或QuorumError
pub fn reduce_read_quorum_errs(
    errs: &[Option<Error>],
    ignored_errs: &[Box<dyn CheckErrorFn>],
    read_quorum: usize,
) -> Option<Error> {
    reduce_quorum_errs(errs, ignored_errs, read_quorum, QuorumError::Read)
}

// 根据写quorum验证错误数量
// 返回最大错误数量的下标，或QuorumError
pub fn reduce_write_quorum_errs(
    errs: &[Option<Error>],
    ignored_errs: &[Box<dyn CheckErrorFn>],
    write_quorum: usize,
) -> Option<Error> {
    reduce_quorum_errs(errs, ignored_errs, write_quorum, QuorumError::Write)
}
