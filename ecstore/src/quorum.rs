use crate::{disk::error::DiskError, error::clone_err};
use common::error::Error;
use std::{collections::HashMap, fmt::Debug};
// pub type CheckErrorFn = fn(e: &Error) -> bool;

pub trait CheckErrorFn: Debug + Send + Sync + 'static {
    fn is(&self, e: &Error) -> bool;
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum QuorumError {
    #[error("Read quorum not met")]
    Read,
    #[error("disk not found")]
    Write,
}

impl QuorumError {
    pub fn to_u32(&self) -> u32 {
        match self {
            QuorumError::Read => 0x01,
            QuorumError::Write => 0x02,
        }
    }

    pub fn from_u32(error: u32) -> Option<Self> {
        match error {
            0x01 => Some(QuorumError::Read),
            0x02 => Some(QuorumError::Write),
            _ => None,
        }
    }
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

// bucket_op_ignored_errs
pub fn bucket_op_ignored_errs() -> Vec<Box<dyn CheckErrorFn>> {
    let mut base = base_ignored_errs();

    let ext: Vec<Box<dyn CheckErrorFn>> = vec![Box::new(DiskError::DiskAccessDenied), Box::new(DiskError::UnformattedDisk)];

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
        if let Some(err) = operr {
            if is_err_ignored(err, ignored_errs) {
                continue;
            }

            let errstr = err.inner_string();

            let _ = *error_map.entry(errstr.clone()).or_insert(i);
            *error_counts.entry(errstr.clone()).or_insert(0) += 1;
        } else {
            *error_counts.entry(nil.clone()).or_insert(0) += 1;
            let _ = *error_map.entry(nil.clone()).or_insert(i);
            continue;
        }

        // let err = operr.as_ref().unwrap();

        // let errstr = err.to_string();

        // let _ = *error_map.entry(errstr.clone()).or_insert(i);
        // *error_counts.entry(errstr.clone()).or_insert(0) += 1;
    }

    let mut max = 0;
    let mut max_err = nil.clone();
    for (err, &count) in error_counts.iter() {
        if count > max || (count == max && *err == nil) {
            max = count;
            max_err.clone_from(err);
        }
    }

    if let Some(&err_idx) = error_map.get(&max_err) {
        let err = errs[err_idx].as_ref().map(clone_err);
        (max, err)
    } else if max_err == nil {
        (max, None)
    } else {
        (0, None)
    }
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
#[tracing::instrument(level = "info", skip_all)]
pub fn reduce_write_quorum_errs(
    errs: &[Option<Error>],
    ignored_errs: &[Box<dyn CheckErrorFn>],
    write_quorum: usize,
) -> Option<Error> {
    reduce_quorum_errs(errs, ignored_errs, write_quorum, QuorumError::Write)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct MockErrorChecker {
        target_error: String,
    }

    impl CheckErrorFn for MockErrorChecker {
        fn is(&self, e: &Error) -> bool {
            e.inner_string() == self.target_error
        }
    }

    fn mock_error(message: &str) -> Error {
        Error::msg(message.to_string())
    }

    #[test]
    fn test_reduce_errs_with_no_errors() {
        let errs: Vec<Option<Error>> = vec![];
        let ignored_errs: Vec<Box<dyn CheckErrorFn>> = vec![];

        let (count, err) = reduce_errs(&errs, &ignored_errs);

        assert_eq!(count, 0);
        assert!(err.is_none());
    }

    #[test]
    fn test_reduce_errs_with_ignored_errors() {
        let errs = vec![Some(mock_error("ignored_error")), Some(mock_error("ignored_error"))];
        let ignored_errs: Vec<Box<dyn CheckErrorFn>> = vec![Box::new(MockErrorChecker {
            target_error: "ignored_error".to_string(),
        })];

        let (count, err) = reduce_errs(&errs, &ignored_errs);

        assert_eq!(count, 0);
        assert!(err.is_none());
    }

    #[test]
    fn test_reduce_errs_with_mixed_errors() {
        let errs = vec![
            Some(Error::new(DiskError::FileNotFound)),
            Some(Error::new(DiskError::FileNotFound)),
            Some(Error::new(DiskError::FileNotFound)),
            Some(Error::new(DiskError::FileNotFound)),
            Some(Error::new(DiskError::FileNotFound)),
            Some(Error::new(DiskError::FileNotFound)),
            Some(Error::new(DiskError::FileNotFound)),
            Some(Error::new(DiskError::FileNotFound)),
            Some(Error::new(DiskError::FileNotFound)),
        ];
        let ignored_errs: Vec<Box<dyn CheckErrorFn>> = vec![Box::new(MockErrorChecker {
            target_error: "error2".to_string(),
        })];

        let (count, err) = reduce_errs(&errs, &ignored_errs);
        println!("count: {}, err: {:?}", count, err);
        assert_eq!(count, 9);
        assert_eq!(err.unwrap().to_string(), DiskError::FileNotFound.to_string());
    }

    #[test]
    fn test_reduce_errs_with_nil_errors() {
        let errs = vec![None, Some(mock_error("error1")), None];
        let ignored_errs: Vec<Box<dyn CheckErrorFn>> = vec![];

        let (count, err) = reduce_errs(&errs, &ignored_errs);

        assert_eq!(count, 2);
        assert!(err.is_none());
    }

    #[test]
    fn test_reduce_read_quorum_errs() {
        let errs = vec![
            Some(mock_error("error1")),
            Some(mock_error("error1")),
            Some(mock_error("error2")),
            None,
            None,
        ];
        let ignored_errs: Vec<Box<dyn CheckErrorFn>> = vec![];
        let read_quorum = 2;

        let result = reduce_read_quorum_errs(&errs, &ignored_errs, read_quorum);

        assert!(result.is_none());
    }

    #[test]
    fn test_reduce_write_quorum_errs_with_quorum_error() {
        let errs = vec![
            Some(mock_error("error1")),
            Some(mock_error("error2")),
            Some(mock_error("error2")),
        ];
        let ignored_errs: Vec<Box<dyn CheckErrorFn>> = vec![];
        let write_quorum = 3;

        let result = reduce_write_quorum_errs(&errs, &ignored_errs, write_quorum);

        assert!(result.is_some());
        assert_eq!(result.unwrap().to_string(), QuorumError::Write.to_string());
    }
}
