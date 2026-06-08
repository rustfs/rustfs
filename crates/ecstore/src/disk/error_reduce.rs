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

use crate::disk::error::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteQuorumFailureSummary {
    pub required: usize,
    pub achieved: usize,
    pub failed: usize,
    pub total: usize,
    pub offline_disks: usize,
    pub ignored_failures: usize,
    pub retryable_failures: usize,
    pub dominant_error: Option<Error>,
    pub dominant_error_label: &'static str,
}

pub static OBJECT_OP_IGNORED_ERRS: &[Error] = &[
    Error::DiskNotFound,
    Error::FaultyDisk,
    Error::FaultyRemoteDisk,
    Error::DiskAccessDenied,
    Error::DiskOngoingReq,
    Error::UnformattedDisk,
];

pub static BUCKET_OP_IGNORED_ERRS: &[Error] = &[
    Error::DiskNotFound,
    Error::FaultyDisk,
    Error::FaultyRemoteDisk,
    Error::DiskAccessDenied,
    Error::UnformattedDisk,
];

pub static BASE_IGNORED_ERRS: &[Error] = &[Error::DiskNotFound, Error::FaultyDisk, Error::FaultyRemoteDisk];

pub fn reduce_write_quorum_errs(errors: &[Option<Error>], ignored_errs: &[Error], quorun: usize) -> Option<Error> {
    reduce_quorum_errs(errors, ignored_errs, quorun, Error::ErasureWriteQuorum)
}

pub fn reduce_read_quorum_errs(errors: &[Option<Error>], ignored_errs: &[Error], quorun: usize) -> Option<Error> {
    reduce_quorum_errs(errors, ignored_errs, quorun, Error::ErasureReadQuorum)
}

pub fn reduce_quorum_errs(errors: &[Option<Error>], ignored_errs: &[Error], quorun: usize, quorun_err: Error) -> Option<Error> {
    let (max_count, err) = reduce_errs(errors, ignored_errs);
    if max_count >= quorun { err } else { Some(quorun_err) }
}

pub fn reduce_errs(errors: &[Option<Error>], ignored_errs: &[Error]) -> (usize, Option<Error>) {
    let nil_error = Error::other("nil".to_string());

    // First count the number of None values (treated as nil errors)
    let nil_count = errors.iter().filter(|e| e.is_none()).count();

    let err_counts = errors
        .iter()
        .filter_map(|e| e.as_ref()) // Only process errors stored in Some
        .fold(std::collections::HashMap::new(), |mut acc, e| {
            if is_ignored_err(ignored_errs, e) {
                return acc;
            }
            *acc.entry(e.clone()).or_insert(0) += 1;
            acc
        });

    // Find the most frequent non-nil error
    let (best_err, best_count) = err_counts
        .into_iter()
        .max_by(|(_, c1), (_, c2)| c1.cmp(c2))
        .unwrap_or((nil_error, 0));

    // Compare nil errors with the top non-nil error and prefer the nil error
    if nil_count > best_count || (nil_count == best_count && nil_count > 0) {
        (nil_count, None)
    } else {
        (best_count, Some(best_err))
    }
}

pub fn build_write_quorum_failure_summary(
    errors: &[Option<Error>],
    ignored_errs: &[Error],
    quorum: usize,
) -> WriteQuorumFailureSummary {
    let total = errors.len();
    let achieved = errors.iter().filter(|err| err.is_none()).count();
    let failed = total.saturating_sub(achieved);
    let offline_disks = count_errs(errors, &Error::DiskNotFound);
    let ignored_failures = errors
        .iter()
        .filter_map(|err| err.as_ref())
        .filter(|err| is_ignored_err(ignored_errs, err))
        .count();
    let retryable_failures = count_retryable_failures(errors);
    let (_, dominant_error) = reduce_errs(errors, ignored_errs);
    let dominant_error_label = dominant_error_label(errors, ignored_errs, dominant_error.as_ref());

    WriteQuorumFailureSummary {
        required: quorum,
        achieved,
        failed,
        total,
        offline_disks,
        ignored_failures,
        retryable_failures,
        dominant_error,
        dominant_error_label,
    }
}

fn dominant_error_label(errors: &[Option<Error>], ignored_errs: &[Error], dominant_error: Option<&Error>) -> &'static str {
    let Some(dominant_error) = dominant_error else {
        return "nil_dominated";
    };

    if dominant_error == &Error::DiskNotFound {
        return "disk_not_found";
    }
    if dominant_error == &Error::ShortWrite {
        return "short_write";
    }

    errors
        .iter()
        .filter_map(|err| err.as_ref())
        .find(|err| !is_ignored_err(ignored_errs, err) && *err == dominant_error)
        .and_then(Error::internode_http_error_kind)
        .map(|kind| kind.metric_label())
        .unwrap_or("other_error")
}

pub fn is_ignored_err(ignored_errs: &[Error], err: &Error) -> bool {
    ignored_errs.iter().any(|e| e == err)
}

pub fn count_errs(errors: &[Option<Error>], err: &Error) -> usize {
    errors.iter().filter(|&e| e.as_ref() == Some(err)).count()
}

pub fn count_retryable_failures(errors: &[Option<Error>]) -> usize {
    errors
        .iter()
        .filter_map(|err| err.as_ref())
        .filter(|err| err.is_retryable_internode_write_failure())
        .count()
}

pub fn is_all_buckets_not_found(errs: &[Option<Error>]) -> bool {
    for err in errs.iter() {
        if let Some(err) = err {
            if err == &Error::DiskNotFound || err == &Error::VolumeNotFound {
                continue;
            }

            return false;
        }
        return false;
    }

    !errs.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err_io(msg: &str) -> Error {
        Error::Io(std::io::Error::other(msg))
    }

    #[test]
    fn test_reduce_errs_basic() {
        let e1 = err_io("a");
        let e2 = err_io("b");
        let errors = vec![Some(e1.clone()), Some(e1.clone()), Some(e2), None];
        let ignored = vec![];
        let (count, err) = reduce_errs(&errors, &ignored);
        assert_eq!(count, 2);
        assert_eq!(err, Some(e1));
    }

    #[test]
    fn test_reduce_errs_ignored() {
        let e1 = err_io("a");
        let e2 = err_io("b");
        let errors = vec![Some(e1.clone()), Some(e2.clone()), Some(e1.clone()), Some(e2.clone()), None];
        let ignored = vec![e2];
        let (count, err) = reduce_errs(&errors, &ignored);
        assert_eq!(count, 2);
        assert_eq!(err, Some(e1));
    }

    #[test]
    fn test_reduce_quorum_errs() {
        let e1 = err_io("a");
        let e2 = err_io("b");
        let errors = vec![Some(e1.clone()), Some(e1.clone()), Some(e2), None];
        let ignored = vec![];
        let quorum_err = Error::FaultyDisk;
        // quorum = 2, should return e1
        let res = reduce_quorum_errs(&errors, &ignored, 2, quorum_err.clone());
        assert_eq!(res, Some(e1));
        // quorum = 3, should return quorum error
        let res = reduce_quorum_errs(&errors, &ignored, 3, quorum_err.clone());
        assert_eq!(res, Some(quorum_err));
    }

    #[test]
    fn test_count_errs() {
        let e1 = err_io("a");
        let e2 = err_io("b");
        let errors = vec![Some(e1.clone()), Some(e2.clone()), Some(e1.clone()), None];
        assert_eq!(count_errs(&errors, &e1), 2);
        assert_eq!(count_errs(&errors, &e2), 1);
    }

    #[test]
    fn test_is_ignored_err() {
        let e1 = err_io("a");
        let e2 = err_io("b");
        let ignored = vec![e1.clone()];
        assert!(is_ignored_err(&ignored, &e1));
        assert!(!is_ignored_err(&ignored, &e2));
    }

    #[test]
    fn test_build_write_quorum_failure_summary() {
        let retryable = Error::from(rustfs_rio::new_test_internode_http_io_error(
            rustfs_rio::InternodeHttpErrorKind::ConnectionReset,
        ));
        let non_retryable = err_io("other");
        let errors = vec![
            None,
            None,
            None,
            None,
            None,
            Some(retryable),
            Some(non_retryable),
            Some(Error::DiskNotFound),
        ];

        let summary = build_write_quorum_failure_summary(&errors, OBJECT_OP_IGNORED_ERRS, 6);
        assert_eq!(summary.required, 6);
        assert_eq!(summary.achieved, 5);
        assert_eq!(summary.failed, 3);
        assert_eq!(summary.total, 8);
        assert_eq!(summary.offline_disks, 1);
        assert_eq!(summary.ignored_failures, 1);
        assert_eq!(summary.retryable_failures, 1);
        assert_eq!(summary.dominant_error, None);
        assert_eq!(summary.dominant_error_label, "nil_dominated");
    }

    #[test]
    fn test_build_write_quorum_failure_summary_preserves_internode_label() {
        let retryable_a = Error::from(rustfs_rio::new_test_internode_http_io_error(
            rustfs_rio::InternodeHttpErrorKind::ConnectionReset,
        ));
        let retryable_b = Error::from(rustfs_rio::new_test_internode_http_io_error(
            rustfs_rio::InternodeHttpErrorKind::ConnectionReset,
        ));
        let errors = vec![Some(retryable_a), Some(retryable_b)];

        let summary = build_write_quorum_failure_summary(&errors, OBJECT_OP_IGNORED_ERRS, 2);

        assert_eq!(summary.retryable_failures, 2);
        assert_eq!(summary.dominant_error_label, "connection_reset");
    }

    #[test]
    fn test_reduce_errs_nil_tiebreak() {
        // Error::Nil and another error have the same count, should prefer Nil
        let e1 = err_io("a");
        let errors = vec![Some(e1.clone()), None, Some(e1), None]; // e1:2, Nil:2
        let ignored = vec![];
        let (count, err) = reduce_errs(&errors, &ignored);
        assert_eq!(count, 2);
        assert_eq!(err, None); // None means Error::Nil is preferred
    }
}
