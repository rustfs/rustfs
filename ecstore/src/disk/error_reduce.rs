use super::error::Error;

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

    // 首先统计 None 的数量（作为 nil 错误）
    let nil_count = errors.iter().filter(|e| e.is_none()).count();

    let err_counts = errors
        .iter()
        .filter_map(|e| e.as_ref()) // 只处理 Some 的错误
        .fold(std::collections::HashMap::new(), |mut acc, e| {
            if is_ignored_err(ignored_errs, e) {
                return acc;
            }
            *acc.entry(e.clone()).or_insert(0) += 1;
            acc
        });

    // 找到最高频率的非 nil 错误
    let (best_err, best_count) = err_counts
        .into_iter()
        .max_by(|(_, c1), (_, c2)| c1.cmp(c2))
        .unwrap_or((nil_error.clone(), 0));

    // 比较 nil 错误和最高频率的非 nil 错误, 优先选择 nil 错误
    if nil_count > best_count || (nil_count == best_count && nil_count > 0) {
        (nil_count, None)
    } else {
        (best_count, Some(best_err))
    }
}

pub fn is_ignored_err(ignored_errs: &[Error], err: &Error) -> bool {
    ignored_errs.iter().any(|e| e == err)
}

pub fn count_errs(errors: &[Option<Error>], err: &Error) -> usize {
    errors.iter().filter(|&e| e.as_ref() == Some(err)).count()
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
        let errors = vec![Some(e1.clone()), Some(e1.clone()), Some(e2.clone()), None];
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
        let ignored = vec![e2.clone()];
        let (count, err) = reduce_errs(&errors, &ignored);
        assert_eq!(count, 2);
        assert_eq!(err, Some(e1));
    }

    #[test]
    fn test_reduce_quorum_errs() {
        let e1 = err_io("a");
        let e2 = err_io("b");
        let errors = vec![Some(e1.clone()), Some(e1.clone()), Some(e2.clone()), None];
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
    fn test_reduce_errs_nil_tiebreak() {
        // Error::Nil and another error have the same count, should prefer Nil
        let e1 = err_io("a");
        let errors = vec![Some(e1.clone()), None, Some(e1.clone()), None]; // e1:2, Nil:2
        let ignored = vec![];
        let (count, err) = reduce_errs(&errors, &ignored);
        assert_eq!(count, 2);
        assert_eq!(err, None); // None means Error::Nil is preferred
    }
}
