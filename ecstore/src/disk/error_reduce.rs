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
    if max_count >= quorun {
        err
    } else {
        Some(quorun_err)
    }
}

pub fn reduce_errs(errors: &[Option<Error>], ignored_errs: &[Error]) -> (usize, Option<Error>) {
    let nil_error = Error::other("nil".to_string());
    let err_counts =
        errors
            .iter()
            .map(|e| e.as_ref().unwrap_or(&nil_error).clone())
            .fold(std::collections::HashMap::new(), |mut acc, e| {
                if is_ignored_err(ignored_errs, &e) {
                    return acc;
                }
                *acc.entry(e).or_insert(0) += 1;
                acc
            });

    let (err, max_count) = err_counts
        .into_iter()
        .max_by(|(e1, c1), (e2, c2)| {
            // Prefer Error::Nil if present in a tie
            let count_cmp = c1.cmp(c2);
            if count_cmp == std::cmp::Ordering::Equal {
                match (e1.to_string().as_str(), e2.to_string().as_str()) {
                    ("nil", _) => std::cmp::Ordering::Greater,
                    (_, "nil") => std::cmp::Ordering::Less,
                    (a, b) => a.cmp(&b),
                }
            } else {
                count_cmp
            }
        })
        .unwrap_or((nil_error.clone(), 0));

    (max_count, if err == nil_error { None } else { Some(err) })
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
        let e2 = err_io("b");
        let errors = vec![Some(e1.clone()), Some(e2.clone()), None, Some(e1.clone()), None]; // e1:1, Nil:1
        let ignored = vec![];
        let (count, err) = reduce_errs(&errors, &ignored);
        assert_eq!(count, 2);
        assert_eq!(err, None); // None means Error::Nil is preferred
    }
}
