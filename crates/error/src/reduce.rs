use crate::error::Error;

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
    let err_counts =
        errors
            .iter()
            .map(|e| e.as_ref().unwrap_or(&Error::Nil))
            .fold(std::collections::HashMap::new(), |mut acc, e| {
                if is_ignored_err(ignored_errs, e) {
                    return acc;
                }
                *acc.entry(e.clone()).or_insert(0) += 1;
                acc
            });

    let (err, max_count) = err_counts
        .into_iter()
        .max_by(|(e1, c1), (e2, c2)| {
            // Prefer Error::Nil if present in a tie
            let count_cmp = c1.cmp(c2);
            if count_cmp == std::cmp::Ordering::Equal {
                match (e1, e2) {
                    (Error::Nil, _) => std::cmp::Ordering::Greater,
                    (_, Error::Nil) => std::cmp::Ordering::Less,
                    _ => format!("{e1:?}").cmp(&format!("{e2:?}")),
                }
            } else {
                count_cmp
            }
        })
        .unwrap_or((Error::Nil, 0));

    (max_count, if err == Error::Nil { None } else { Some(err) })
}

pub fn is_ignored_err(ignored_errs: &[Error], err: &Error) -> bool {
    ignored_errs.iter().any(|e| e == err)
}

pub fn count_errs(errors: &[Option<Error>], err: Error) -> usize {
    errors
        .iter()
        .map(|e| if e.is_none() { &Error::Nil } else { e.as_ref().unwrap() })
        .filter(|&e| e == &err)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err_io(msg: &str) -> Error {
        Error::IoError(std::io::Error::other(msg))
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
        // quorum = 2, should return e1
        let res = reduce_quorum_errs(&errors, &ignored, 2, Error::ErasureReadQuorum);
        assert_eq!(res, Some(e1));
        // quorum = 3, should return quorum error
        let res = reduce_quorum_errs(&errors, &ignored, 3, Error::ErasureReadQuorum);
        assert_eq!(res, Some(Error::ErasureReadQuorum));
    }

    #[test]
    fn test_count_errs() {
        let e1 = err_io("a");
        let e2 = err_io("b");
        let errors = vec![Some(e1.clone()), Some(e2.clone()), Some(e1.clone()), None];
        assert_eq!(count_errs(&errors, e1.clone()), 2);
        assert_eq!(count_errs(&errors, e2.clone()), 1);
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
