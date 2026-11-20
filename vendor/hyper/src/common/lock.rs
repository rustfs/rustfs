use std::sync::LockResult;

pub(crate) trait LockResultExt<T> {
    fn panic_if_poisoned(self) -> T;
}

#[allow(clippy::panic)]
impl<T> LockResultExt<T> for LockResult<T> {
    #[track_caller]
    fn panic_if_poisoned(self) -> T {
        match self {
            Ok(inner) => inner,
            Err(err) => panic!("lock poisoned by panic: {err}"),
        }
    }
}
