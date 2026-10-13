use nix::{errno::Errno, unistd::read};
use std::mem::size_of;
use std::os::fd::{AsRawFd, OwnedFd};

#[derive(Default)]
pub(crate) struct Pipes {
    read_fd: Option<OwnedFd>,
    write_fd: Option<OwnedFd>,
}
#[inline]
#[cfg(any(target_os = "android", target_os = "linux"))]
fn create_pipe() -> nix::Result<(OwnedFd, OwnedFd)> {
    use nix::fcntl::OFlag;
    use nix::unistd::pipe2;

    pipe2(OFlag::O_CLOEXEC | OFlag::O_NONBLOCK)
}

#[inline]
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
fn create_pipe() -> nix::Result<(OwnedFd, OwnedFd)> {
    use nix::fcntl::{fcntl, FcntlArg, FdFlag, OFlag};
    use nix::unistd::pipe;

    fn set_flags(fd: &OwnedFd) -> nix::Result<()> {
        let mut flags = FdFlag::from_bits(fcntl(fd, FcntlArg::F_GETFD)?).unwrap();
        flags |= FdFlag::FD_CLOEXEC;
        fcntl(fd, FcntlArg::F_SETFD(flags))?;
        let mut flags = OFlag::from_bits(fcntl(fd, FcntlArg::F_GETFL)?).unwrap();
        flags |= OFlag::O_NONBLOCK;
        fcntl(fd, FcntlArg::F_SETFL(flags))?;
        Ok(())
    }

    let (read_fd, write_fd) = pipe()?;
    set_flags(&read_fd)?;
    set_flags(&write_fd)?;
    Ok((read_fd, write_fd))
}

fn open_pipe(pipes: &mut Pipes) -> nix::Result<()> {
    pipes.read_fd = None;
    pipes.write_fd = None;

    let (read_fd, write_fd) = create_pipe()?;

    pipes.read_fd = Some(read_fd);
    pipes.write_fd = Some(write_fd);

    Ok(())
}

// validate whether the address `addr` is readable through `write()` to a pipe
//
// if the second argument of `write(fd, buf, count)` is not a valid address, the
// `write()` will return an error the error number should be `EFAULT` in most
// cases, but we regard all errors (except EINTR) as a failure of validation
pub fn validate(pipes: &mut Pipes, addr: *const libc::c_void) -> bool {
    // Short-circuit null pointers without asking the kernel to validate them.
    if addr.is_null() {
        return false;
    }

    const CHECK_LENGTH: usize = 2 * size_of::<*const libc::c_void>() / size_of::<u8>();

    // read data in the pipe
    let valid_read = loop {
        match &pipes.read_fd {
            None => break false,
            Some(read_fd) => {
                let mut buf = [0u8; CHECK_LENGTH];

                match read(read_fd, &mut buf) {
                    Ok(bytes) => break bytes > 0,
                    Err(_err @ Errno::EINTR) => continue,
                    Err(_err @ Errno::EAGAIN) => break true,
                    Err(_) => break false,
                }
            }
        }
    };

    if !valid_read && open_pipe(pipes).is_err() {
        return false;
    }

    let Some(write_fd) = &pipes.write_fd else {
        return false; // impossible
    };
    loop {
        let res = unsafe { libc::write(write_fd.as_raw_fd(), addr, CHECK_LENGTH) };
        let res = if res == -1 {
            Err(Errno::last())
        } else {
            Ok(res)
        };
        match res {
            Ok(bytes) => break bytes > 0,
            Err(_err @ Errno::EINTR) => continue,
            Err(_) => break false,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use claims::assert_some;

    #[test]
    fn validate_stack() {
        let mut p = Pipes::default();
        let i = 0;

        assert!(validate(&mut p, &i as *const _ as *const libc::c_void));
        assert_some!(p.read_fd);
        assert_some!(p.write_fd);
    }

    #[test]
    fn validate_heap() {
        let mut p = Pipes::default();
        let vec = vec![0; 1000];

        for i in vec.iter() {
            assert!(validate(&mut p, i as *const _ as *const libc::c_void));
        }
        assert_some!(p.read_fd);
        assert_some!(p.write_fd);
    }

    #[test]
    fn failed_validate() {
        let mut p = Pipes::default();
        assert!(!validate(&mut p, std::ptr::null::<libc::c_void>()));
        assert!(!validate(&mut p, -1_i32 as usize as *const libc::c_void));
        assert_some!(p.read_fd);
        assert_some!(p.write_fd);
    }
}
