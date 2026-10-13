#[cfg(target_os = "linux")]
mod tests {
    use claims::assert_ok;
    use pyroscope::timer::epoll::{
        epoll_create1, epoll_ctl, epoll_wait, timerfd_create, timerfd_settime,
    };

    #[test]
    fn test_timerfd_create() {
        let timer_fd = timerfd_create(libc::CLOCK_REALTIME, libc::TFD_NONBLOCK).unwrap();
        assert!(timer_fd > 0);
    }

    #[test]
    fn test_timerfd_settime() {
        let mut new_value = libc::itimerspec {
            it_interval: libc::timespec {
                tv_sec: 10,
                tv_nsec: 0,
            },
            it_value: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
        };

        let mut old_value = libc::itimerspec {
            it_interval: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            it_value: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
        };

        let timer_fd = timerfd_create(libc::CLOCK_REALTIME, libc::TFD_NONBLOCK).unwrap();
        assert_ok!(timerfd_settime(
            timer_fd,
            libc::TFD_TIMER_ABSTIME,
            &mut new_value,
            &mut old_value,
        ));
    }

    #[test]
    fn test_epoll_create1() {
        let epoll_fd = epoll_create1(0).unwrap();
        assert!(epoll_fd > 0);
    }

    #[test]
    fn test_epoll_ctl() {
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: 1,
        };

        let epoll_fd = epoll_create1(0).unwrap();
        let timer_fd = timerfd_create(libc::CLOCK_REALTIME, libc::TFD_NONBLOCK).unwrap();
        assert_ok!(epoll_ctl(
            epoll_fd,
            libc::EPOLL_CTL_ADD,
            timer_fd,
            &mut event
        ));
    }

    #[test]
    fn test_epoll_wait() {
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: 1,
        };

        let epoll_fd = epoll_create1(0).unwrap();
        let timer_fd = timerfd_create(libc::CLOCK_REALTIME, libc::TFD_NONBLOCK).unwrap();
        epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, timer_fd, &mut event).unwrap();

        let mut events = vec![libc::epoll_event { events: 0, u64: 0 }];

        // Expire in 1ms
        assert_ok!(unsafe { epoll_wait(epoll_fd, events.as_mut_ptr(), 1, 1) });
    }
}
