// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::os::raw::c_int;
use std::ptr::null_mut;

#[repr(C)]
#[derive(Clone)]
struct Timeval {
    pub tv_sec: i64,
    pub tv_usec: i64,
}

#[repr(C)]
#[derive(Clone)]
struct Itimerval {
    pub it_interval: Timeval,
    pub it_value: Timeval,
}

extern "C" {
    fn setitimer(which: c_int, new_value: *mut Itimerval, old_value: *mut Itimerval) -> c_int;
}

const ITIMER_PROF: c_int = 2;

pub struct Timer {}

impl Timer {
    pub fn new(frequency: c_int) -> Timer {
        let interval = 1e6 as i64 / i64::from(frequency);
        let it_interval = Timeval {
            tv_sec: interval / 1e6 as i64,
            tv_usec: interval % 1e6 as i64,
        };
        let it_value = it_interval.clone();

        unsafe {
            setitimer(
                ITIMER_PROF,
                &mut Itimerval {
                    it_interval,
                    it_value,
                },
                null_mut(),
            )
        };

        Timer {}
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let it_interval = Timeval {
            tv_sec: 0,
            tv_usec: 0,
        };
        let it_value = it_interval.clone();
        unsafe {
            setitimer(
                ITIMER_PROF,
                &mut Itimerval {
                    it_interval,
                    it_value,
                },
                null_mut(),
            )
        };
    }
}
