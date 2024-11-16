use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Default)]
pub struct AccElem {
    pub total: u64,
    pub size: u64,
    pub n: u64,
}

impl AccElem {
    pub fn add(&mut self, dur: &Duration) {
        let dur = dur.as_secs();
        self.total += dur;
        self.n += 1;
    }

    pub fn merge(&mut self, b: &AccElem) {
        self.n += b.n;
        self.total += b.total;
        self.size += b.size;
    }

    pub fn avg(&self) -> Duration {
        if self.n >= 1 && self.total > 0 {
            return Duration::from_secs(self.total / self.n);
        }
        Duration::from_secs(0)
    }
}

#[derive(Clone)]
pub struct LastMinuteLatency {
    pub totals: Vec<AccElem>,
    pub last_sec: u64,
}

impl Default for LastMinuteLatency {
    fn default() -> Self {
        Self {
            totals: vec![AccElem::default(); 60],
            last_sec: Default::default(),
        }
    }
}

impl LastMinuteLatency {
    pub fn merge(&mut self, o: &mut LastMinuteLatency) -> LastMinuteLatency {
        let mut merged = LastMinuteLatency::default();
        if self.last_sec > o.last_sec {
            o.forward_to(self.last_sec);
            merged.last_sec = self.last_sec;
        } else {
            self.forward_to(o.last_sec);
            merged.last_sec = o.last_sec;
        }

        for i in 0..merged.totals.len() {
            merged.totals[i] = AccElem {
                total: self.totals[i].total + o.totals[i].total,
                n: self.totals[i].n + o.totals[i].n,
                size: self.totals[i].size + o.totals[i].size,
            }
        }
        merged
    }

    pub fn add(&mut self, t: &Duration) {
        let sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        self.forward_to(sec);
        let win_idx = sec % 60;
        self.totals[win_idx as usize].add(t);
        self.last_sec = sec;
    }

    pub fn add_all(&mut self, sec: u64, a: &AccElem) {
        self.forward_to(sec);
        let win_idx = sec % 60;
        self.totals[win_idx as usize].merge(a);
        self.last_sec = sec;
    }

    pub fn get_total(&mut self) -> AccElem {
        let mut res = AccElem::default();
        let sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        self.forward_to(sec);
        for elem in self.totals.iter() {
            res.merge(elem);
        }
        res
    }

    pub fn forward_to(&mut self, t: u64) {
        if self.last_sec >= t {
            return;
        }
        if t - self.last_sec >= 60 {
            self.totals = vec![AccElem::default(); 60];
            return;
        }
        while self.last_sec != t {
            let idx = (self.last_sec + 1) % 60;
            self.totals[idx as usize] = AccElem::default();
            self.last_sec += 1;
        }
    }
}
