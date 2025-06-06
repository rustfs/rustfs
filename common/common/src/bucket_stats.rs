use std::{collections::HashMap, i64, u64};

use crate::last_minute::{self};
pub struct ReplicationLatency {
    // 单个和多部分 PUT 请求的延迟
    upload_histogram: last_minute::LastMinuteHistogram,
}

impl ReplicationLatency {
    // 合并两个 ReplicationLatency
    pub fn merge(&mut self, other: &mut ReplicationLatency) -> &ReplicationLatency {
        self.upload_histogram.merge(&other.upload_histogram);
        return self;
    }

    // 获取上传延迟（按对象大小区间分类）
    pub fn get_upload_latency(&mut self) -> HashMap<String, u64> {
        let mut ret = HashMap::new();
        let avg = self.upload_histogram.get_avg_data();
        for (i, v) in avg.iter().enumerate() {
            let avg_duration = v.avg();
            ret.insert(self.size_tag_to_string(i), avg_duration.as_millis() as u64);
        }
        ret
    }
    pub fn update(&mut self, size: i64, during: std::time::Duration) {
        self.upload_histogram.add(size, during);
    }

    // 模拟从 size tag 到字符串的转换
    fn size_tag_to_string(&self, tag: usize) -> String {
        match tag {
            0 => String::from("Size < 1 KiB"),
            1 => String::from("Size < 1 MiB"),
            2 => String::from("Size < 10 MiB"),
            3 => String::from("Size < 100 MiB"),
            4 => String::from("Size < 1 GiB"),
            _ => String::from("Size > 1 GiB"),
        }
    }
}

// #[derive(Debug, Clone, Default)]
// pub struct ReplicationLastMinute {
//     pub last_minute: LastMinuteLatency,
// }

// impl ReplicationLastMinute {
//     pub fn merge(&mut self, other: ReplicationLastMinute) -> ReplicationLastMinute {
//         let mut nl = ReplicationLastMinute::default();
//         nl.last_minute = self.last_minute.merge(&mut other.last_minute);
//         nl
//     }

//     pub fn add_size(&mut self, n: i64) {
//         let t = SystemTime::now()
//             .duration_since(UNIX_EPOCH)
//             .expect("Time went backwards")
//             .as_secs();
//         self.last_minute.add_all(t - 1, &AccElem { total: t - 1, size: n as u64, n: 1 });
//     }

//     pub fn get_total(&self) -> AccElem {
//         self.last_minute.get_total()
//     }
// }

// impl fmt::Display for ReplicationLastMinute {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         let t = self.last_minute.get_total();
//         write!(f, "ReplicationLastMinute sz= {}, n= {}, dur= {}", t.size, t.n, t.total)
//     }
// }
