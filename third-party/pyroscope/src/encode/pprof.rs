use std::collections::HashMap;

use crate::backend::types::Report;
use crate::encode::gen::google::{Function, Label, Line, Location, Profile, Sample, ValueType};

struct PProfBuilder {
    profile: Profile,
    strings: HashMap<String, i64>,
    functions: HashMap<FunctionMirror, u64>,
    locations: HashMap<LocationMirror, u64>,
}

#[derive(Hash, PartialEq, Eq, Clone)]
pub struct LocationMirror {
    pub function_id: u64,
    pub line: i64,
}

#[derive(Hash, PartialEq, Eq, Clone)]
pub struct FunctionMirror {
    pub name: i64,
    pub filename: i64,
}

impl PProfBuilder {
    fn add_string(&mut self, s: &String) -> i64 {
        let v = self.strings.get(s);
        if let Some(v) = v {
            return *v;
        }
        assert_ne!(self.strings.len(), self.profile.string_table.len() + 1);
        let id: i64 = self.strings.len() as i64;
        self.strings.insert(s.to_owned(), id);
        self.profile.string_table.push(s.to_owned());
        id
    }

    fn add_function(&mut self, fm: FunctionMirror) -> u64 {
        let v = self.functions.get(&fm);
        if let Some(v) = v {
            return *v;
        }
        assert_ne!(self.functions.len(), self.profile.function.len() + 1);
        let id: u64 = self.functions.len() as u64 + 1;
        let f = Function {
            id,
            name: fm.name,
            system_name: 0,
            filename: fm.filename,
            start_line: 0,
        };
        self.functions.insert(fm, id);
        self.profile.function.push(f);
        id
    }

    fn add_location(&mut self, lm: LocationMirror) -> u64 {
        let v = self.locations.get(&lm);
        if let Some(v) = v {
            return *v;
        }
        assert_ne!(self.locations.len(), self.profile.location.len() + 1);
        let id: u64 = self.locations.len() as u64 + 1;
        let l = Location {
            id,
            mapping_id: 0,
            address: 0,
            line: vec![Line {
                function_id: lm.function_id,
                line: lm.line,
            }],
            is_folded: false,
        };
        self.locations.insert(lm, id);
        self.profile.location.push(l);
        id
    }
}

pub fn encode(
    reports: &Vec<Report>,
    sample_rate: u32,
    start_time_nanos: u64,
    duration_nanos: u64,
) -> Profile {
    let mut b = PProfBuilder {
        strings: HashMap::new(),
        functions: HashMap::new(),
        locations: HashMap::new(),
        profile: Profile {
            sample_type: vec![],
            sample: vec![],
            mapping: vec![],
            location: vec![],
            function: vec![],
            string_table: vec![],
            drop_frames: 0,
            keep_frames: 0,
            time_nanos: start_time_nanos as i64,
            duration_nanos: duration_nanos as i64,
            period_type: None,
            period: 0,
            comment: vec![],
            default_sample_type: 0,
        },
    };
    b.add_string(&"".to_string());
    {
        let cpu = b.add_string(&"cpu".to_string());
        let nanoseconds = b.add_string(&"nanoseconds".to_string());
        b.profile.sample_type.push(ValueType {
            r#type: cpu,
            unit: nanoseconds,
        });
        b.profile.period = 1_000_000_000 / sample_rate as i64;
        b.profile.period_type = Some(ValueType {
            r#type: cpu,
            unit: nanoseconds,
        });
    }
    for report in reports {
        for (stacktrace, value) in &report.data {
            let mut sample = Sample {
                location_id: vec![],
                value: vec![*value as i64 * b.profile.period],
                label: vec![],
            };
            for sf in &stacktrace.frames {
                let name = b.add_string(sf.name.as_ref().unwrap_or(&"".to_string()));
                let filename = b.add_string(sf.filename.as_ref().unwrap_or(&"".to_string()));
                let line = sf.line.unwrap_or(0) as i64;
                let function_id = b.add_function(FunctionMirror { name, filename });
                let location_id = b.add_location(LocationMirror { function_id, line });
                sample.location_id.push(location_id);
            }
            let mut labels = HashMap::new();
            for l in &stacktrace.metadata.tags {
                let k = b.add_string(&l.key);
                let v = b.add_string(&l.value);
                labels.insert(k, v);
            }
            for l in &report.metadata.tags {
                let k = b.add_string(&l.key);
                let v = b.add_string(&l.value);
                labels.insert(k, v);
            }
            for (k, v) in &labels {
                sample.label.push(Label {
                    key: *k,
                    str: *v,
                    num: 0,
                    num_unit: 0,
                })
            }
            b.profile.sample.push(sample);
        }
    }
    b.profile
}
