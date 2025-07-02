// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

pub fn parse_duration(s: &str) -> Result<Duration, String> {
    // Implement your own duration parsing logic here
    // For example, you could use the humantime crate or a custom parser
    humantime::parse_duration(s).map_err(|e| e.to_string())
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::parse_duration;

    #[test]
    fn test_parse_dur() {
        let s = String::from("3s");
        let dur = parse_duration(&s);
        println!("{dur:?}");
        assert_eq!(Ok(Duration::from_secs(3)), dur);

        let s = String::from("3ms");
        let dur = parse_duration(&s);
        println!("{dur:?}");
        assert_eq!(Ok(Duration::from_millis(3)), dur);

        let s = String::from("3m");
        let dur = parse_duration(&s);
        println!("{dur:?}");
        assert_eq!(Ok(Duration::from_secs(3 * 60)), dur);

        let s = String::from("3h");
        let dur = parse_duration(&s);
        println!("{dur:?}");
        assert_eq!(Ok(Duration::from_secs(3 * 60 * 60)), dur);
    }
}
