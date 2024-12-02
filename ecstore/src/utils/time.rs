use std::time::Duration;

use tracing::info;

pub fn parse_duration(s: &str) -> Option<Duration> {
    if s.ends_with("ms") {
        if let Ok(s) = s.trim_end_matches("ms").parse::<u64>() {
            return Some(Duration::from_millis(s));
        }
    } else if s.ends_with("s") {
        if let Ok(s) = s.trim_end_matches('s').parse::<u64>() {
            return Some(Duration::from_secs(s));
        }
    } else if s.ends_with("m") {
        if let Ok(s) = s.trim_end_matches('m').parse::<u64>() {
            return Some(Duration::from_secs(s * 60));
        }
    } else if s.ends_with("h") {
        if let Ok(s) = s.trim_end_matches('h').parse::<u64>() {
            return Some(Duration::from_secs(s * 60 * 60));
        }
    }
    info!("can not parse duration, s: {}", s);
    None
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::parse_duration;

    #[test]
    fn test_parse_dur() {
        let s = String::from("3s");
        let dur = parse_duration(&s);
        println!("{:?}", dur);
        assert_eq!(Some(Duration::from_secs(3)), dur);

        let s = String::from("3ms");
        let dur = parse_duration(&s);
        println!("{:?}", dur);
        assert_eq!(Some(Duration::from_millis(3)), dur);

        let s = String::from("3m");
        let dur = parse_duration(&s);
        println!("{:?}", dur);
        assert_eq!(Some(Duration::from_secs(3 * 60)), dur);

        let s = String::from("3h");
        let dur = parse_duration(&s);
        println!("{:?}", dur);
        assert_eq!(Some(Duration::from_secs(3 * 60 * 60)), dur);
    }
}
