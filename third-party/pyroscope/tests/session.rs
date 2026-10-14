use claims::assert_ok;
use pyroscope::{
    backend::{Report, ReportBatch, ReportData},
    pyroscope::PyroscopeConfig,
    session::{Session, SessionManager, SessionSignal},
};
use std::collections::HashMap;

#[test]
fn test_session_manager_new() {
    let session_manager = SessionManager::new().unwrap();
    assert!(session_manager.handle.is_some());
}

#[test]
fn test_session_manager_push_kill() {
    let session_manager = SessionManager::new().unwrap();
    session_manager.push(SessionSignal::Kill).unwrap();
    assert_ok!(session_manager.handle.unwrap().join().unwrap());
}

#[test]
fn test_session_new() {
    let config = PyroscopeConfig {
        url: "http://localhost:8080".to_string(),
        application_name: "test".to_string(),
        tags: HashMap::new(),
        sample_rate: 100u32,
        spy_name: "test-rs".to_string(),
        ..Default::default()
    };

    let batch = ReportBatch {
        profile_type: "process_cpu".into(),
        data: ReportData::Reports(vec![Report::new(HashMap::new())]),
    };

    let session = Session::new(1950, config, batch).unwrap();

    assert_eq!(session.from, 1940);
    assert_eq!(session.until, 1950);
}

#[test]
fn test_session_send_error() {
    let config = PyroscopeConfig {
        url: "http://invalid_url".to_string(),
        application_name: "test".to_string(),
        tags: HashMap::new(),
        sample_rate: 100u32,
        spy_name: "test-rs".to_string(),
        ..Default::default()
    };

    let batch = ReportBatch {
        profile_type: "process_cpu".into(),
        data: ReportData::Reports(vec![Report::new(HashMap::new())]),
    };

    let _session = Session::new(1950, config, batch).unwrap();
}
