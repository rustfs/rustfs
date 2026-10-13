use assert_matches::assert_matches;
use pyroscope::timer::{Timer, TimerSignal};
use std::time::Duration;

#[test]
fn test_timer() {
    // Initialize Timer
    let mut timer = Timer::initialize(Duration::from_secs(10)).unwrap();

    // Attach a listener
    let (tx, rx) = std::sync::mpsc::channel();
    timer.attach_listener(tx).unwrap();

    // Wait for event (should arrive in 10s)
    let planned = rx.recv().unwrap();
    assert_matches!(planned, TimerSignal::NextSnapshot(planned) => {
        // Get current time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Check that recv and now are within 10s of each other
        assert!(planned - now < 10);

        // Check that recv is divisible by 10
        assert!(planned % 10 == 0);
    })
}
