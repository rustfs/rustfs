use pyroscope::pyroscope::PyroscopeConfig;

#[test]
fn test_config_new() {
    let config = PyroscopeConfig::new("http://localhost:8080", "myapp", 100, "testspy", "1.2.3");
    assert_eq!(config.url, "http://localhost:8080");
    assert_eq!(config.application_name, "myapp");
    assert_eq!(config.sample_rate, 100u32);
    assert_eq!(config.tags.len(), 0);
}

#[test]
fn test_config_constructor_values() {
    let config = PyroscopeConfig::new("http://localhost:8080", "myapp", 10, "testspy", "1.2.3");
    assert_eq!(config.sample_rate, 10u32);
    assert_eq!(config.spy_name, "testspy");
    assert_eq!(config.spy_version, "1.2.3");
}

#[test]
fn test_config_tags_empty() {
    let config = PyroscopeConfig::new("http://localhost:8080", "myapp", 100, "testspy", "1.2.3");
    assert_eq!(config.tags.len(), 0);
}

#[test]
fn test_config_tags() {
    let config = PyroscopeConfig::new("http://localhost:8080", "myapp", 100, "testspy", "1.2.3")
        .tags([("tag", "value")].to_vec());
    assert_eq!(config.tags.len(), 1);
    assert_eq!(config.tags.get("tag"), Some(&"value".to_owned()));
}

#[test]
fn test_config_tags_multiple() {
    let config = PyroscopeConfig::new("http://localhost:8080", "myapp", 100, "testspy", "1.2.3")
        .tags([("tag1", "value1"), ("tag2", "value2")].to_vec());
    assert_eq!(config.tags.len(), 2);
    assert_eq!(config.tags.get("tag1"), Some(&"value1".to_owned()));
    assert_eq!(config.tags.get("tag2"), Some(&"value2".to_owned()));
}
