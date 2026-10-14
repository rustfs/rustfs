#[cfg(test)]
use crate::backend::{
    BackendConfig, Report, StackBuffer, StackFrame, StackTrace, Tag, ThreadTag, ThreadTagsSet,
};
#[cfg(test)]
use std::collections::{HashMap, HashSet};

#[test]
fn test_stack_frame_display() {
    let frame = StackFrame::new(
        Some("module".to_string()),
        Some("name".to_string()),
        Some("filename".to_string()),
        Some("relative_path".to_string()),
        Some("absolute_path".to_string()),
        Some(1),
    );

    assert_eq!(format!("{frame}"), "filename:1 - name");
}

#[test]
fn test_stack_trace_display() {
    let frames = vec![
        StackFrame::new(
            Some("module".to_string()),
            Some("name".to_string()),
            Some("filename".to_string()),
            Some("relative_path".to_string()),
            Some("absolute_path".to_string()),
            Some(1),
        ),
        StackFrame::new(
            Some("module".to_string()),
            Some("name".to_string()),
            Some("filename".to_string()),
            Some("relative_path".to_string()),
            Some("absolute_path".to_string()),
            Some(2),
        ),
    ];

    let stack_trace = StackTrace::new(&BackendConfig::default(), None, None, None, frames);

    assert_eq!(
        format!("{stack_trace}"),
        "filename:2 - name;filename:1 - name"
    );
}

#[test]
fn test_report_record() {
    let mut report = Report::new(HashMap::new());

    let stack_trace = StackTrace::new(&BackendConfig::default(), None, None, None, vec![]);

    report.record(stack_trace);
    assert_eq!(report.data.len(), 1);
}

#[test]
fn test_tag_new() {
    let tag = Tag::new("key".to_string(), "value".to_string());

    assert_eq!(tag.key, "key");
    assert_eq!(tag.value, "value");
}

#[test]
fn test_rule_new() {
    let tid = crate::ThreadId::pthread_self();
    let rule = ThreadTag::new(
        tid.clone(),
        Tag::new("key".to_string(), "value".to_string()),
    );

    assert_eq!(
        rule,
        ThreadTag::new(tid, Tag::new("key".to_string(), "value".to_string()))
    );
}

#[cfg(test)]
fn test_thread_id(v: u64) -> crate::utils::ThreadId {
    (v as libc::pthread_t).into()
}

#[test]
fn test_ruleset_new() {
    let ruleset = ThreadTagsSet::new();

    assert_eq!(ruleset.rules.lock().unwrap().len(), 0);
}

#[test]
fn test_ruleset_add_rule() {
    let tid = crate::ThreadId::pthread_self();
    let ruleset = ThreadTagsSet::new();

    let rule = ThreadTag::new(tid, Tag::new("key".to_string(), "value".to_string()));

    ruleset.add(rule).unwrap();

    assert_eq!(ruleset.rules.lock().unwrap().len(), 1);
}

#[test]
fn test_ruleset_remove_rule() {
    let tid = crate::ThreadId::pthread_self();
    let ruleset = ThreadTagsSet::new();

    let add_rule = ThreadTag::new(
        tid.clone(),
        Tag::new("key".to_string(), "value".to_string()),
    );

    ruleset.add(add_rule).unwrap();

    assert_eq!(ruleset.rules.lock().unwrap().len(), 1);

    let remove_rule = ThreadTag::new(tid, Tag::new("key".to_string(), "value".to_string()));

    ruleset.remove(remove_rule).unwrap();

    assert_eq!(ruleset.rules.lock().unwrap().len(), 0);
}

#[test]
fn test_ruleset() {
    let ruleset = ThreadTagsSet::new();

    ruleset
        .add(ThreadTag::new(
            test_thread_id(1),
            Tag::new("key1".to_string(), "value".to_string()),
        ))
        .unwrap();

    ruleset
        .add(ThreadTag::new(
            test_thread_id(2),
            Tag::new("key1".to_string(), "value".to_string()),
        ))
        .unwrap();

    ruleset
        .add(ThreadTag::new(
            test_thread_id(3),
            Tag::new("key1".to_string(), "value".to_string()),
        ))
        .unwrap();

    // Remove ThreadTag number 2
    ruleset
        .remove(ThreadTag::new(
            test_thread_id(2),
            Tag::new("key1".to_string(), "value".to_string()),
        ))
        .unwrap();

    // Verify ThreadTag number 2 is removed from the ruleset Vector
    assert_eq!(
        ruleset.rules.lock().unwrap().clone(),
        HashSet::from([
            ThreadTag::new(
                test_thread_id(1),
                Tag::new("key1".to_string(), "value".to_string(),)
            ),
            ThreadTag::new(
                test_thread_id(3),
                Tag::new("key1".to_string(), "value".to_string(),)
            )
        ])
    );
}

#[test]
fn test_ruleset_duplicates() {
    let ruleset = ThreadTagsSet::new();

    let tid = crate::ThreadId::pthread_self();
    ruleset
        .add(ThreadTag::new(
            tid.clone(),
            Tag::new("key1".to_string(), "value".to_string()),
        ))
        .unwrap();

    ruleset
        .add(ThreadTag::new(
            tid.clone(),
            Tag::new("key1".to_string(), "value".to_string()),
        ))
        .unwrap();

    assert_eq!(
        ruleset.thread_tags(tid),
        vec![Tag::new("key1".to_string(), "value".to_string())]
    );
}

#[test]
fn test_ruleset_remove_nonexistent() {
    let ruleset = ThreadTagsSet::new();

    let tid = crate::ThreadId::pthread_self();
    ruleset
        .add(ThreadTag::new(
            tid.clone(),
            Tag::new("key1".to_string(), "value".to_string()),
        ))
        .unwrap();

    ruleset
        .remove(ThreadTag::new(
            tid.clone(),
            Tag::new("key2".to_string(), "value".to_string()),
        ))
        .unwrap();

    assert_eq!(
        ruleset.thread_tags(tid),
        vec![Tag::new("key1".to_string(), "value".to_string())]
    );
}

#[test]
fn test_stacktrace_add() {
    let ruleset = ThreadTagsSet::new();

    ruleset
        .add(ThreadTag::new(
            test_thread_id(55),
            Tag::new("keyA".to_string(), "valueA".to_string()),
        ))
        .unwrap();

    ruleset
        .add(ThreadTag::new(
            test_thread_id(100),
            Tag::new("keyB".to_string(), "valueB".to_string()),
        ))
        .unwrap();

    let backend_config = BackendConfig {
        report_pid: true,
        report_thread_id: true,
        report_thread_name: true,
    };

    let stacktrace = StackTrace::new(
        &backend_config,
        Some(1),
        Some(test_thread_id(55)),
        Some("thread_name".to_string()),
        vec![crate::backend::StackFrame::new(
            Some("file1".to_string()),
            Some("function1".to_string()),
            Some("file1".to_string()),
            Some("file1".to_string()),
            Some("file1".to_string()),
            Some(1),
        )],
    );

    // assert initial metadata of the stacktrace
    let mut initial_metadata = crate::backend::Metadata::default();
    initial_metadata.add_tag(Tag::new("pid".to_string(), "1".to_string()));
    initial_metadata.add_tag(Tag::new("thread_id".to_string(), "55".to_string()));
    initial_metadata.add_tag(Tag::new(
        "thread_name".to_string(),
        "thread_name".to_string(),
    ));

    assert_eq!(stacktrace.metadata, initial_metadata);

    // Add the Stacktrace to the Ruleset
    let applied_stacktrace = stacktrace.add_tag_rules(&ruleset);

    initial_metadata.add_tag(Tag::new("keyA".to_string(), "valueA".to_string()));

    // assert that the metadata of the stacktrace is updated
    assert_eq!(applied_stacktrace.metadata, initial_metadata);

    // Re-apply the Ruleset
    let re_applied_stacktrace = applied_stacktrace.add_tag_rules(&ruleset);

    // assert that the metadata of the stacktrace is the same
    assert_eq!(re_applied_stacktrace.metadata, initial_metadata);
}

#[test]
fn test_stackbuffer_record() {
    let mut buffer = StackBuffer::new(HashMap::new());
    let stack_trace = StackTrace::new(
        &BackendConfig::default(),
        None,
        None,
        None,
        vec![StackFrame::new(
            None,
            Some("test_record".to_string()),
            None,
            None,
            None,
            None,
        )],
    );
    // First record
    buffer.record(stack_trace.clone()).unwrap();
    assert_eq!(buffer.data.len(), 1);
    assert_eq!(buffer.data[&stack_trace], 1);

    // Second record
    buffer.record(stack_trace.clone()).unwrap();
    assert_eq!(buffer.data.len(), 1);
    assert_eq!(buffer.data[&stack_trace], 2);
}

#[test]
fn test_stackbuffer_record_with_count() {
    let mut buffer = StackBuffer::new(HashMap::new());
    let stack_trace = StackTrace::new(
        &BackendConfig::default(),
        None,
        None,
        None,
        vec![StackFrame::new(
            None,
            Some("test_record".to_string()),
            None,
            None,
            None,
            None,
        )],
    );
    // First record
    buffer.record_with_count(stack_trace.clone(), 1).unwrap();
    assert_eq!(buffer.data.len(), 1);
    assert_eq!(buffer.data[&stack_trace], 1);

    // Second record
    buffer.record_with_count(stack_trace.clone(), 2).unwrap();
    assert_eq!(buffer.data.len(), 1);
    assert_eq!(buffer.data[&stack_trace], 3);
}

#[test]
fn test_stackbuffer_clear() {
    let mut buffer = StackBuffer::new(HashMap::new());
    let stack_trace = StackTrace::new(
        &BackendConfig::default(),
        None,
        None,
        None,
        vec![StackFrame::new(
            None,
            Some("test_record".to_string()),
            None,
            None,
            None,
            None,
        )],
    );
    // First record
    buffer.record(stack_trace.clone()).unwrap();
    assert_eq!(buffer.data.len(), 1);
    assert_eq!(buffer.data[&stack_trace], 1);

    // Second record
    buffer.record(stack_trace.clone()).unwrap();
    assert_eq!(buffer.data.len(), 1);
    assert_eq!(buffer.data[&stack_trace], 2);

    // Clear
    buffer.clear();
    assert_eq!(buffer.data.len(), 0);
}
