#![no_main]

//! Fuzz target for S3 bucket notification XML parsing and validation
//! (`NotificationConfiguration::from_reader` and `NotificationConfiguration::validate`).
//!
//! Exercises both raw XML bytes (malformed, truncated, deeply nested, invalid
//! Unicode) and bounded `Arbitrary` structured XML variants, enforcing
//! deterministic business-rejection oracles in addition to no-panic/no-OOM.

use std::collections::HashSet;
use std::io::Cursor;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use rustfs_notify::rules::{NotificationConfiguration, ParseConfigError};

const MAX_INPUT_BYTES: usize = 64 * 1024;
const MAX_QUEUES: usize = 8;
const MAX_EVENTS: usize = 8;
const MAX_FILTERS: usize = 8;
const MAX_TEXT_BYTES: usize = 256;

const KNOWN_REGION: &str = "us-east-1";
const KNOWN_ARN: &str = "arn:rustfs:sqs:us-east-1:1:webhook";
const ALT_KNOWN_ARN: &str = "arn:rustfs:sqs:us-east-1:2:webhook";

#[derive(Arbitrary, Debug)]
struct FuzzFilterRule {
    name_kind: u8,
    custom_name: String,
    value: String,
}

#[derive(Arbitrary, Debug)]
struct FuzzQueueXml {
    id: Option<String>,
    arn_kind: u8,
    custom_arn: String,
    events: Vec<u8>,
    filters: Vec<FuzzFilterRule>,
}

#[derive(Arbitrary, Debug)]
struct FuzzNotifyXml {
    include_topic: bool,
    include_lambda: bool,
    duplicate_first_queue: bool,
    queues: Vec<FuzzQueueXml>,
}

fn clamp_str(input: &str, max_bytes: usize) -> &str {
    if input.len() <= max_bytes {
        return input;
    }
    let mut end = max_bytes;
    while end > 0 && !input.is_char_boundary(end) {
        end -= 1;
    }
    &input[..end]
}

fn escape_xml(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            '\0' => {}
            _ => out.push(ch),
        }
    }
    out
}

fn render_structured_xml(spec: FuzzNotifyXml) -> String {
    let mut xml = String::from("<NotificationConfiguration>");
    let mut rendered_queues = Vec::new();
    for queue in spec.queues.into_iter().take(MAX_QUEUES) {
        let mut q = String::from("<QueueConfiguration>");
        if let Some(id) = queue.id {
            let escaped = escape_xml(clamp_str(&id, MAX_TEXT_BYTES));
            q.push_str("<Id>");
            q.push_str(&escaped);
            q.push_str("</Id>");
        }
        let arn = match queue.arn_kind % 4 {
            0 => KNOWN_ARN.to_string(),
            1 => ALT_KNOWN_ARN.to_string(),
            2 => "arn:rustfs:sqs:eu-west-1:1:webhook".to_string(),
            _ => escape_xml(clamp_str(&queue.custom_arn, MAX_TEXT_BYTES)),
        };
        q.push_str("<Queue>");
        q.push_str(&arn);
        q.push_str("</Queue>");
        for ev in queue.events.into_iter().take(MAX_EVENTS) {
            let ev_name = match ev % 4 {
                0 => "s3:ObjectCreated:Put",
                1 => "s3:ObjectRemoved:Delete",
                2 => "s3:ObjectCreated:*",
                _ => "s3:ObjectRestore:Completed",
            };
            q.push_str("<Event>");
            q.push_str(ev_name);
            q.push_str("</Event>");
        }
        if !queue.filters.is_empty() {
            q.push_str("<Filter><S3Key>");
            for rule in queue.filters.into_iter().take(MAX_FILTERS) {
                let name = match rule.name_kind % 4 {
                    0 => "prefix".to_string(),
                    1 => "suffix".to_string(),
                    2 => "PREFIX".to_string(),
                    _ => escape_xml(clamp_str(&rule.custom_name, 64)),
                };
                let value = escape_xml(clamp_str(&rule.value, MAX_TEXT_BYTES));
                q.push_str("<FilterRule><Name>");
                q.push_str(&name);
                q.push_str("</Name><Value>");
                q.push_str(&value);
                q.push_str("</Value></FilterRule>");
            }
            q.push_str("</S3Key></Filter>");
        }
        q.push_str("</QueueConfiguration>");
        rendered_queues.push(q);
    }

    if spec.duplicate_first_queue
        && let Some(first) = rendered_queues.first().cloned()
    {
        rendered_queues.push(first);
    }
    for q in rendered_queues {
        xml.push_str(&q);
    }

    if spec.include_topic {
        xml.push_str("<TopicConfiguration><Topic>arn:aws:sns:us-east-1:123456789012:topic</Topic></TopicConfiguration>");
    }
    if spec.include_lambda {
        xml.push_str(
            "<CloudFunctionConfiguration><CloudFunction>arn:aws:lambda:us-east-1:123456789012:function:fn</CloudFunction></CloudFunctionConfiguration>",
        );
    }
    xml.push_str("</NotificationConfiguration>");
    xml
}

fn has_invalid_filter_rule(config: &NotificationConfiguration) -> bool {
    for queue in &config.queue_list {
        let mut has_prefix = false;
        let mut has_suffix = false;
        for rule in &queue.filter.filter_rule_list {
            let is_prefix = rule.name.eq_ignore_ascii_case("prefix");
            let is_suffix = rule.name.eq_ignore_ascii_case("suffix");
            if !is_prefix && !is_suffix {
                return true;
            }
            if rule.value.split('/').any(|seg| seg == "." || seg == "..")
                || rule.value.chars().count() > 1024
                || rule.value.contains('\\')
            {
                return true;
            }
            if is_prefix {
                if has_prefix {
                    return true;
                }
                has_prefix = true;
            } else {
                if has_suffix {
                    return true;
                }
                has_suffix = true;
            }
        }
    }
    false
}

fn has_duplicate_queue(config: &NotificationConfiguration) -> bool {
    let mut seen = HashSet::new();
    for queue in &config.queue_list {
        if !seen.insert((queue.id.clone(), queue.arn.to_string())) {
            return true;
        }
    }
    false
}

fn exercise_xml_bytes(bytes: &[u8]) {
    let known_arns = [KNOWN_ARN.to_string(), ALT_KNOWN_ARN.to_string()];
    let Ok(config) = NotificationConfiguration::from_reader(Cursor::new(bytes)) else {
        return;
    };

    let result = config.validate(KNOWN_REGION, &known_arns);

    if !config.lambda_list.is_empty() || !config.topic_list.is_empty() {
        assert!(
            matches!(result, Err(ParseConfigError::UnsupportedConfiguration)),
            "TopicConfiguration/CloudFunctionConfiguration must be deterministically rejected"
        );
        return;
    }

    if has_invalid_filter_rule(&config) {
        assert!(
            result.is_err(),
            "NotificationConfiguration with invalid or duplicate filter rules must be rejected"
        );
    }

    if config.queue_list.iter().any(|q| q.events.is_empty()) {
        assert!(result.is_err(), "QueueConfiguration with missing events must be rejected");
    }

    if has_duplicate_queue(&config) {
        assert!(
            result.is_err(),
            "NotificationConfiguration with duplicate (id, arn) queue must be rejected"
        );
    }

    if result.is_ok()
        && let Some(first_queue) = config.queue_list.first().cloned()
    {
        let mut duplicated = config.clone();
        duplicated.queue_list.push(first_queue);
        assert!(
            matches!(
                duplicated.validate(KNOWN_REGION, &known_arns),
                Err(ParseConfigError::DuplicateQueueConfiguration(_, _))
            ),
            "synthesized duplicate queue must trigger DuplicateQueueConfiguration"
        );
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    exercise_xml_bytes(data);

    let mut unstructured = Unstructured::new(data);
    if let Ok(spec) = FuzzNotifyXml::arbitrary(&mut unstructured) {
        let xml = render_structured_xml(spec);
        if xml.len() <= MAX_INPUT_BYTES {
            exercise_xml_bytes(xml.as_bytes());
        }
    }
});
