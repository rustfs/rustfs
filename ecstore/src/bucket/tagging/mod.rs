use s3s::dto::Tag;
use url::form_urlencoded;

pub fn decode_tags(tags: &str) -> Vec<Tag> {
    let values = form_urlencoded::parse(tags.as_bytes());

    let mut list = Vec::new();

    for (k, v) in values {
        if k.is_empty() || v.is_empty() {
            continue;
        }

        list.push(Tag {
            key: Some(k.to_string()),
            value: Some(v.to_string()),
        });
    }

    list
}

pub fn encode_tags(tags: Vec<Tag>) -> String {
    let mut encoded = form_urlencoded::Serializer::new(String::new());

    for tag in tags.iter() {
        if let (Some(k), Some(v)) = (tag.key.as_ref(), tag.value.as_ref()) {
            encoded.append_pair(k.as_str(), v.as_str());
        }
    }

    encoded.finish()
}
