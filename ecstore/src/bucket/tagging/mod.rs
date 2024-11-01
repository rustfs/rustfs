use s3s::dto::Tag;
use url::form_urlencoded;

pub fn decode_tags(tags: &str) -> Vec<Tag> {
    let values = form_urlencoded::parse(tags.as_bytes());

    let mut list = Vec::new();

    for (k, v) in values {
        list.push(Tag {
            key: k.to_string(),
            value: v.to_string(),
        });
    }

    list
}

pub fn encode_tags(tags: Vec<Tag>) -> String {
    let mut encoded = form_urlencoded::Serializer::new(String::new());

    for tag in tags.iter() {
        encoded.append_pair(tag.key.as_str(), tag.value.as_str());
    }

    encoded.finish()
}
