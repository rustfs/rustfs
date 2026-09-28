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

use s3s::{S3Result, dto::WebsiteConfiguration, s3_error};

pub(crate) fn validate_website_configuration(config: &WebsiteConfiguration) -> S3Result<()> {
    if config.redirect_all_requests_to.is_some()
        && (config.index_document.is_some() || config.error_document.is_some() || config.routing_rules.is_some())
    {
        return Err(s3_error!(
            MalformedXML,
            "RedirectAllRequestsTo cannot be combined with other website settings"
        ));
    }
    if config.redirect_all_requests_to.is_none() && config.index_document.is_none() {
        return Err(s3_error!(
            MalformedXML,
            "IndexDocument is required unless RedirectAllRequestsTo is configured"
        ));
    }
    if let Some(index) = &config.index_document
        && (index.suffix.is_empty() || index.suffix.contains('/'))
    {
        return Err(s3_error!(MalformedXML, "IndexDocument suffix must be a single nonempty name"));
    }
    if let Some(error) = &config.error_document
        && error.key.is_empty()
    {
        return Err(s3_error!(MalformedXML, "ErrorDocument key cannot be empty"));
    }
    if let Some(rules) = &config.routing_rules {
        if rules.len() > 50 {
            return Err(s3_error!(MalformedXML, "RoutingRules cannot contain more than 50 rules"));
        }
        if rules.is_empty() {
            return Err(s3_error!(MalformedXML, "RoutingRules cannot be empty"));
        }
        for rule in rules {
            if let Some(condition) = &rule.condition
                && condition.key_prefix_equals.is_none()
                && condition.http_error_code_returned_equals.is_none()
            {
                return Err(s3_error!(MalformedXML, "RoutingRule Condition cannot be empty"));
            }
            if rule.redirect.host_name.is_none()
                && rule.redirect.protocol.is_none()
                && rule.redirect.replace_key_prefix_with.is_none()
                && rule.redirect.replace_key_with.is_none()
                && rule.redirect.http_redirect_code.is_none()
            {
                return Err(s3_error!(MalformedXML, "RoutingRule Redirect cannot be empty"));
            }
            if rule.redirect.replace_key_prefix_with.is_some() && rule.redirect.replace_key_with.is_some() {
                return Err(s3_error!(MalformedXML, "RoutingRule redirect key replacements are mutually exclusive"));
            }
            if let Some(protocol) = rule.redirect.protocol.as_ref().map(|protocol| protocol.as_str())
                && !matches!(protocol, "http" | "https")
            {
                return Err(s3_error!(MalformedXML, "RoutingRule protocol must be http or https"));
            }
            if let Some(code) = rule.redirect.http_redirect_code.as_deref()
                && !matches!(code, "301" | "302" | "303" | "307" | "308")
            {
                return Err(s3_error!(MalformedXML, "RoutingRule redirect code is invalid"));
            }
            if let Some(host) = rule.redirect.host_name.as_deref() {
                validate_website_redirect_host(host)?;
            }
            if let Some(code) = rule
                .condition
                .as_ref()
                .and_then(|condition| condition.http_error_code_returned_equals.as_deref())
                && (code.parse::<u16>().ok().is_none_or(|value| !(400..=599).contains(&value)))
            {
                return Err(s3_error!(MalformedXML, "RoutingRule error code is invalid"));
            }
        }
    }
    if let Some(redirect) = &config.redirect_all_requests_to {
        validate_website_redirect_host(&redirect.host_name)?;
        if let Some(protocol) = redirect.protocol.as_ref().map(|protocol| protocol.as_str())
            && !matches!(protocol, "http" | "https")
        {
            return Err(s3_error!(MalformedXML, "RedirectAllRequestsTo protocol must be http or https"));
        }
    }
    Ok(())
}

fn validate_website_redirect_host(host: &str) -> S3Result<()> {
    let url =
        url::Url::parse(&format!("http://{host}/")).map_err(|_| s3_error!(MalformedXML, "website redirect host is invalid"))?;
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(s3_error!(MalformedXML, "website redirect host is invalid"));
    }
    Ok(())
}
