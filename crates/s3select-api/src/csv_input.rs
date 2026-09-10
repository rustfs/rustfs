// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::Bytes;
use datafusion::object_store::{Error, Result};
use futures::{Stream, StreamExt, stream::BoxStream};
use transform_stream::AsyncTryStream;

use crate::SelectError;

/// Arrow accepts byte-sized CSV controls. Unicode quotes need streaming normalization.
pub fn csv_input_requires_normalization(quote: Option<&str>, escape: Option<&str>) -> bool {
    quote.is_some_and(|quote| quote.len() > 1) || escape.is_some_and(|escape| escape.len() > 1)
}

/// CSV syntax independent of request headers, serialization formats, or S3 DTOs.
#[derive(Default)]
pub(crate) struct CsvSyntax<'a> {
    pub quote: Option<&'a str>,
    pub escape: Option<&'a str>,
    pub field: Option<&'a str>,
    pub record: Option<&'a str>,
    pub comment: Option<u8>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    FieldStart,
    Unquoted,
    Quoted,
    AfterQuote,
    Escaped,
    Comment,
}

/// Emits ordinary CSV with every field quoted. This avoids reserving a sentinel
/// byte that might also appear in a UTF-8 field. Only a partial control token is
/// retained between chunks; neither records nor objects are buffered.
struct CsvInputNormalizer {
    quote: Vec<u8>,
    escape: Vec<u8>,
    field: Vec<u8>,
    record: Vec<u8>,
    comment: Option<u8>,
    default_records: bool,
    state: State,
    record_start: bool,
    carry: Vec<u8>,
    token_size: usize,
}

impl CsvInputNormalizer {
    fn new(csv: &CsvSyntax<'_>) -> Self {
        let quote = csv
            .quote
            .filter(|value| !value.is_empty())
            .unwrap_or("\"")
            .as_bytes()
            .to_vec();
        let escape = csv
            .escape
            .filter(|value| !value.is_empty())
            .unwrap_or("\"")
            .as_bytes()
            .to_vec();
        let field = csv.field.filter(|value| !value.is_empty()).unwrap_or(",").as_bytes().to_vec();
        let record = csv
            .record
            .filter(|value| !value.is_empty())
            .unwrap_or("\n")
            .as_bytes()
            .to_vec();
        let token_size = quote.len().max(escape.len()).max(field.len()).max(record.len()).max(2);
        Self {
            quote,
            escape,
            field,
            record,
            comment: csv.comment,
            default_records: csv.record.is_none(),
            state: State::FieldStart,
            record_start: true,
            carry: Vec::new(),
            token_size,
        }
    }

    fn record_len(&self, bytes: &[u8]) -> usize {
        if self.default_records && bytes.starts_with(b"\r\n") {
            2
        } else if self.default_records && bytes.starts_with(b"\r") {
            1
        } else if bytes.starts_with(&self.record) {
            self.record.len()
        } else {
            0
        }
    }

    fn push_value(output: &mut Vec<u8>, bytes: &[u8]) {
        for byte in bytes {
            if *byte == b'"' {
                output.push(b'"');
            }
            output.push(*byte);
        }
    }

    fn convert(&mut self, chunk: &[u8], last: bool) -> std::result::Result<Vec<u8>, SelectError> {
        let mut bytes = std::mem::take(&mut self.carry);
        bytes.extend_from_slice(chunk);
        let end = if last {
            bytes.len()
        } else {
            bytes.len().saturating_sub(self.token_size - 1)
        };
        let mut output = Vec::with_capacity(bytes.len());
        let mut pos = 0;
        while pos < end {
            let rest = &bytes[pos..];
            let record_len = self.record_len(rest);
            let field = rest.starts_with(&self.field) && self.field.len() > record_len;
            match self.state {
                State::Comment => {
                    if record_len > 0 {
                        self.state = State::FieldStart;
                        pos += record_len;
                    } else {
                        pos += 1;
                    }
                }
                State::Escaped => {
                    if record_len > 0 {
                        return Err(SelectError::CsvParsingError);
                    }
                    Self::push_value(&mut output, &rest[..1]);
                    self.state = State::Quoted;
                    pos += 1;
                }
                State::Quoted if rest.starts_with(&self.quote) => {
                    self.state = State::AfterQuote;
                    pos += self.quote.len();
                }
                State::Quoted if rest.starts_with(&self.escape) => {
                    self.state = State::Escaped;
                    pos += self.escape.len();
                }
                State::Quoted => {
                    if record_len > 0 {
                        return Err(SelectError::CsvParsingError);
                    }
                    Self::push_value(&mut output, &rest[..1]);
                    pos += 1;
                }
                State::AfterQuote if rest.starts_with(&self.quote) => {
                    Self::push_value(&mut output, &self.quote);
                    self.state = State::Quoted;
                    pos += self.quote.len();
                }
                State::FieldStart if self.record_start && self.comment == Some(rest[0]) => {
                    self.state = State::Comment;
                    pos += 1;
                }
                State::FieldStart if rest.starts_with(&self.quote) => {
                    output.push(b'"');
                    self.state = State::Quoted;
                    self.record_start = false;
                    pos += self.quote.len();
                }
                _ if field || record_len > 0 => {
                    if self.state == State::FieldStart {
                        if field || !self.record_start {
                            output.extend_from_slice(b"\"\"");
                        }
                    } else {
                        output.push(b'"');
                    }
                    output.push(if field { b',' } else { b'\n' });
                    self.state = State::FieldStart;
                    self.record_start = !field;
                    pos += if field { self.field.len() } else { record_len };
                }
                _ => {
                    if self.state == State::FieldStart {
                        output.push(b'"');
                    }
                    self.state = State::Unquoted;
                    self.record_start = false;
                    Self::push_value(&mut output, &rest[..1]);
                    pos += 1;
                }
            }
        }
        self.carry.extend_from_slice(&bytes[pos..]);
        if last {
            match self.state {
                State::Quoted | State::Escaped => return Err(SelectError::CsvParsingError),
                State::Unquoted | State::AfterQuote => output.push(b'"'),
                State::FieldStart if !self.record_start => output.extend_from_slice(b"\"\""),
                State::FieldStart | State::Comment => {}
            }
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalize_chunks(csv: &CsvSyntax<'_>, input: &[u8], chunk_size: usize) -> Vec<u8> {
        let mut normalizer = CsvInputNormalizer::new(csv);
        let mut output = Vec::new();
        for chunk in input.chunks(chunk_size) {
            output.extend(normalizer.convert(chunk, false).expect("normalize complete CSV input"));
            assert!(normalizer.carry.len() < normalizer.token_size, "only a partial token may be retained");
        }
        output.extend(normalizer.convert(&[], true).expect("finish complete CSV input"));
        output
    }

    #[test]
    fn unicode_csv_quotes_preserve_values_at_every_chunk_boundary() {
        let cases = [
            ("ع", "\"", "عcol1ع,عcol2ع,عcol3ع\n", "\"col1\",\"col2\",\"col3\"\n"),
            ("ع", "\"", "\"left\",tail\n", "\"\"\"left\"\"\",\"tail\"\n"),
            ("ع", "\"", "عA,Bع,plain\n", "\"A,B\",\"plain\"\n"),
            ("ع", "\"", "عAععBع,tail\n", "\"AعB\",\"tail\"\n"),
            ("ع", "\"", "عA\"عBع,tail\n", "\"AعB\",\"tail\"\n"),
            ("ع", "\\", "عA\\\"Bع,tail\n", "\"A\"\"B\",\"tail\"\n"),
            ("\"", "界", "\"A界\"B\",\"C\"\n", "\"A\"\"B\",\"C\"\n"),
            ("🦀", "🦀", "🦀A🦀🦀B🦀,C\n", "\"A🦀B\",\"C\"\n"),
            ("ع", "\"", "a\0b,عc\0dع\n", "\"a\0b\",\"c\0d\"\n"),
            ("ع", "\"", "AعB,tail\n", "\"AعB\",\"tail\"\n"),
            ("ع", "\"", "عaعsuffix,tail\n", "\"asuffix\",\"tail\"\n"),
            ("ع", "\"", ",\n", "\"\",\"\"\n"),
            ("ع", "\"", "a,", "\"a\",\"\""),
            ("ع", "\"", "عع", "\"\""),
            ("ع", "\"", "\n", "\n"),
            ("ع", "\"", "", ""),
        ];
        for (quote, escape, input, expected) in cases {
            let csv = CsvSyntax {
                quote: Some(quote),
                escape: Some(escape),
                record: Some("\n"),
                ..Default::default()
            };
            for chunk_size in 1..=input.len().max(1) {
                assert_eq!(
                    normalize_chunks(&csv, input.as_bytes(), chunk_size),
                    expected.as_bytes(),
                    "input={input:?}, chunk_size={chunk_size}"
                );
            }
        }
    }

    #[test]
    fn unicode_csv_quotes_keep_custom_delimiters_and_comments_out_of_values() {
        let csv = CsvSyntax {
            quote: Some("ع"),
            escape: Some("\\"),
            field: Some("界"),
            record: Some("^Y"),
            comment: Some(b'#'),
        };
        let input = "#skipع界^Yعa界bع界\"literal\"^Yعline\nbreakع界end^Y";
        let expected = "\"a界b\",\"\"\"literal\"\"\"\n\"line\nbreak\",\"end\"\n";
        for chunk_size in 1..=input.len() {
            assert_eq!(normalize_chunks(&csv, input.as_bytes(), chunk_size), expected.as_bytes());
        }
    }

    #[test]
    fn unicode_csv_quotes_reject_unterminated_fields_and_quoted_record_delimiters() {
        for input in ["عunfinished", "عescape\\", "عline\nbreakع\n", "عline\\\nbreakع\n"] {
            let csv = CsvSyntax {
                quote: Some("ع"),
                escape: Some("\\"),
                ..Default::default()
            };
            let mut normalizer = CsvInputNormalizer::new(&csv);
            assert_eq!(normalizer.convert(input.as_bytes(), true), Err(SelectError::CsvParsingError));
        }
    }

    #[test]
    fn unicode_csv_quotes_preserve_omitted_syntax_defaults() {
        assert!(!csv_input_requires_normalization(None, None));
        assert!(!csv_input_requires_normalization(Some("\""), Some("\\")));
        assert!(csv_input_requires_normalization(Some("ع"), None));
        assert!(csv_input_requires_normalization(None, Some("界")));

        let quote_only = CsvSyntax {
            quote: Some("ع"),
            ..Default::default()
        };
        assert_eq!(
            normalize_chunks(&quote_only, "عA\"عBع,tail\r\n".as_bytes(), 1),
            "\"AعB\",\"tail\"\n".as_bytes()
        );
        let escape_only = CsvSyntax {
            escape: Some("界"),
            ..Default::default()
        };
        assert_eq!(
            normalize_chunks(&escape_only, "\"A界\"B\",tail\r\n".as_bytes(), 1),
            b"\"A\"\"B\",\"tail\"\n"
        );
    }

    #[test]
    fn unicode_csv_quotes_stream_large_fields_without_retaining_records() {
        let csv = CsvSyntax {
            quote: Some("ع"),
            ..Default::default()
        };
        let mut normalizer = CsvInputNormalizer::new(&csv);
        let chunk = vec![b'x'; 64 * 1024];
        let mut output_len = normalizer.convert("ع".as_bytes(), false).expect("opening quote").len();
        for _ in 0..64 {
            let output = normalizer.convert(&chunk, false).expect("stream field chunk");
            assert!(output.len() >= chunk.len() - 3, "field data must be emitted before its closing quote");
            assert!(normalizer.carry.len() < 4);
            output_len += output.len();
        }
        output_len += normalizer.convert("ع\n".as_bytes(), true).expect("close field").len();
        assert_eq!(output_len, chunk.len() * 64 + 3);
    }
}

pub(crate) fn normalize_csv_stream<S>(stream: S, csv: &CsvSyntax<'_>) -> BoxStream<'static, Result<Bytes>>
where
    S: Stream<Item = Result<Bytes>> + Send + 'static,
{
    let mut normalizer = CsvInputNormalizer::new(csv);
    AsyncTryStream::<Bytes, Error, _>::new(|mut y| async move {
        futures::pin_mut!(stream);
        while let Some(chunk) = stream.next().await {
            let converted = normalizer.convert(&chunk?, false).map_err(|source| Error::Generic {
                store: "EcObjectStore",
                source: Box::new(source),
            })?;
            if !converted.is_empty() {
                y.yield_ok(Bytes::from(converted)).await;
            }
        }
        let converted = normalizer.convert(&[], true).map_err(|source| Error::Generic {
            store: "EcObjectStore",
            source: Box::new(source),
        })?;
        if !converted.is_empty() {
            y.yield_ok(Bytes::from(converted)).await;
        }
        Ok(())
    })
    .boxed()
}

#[cfg(test)]
mod stream_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct DropProbe(Arc<AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn unicode_csv_quotes_drop_the_source_without_reading_ahead() {
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let source =
            futures::stream::unfold((DropProbe(Arc::clone(&dropped)), Arc::clone(&polls)), |(guard, polls)| async move {
                polls.fetch_add(1, Ordering::SeqCst);
                Some((Ok(Bytes::from_static("عvalueع\n".as_bytes())), (guard, polls)))
            });
        let csv = CsvSyntax {
            quote: Some("ع"),
            ..Default::default()
        };
        let mut stream = normalize_csv_stream(source, &csv);
        assert!(!stream.next().await.expect("first output").expect("valid CSV").is_empty());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        drop(stream);
        assert!(dropped.load(Ordering::SeqCst), "cancellation must release the source reader");
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unicode_csv_quotes_preserve_source_errors_after_partial_output() {
        let source = futures::stream::iter([
            Ok(Bytes::from_static("عvalueع\n".as_bytes())),
            Err(Error::Generic {
                store: "fixture",
                source: std::io::Error::other("source read failed").into(),
            }),
        ]);
        let csv = CsvSyntax {
            quote: Some("ع"),
            ..Default::default()
        };
        let mut stream = normalize_csv_stream(source, &csv);
        assert!(!stream.next().await.expect("partial output").expect("valid prefix").is_empty());
        let error = stream
            .next()
            .await
            .expect("source failure must remain visible")
            .expect_err("must not return a successful tail");
        assert!(error.to_string().contains("source read failed"));
        assert!(stream.next().await.is_none());
    }
}
