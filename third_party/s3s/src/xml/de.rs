//! AWS restXml deserializer
//!
//! See <https://smithy.io/2.0/aws/protocols/aws-restxml-protocol.html#xml-shape-serialization>
//!

use crate::dto::{self, List, Timestamp, TimestampFormat};

use std::fmt;

use quick_xml::Reader;
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use stdx::str::StrExt;

/// A data type that can be deserialized with AWS restXml deserializer
pub trait Deserialize<'xml>: Sized {
    /// Deserializes the data type
    ///
    /// # Errors
    /// Returns an error if the deserialization fails
    fn deserialize(d: &mut Deserializer<'xml>) -> DeResult<Self>;
}

/// A data type that can be deserialized with AWS restXml deserializer
pub trait DeserializeContent<'xml>: Sized {
    /// Deserializes the content of the data type
    ///
    /// # Errors
    /// Returns an error if the deserialization fails
    fn deserialize_content(d: &mut Deserializer<'xml>) -> DeResult<Self>;
}

/// AWS restXml deserializer
pub struct Deserializer<'xml> {
    /// xml reader
    inner: Reader<&'xml [u8]>,

    /// peeked event
    peeked: Option<DeEvent<'xml>>,

    /// store an extra event
    next_slot: Option<DeEvent<'xml>>,
}

/// XML deserialization result
pub type DeResult<T = (), E = DeError> = std::result::Result<T, E>;

/// XML deserialization error
#[derive(Debug, thiserror::Error)]
pub enum DeError {
    /// Invalid XML
    #[error("invalid XML: {0}")]
    InvalidXml(quick_xml::Error),

    /// Unexpected EOF
    #[error("unexpected eof")]
    UnexpectedEof,

    /// Unexpected start
    #[error("unexpected start")]
    UnexpectedStart,

    /// Unexpected end
    #[error("unexpected end")]
    UnexpectedEnd,

    /// Unexpected tag name
    #[error("unexpected tag name")]
    UnexpectedTagName,

    #[error("invalid attribute")]
    InvalidAttribute,

    #[error("unexpected attribute name")]
    UnexpectedAttributeName,

    /// Invalid content
    #[error("invalid content")]
    InvalidContent,

    /// Missing field
    #[error("missing field")]
    MissingField,

    /// Duplicate field
    #[error("duplicate field")]
    DuplicateField,
}

/// XML deserialization event
#[derive(Clone)]
enum DeEvent<'xml> {
    /// start
    Start(BytesStart<'xml>),
    /// end
    End(BytesEnd<'xml>),
    /// text
    Text(BytesText<'xml>),
    /// eof
    Eof,
}

impl<'xml> Deserializer<'xml> {
    /// Creates a new deserializer
    #[must_use]
    pub fn new(xml: &'xml [u8]) -> Self {
        Self {
            inner: Reader::from_reader(xml),
            peeked: None,
            next_slot: None,
        }
    }

    /// Reads the next event
    fn read_event(&mut self) -> DeResult<DeEvent<'xml>> {
        if let Some(ev) = self.next_slot.take() {
            return Ok(ev);
        }
        loop {
            let ev = self.inner.read_event().map_err(invalid_xml)?;
            let de = match ev {
                Event::Start(x) => DeEvent::Start(x),
                Event::End(x) => DeEvent::End(x),
                Event::Text(x) => DeEvent::Text(x),
                Event::Eof => DeEvent::Eof,

                Event::Empty(x) => {
                    // translate `<CSV/>` to `<CSV></CSV>`
                    self.next_slot = Some(DeEvent::End(x.to_end().into_owned()));
                    DeEvent::Start(x)
                }

                // ignore the others
                Event::Comment(_) | Event::CData(_) | Event::Decl(_) | Event::PI(_) | Event::DocType(_) => continue,
            };
            break Ok(de);
        }
    }

    /// Returns the next event
    fn next_event(&mut self) -> DeResult<DeEvent<'xml>> {
        if let Some(ev) = self.peeked.take() {
            return Ok(ev);
        }
        self.read_event()
    }

    /// Peeks the next event
    #[allow(clippy::unwrap_used, clippy::unwrap_in_result)]
    fn peek_event(&mut self) -> DeResult<DeEvent<'xml>> {
        if self.peeked.is_none() {
            self.peeked = Some(self.read_event()?);
        }
        Ok(self.peeked.clone().unwrap())
    }

    /// Consumes the peeked event
    fn consume_peeked(&mut self) {
        self.peeked = None;
    }

    /// Expects a start event
    fn expect_start(&mut self, name: &[u8]) -> DeResult {
        loop {
            match self.next_event()? {
                DeEvent::Start(x) => {
                    if x.name().as_ref() != name {
                        return Err(unexpected_tag_name());
                    }
                    return Ok(());
                }
                DeEvent::End(_) => return Err(unexpected_end()),
                DeEvent::Text(_) => continue,
                DeEvent::Eof => return Err(unexpected_eof()),
            }
        }
    }

    /// Expects a start event with any of the given names. Returns the matched name (owned).
    fn expect_start_any(&mut self, names: &[&str]) -> DeResult<Vec<u8>> {
        let names_bytes: Vec<Vec<u8>> = names.iter().map(|s| s.as_bytes().to_vec()).collect();
        loop {
            match self.next_event()? {
                DeEvent::Start(x) => {
                    let name = x.name().as_ref().to_vec();
                    if names_bytes.contains(&name) {
                        return Ok(name);
                    }
                    return Err(unexpected_tag_name());
                }
                DeEvent::End(_) => return Err(unexpected_end()),
                DeEvent::Text(_) => continue,
                DeEvent::Eof => return Err(unexpected_eof()),
            }
        }
    }

    /// Expects an end event (accepts both `&[u8]` and `AsRef<[u8]>` for flexibility)
    fn expect_end(&mut self, name: impl AsRef<[u8]>) -> DeResult {
        let name = name.as_ref();
        loop {
            match self.next_event()? {
                DeEvent::Start(_) => return Err(unexpected_start()),
                DeEvent::End(x) => {
                    if x.name().as_ref() != name {
                        return Err(unexpected_tag_name());
                    }
                    return Ok(());
                }
                DeEvent::Text(_) => continue,
                DeEvent::Eof => return Err(unexpected_eof()),
            }
        }
    }

    /// Expects an eof event
    pub fn expect_eof(&mut self) -> DeResult {
        loop {
            match self.next_event()? {
                DeEvent::Start(_) => return Err(unexpected_start()),
                DeEvent::End(_) => return Err(unexpected_end()),
                DeEvent::Text(_) => continue,
                DeEvent::Eof => return Ok(()),
            }
        }
    }

    /// Deserializes an element
    ///
    /// # Errors
    /// Returns an error if the deserialization fails.
    pub fn named_element<T>(&mut self, name: &str, f: impl FnOnce(&mut Self) -> DeResult<T>) -> DeResult<T> {
        self.expect_start(name.as_bytes())?;
        let ans = f(self)?;
        self.expect_end(name.as_bytes())?;
        Ok(ans)
    }

    /// Deserializes an element with any of the given root names (`MinIO` compatibility).
    ///
    /// # Errors
    /// Returns an error if the deserialization fails.
    pub fn named_element_any<T>(&mut self, names: &[&str], f: impl FnOnce(&mut Self) -> DeResult<T>) -> DeResult<T> {
        let name = self.expect_start_any(names)?;
        let ans = f(self)?;
        self.expect_end(name)?;
        Ok(ans)
    }

    pub fn element<T>(&mut self, f: impl FnOnce(&mut Self, &[u8]) -> DeResult<T>) -> DeResult<T> {
        loop {
            match self.peek_event()? {
                DeEvent::Start(start) => {
                    self.consume_peeked();
                    let name = start.name();
                    let ans = f(self, name.as_ref())?;
                    self.expect_end(name.as_ref())?;
                    return Ok(ans);
                }
                DeEvent::Text(_) => {
                    self.consume_peeked();
                }
                DeEvent::End(_) | DeEvent::Eof => {
                    return Err(unexpected_end());
                }
            }
        }
    }

    /// Deserializes each element
    ///
    /// # Errors
    /// Returns an error if the deserialization fails.
    pub fn for_each_element(&mut self, mut f: impl FnMut(&mut Self, &[u8]) -> DeResult) -> DeResult {
        loop {
            match self.peek_event()? {
                DeEvent::Start(start) => {
                    self.consume_peeked();

                    let name = start.name();
                    let name = name.as_ref();
                    f(self, name)?;
                    self.expect_end(name)?;

                    continue;
                }
                DeEvent::Text(_) => {
                    self.consume_peeked();
                    continue;
                }
                DeEvent::End(_) | DeEvent::Eof => {
                    return Ok(());
                }
            }
        }
    }

    pub fn for_each_element_with_start(&mut self, mut f: impl FnMut(&mut Self, &[u8], &BytesStart<'_>) -> DeResult) -> DeResult {
        loop {
            match self.peek_event()? {
                DeEvent::Start(start) => {
                    self.consume_peeked();

                    let name = start.name();
                    let name = name.as_ref();
                    f(self, name, &start)?;
                    self.expect_end(name)?;

                    continue;
                }
                DeEvent::Text(_) => {
                    self.consume_peeked();
                    continue;
                }
                DeEvent::End(_) | DeEvent::Eof => {
                    return Ok(());
                }
            }
        }
    }

    /// Deserializes text
    ///
    /// # Errors
    /// Returns an error if the deserialization fails.
    pub fn text<T>(&mut self, f: impl FnOnce(BytesText<'xml>) -> DeResult<T>) -> DeResult<T> {
        match self.peek_event()? {
            DeEvent::Start(_) => {
                self.consume_peeked();
                Err(unexpected_start())
            }
            DeEvent::End(_) => {
                f(BytesText::from_escaped("")) //
            }
            DeEvent::Text(x) => {
                self.consume_peeked();
                f(x)
            }
            DeEvent::Eof => {
                self.consume_peeked();
                Err(unexpected_eof())
            }
        }
    }

    /// Deserializes the content of a field
    ///
    /// # Errors
    /// Returns an error if the deserialization fails.
    pub fn content<T: DeserializeContent<'xml>>(&mut self) -> DeResult<T> {
        T::deserialize_content(self)
    }

    pub fn list_content<T: DeserializeContent<'xml>>(&mut self, name: &str) -> DeResult<List<T>> {
        let mut list = List::new();
        self.for_each_element(|d, x| {
            if x != name.as_bytes() {
                return Err(unexpected_tag_name());
            }
            list.push(d.content()?);
            Ok(())
        })?;
        Ok(list)
    }

    pub fn timestamp(&mut self, fmt: TimestampFormat) -> DeResult<Timestamp> {
        self.text(|t| {
            let string = str::from_ascii_simd(t.as_ref()).map_err(|_| DeError::InvalidContent)?;
            Timestamp::parse(fmt, string).map_err(|_| DeError::InvalidContent)
        })
    }
}

impl fmt::Debug for Deserializer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Deserializer").finish_non_exhaustive()
    }
}

/// helper
const fn invalid_xml(err: quick_xml::Error) -> DeError {
    DeError::InvalidXml(err)
}

/// helper
const fn unexpected_eof() -> DeError {
    DeError::UnexpectedEof
}

/// helper
const fn unexpected_end() -> DeError {
    DeError::UnexpectedEnd
}

/// helper
const fn unexpected_tag_name() -> DeError {
    DeError::UnexpectedTagName
}

/// helper
const fn unexpected_start() -> DeError {
    DeError::UnexpectedStart
}

impl<'xml> DeserializeContent<'xml> for bool {
    fn deserialize_content(d: &mut Deserializer<'xml>) -> DeResult<Self> {
        d.text(|t| match t.as_ref() {
            b"true" | b"TRUE" => Ok(true),
            b"false" | b"FALSE" => Ok(false),
            _ => Err(DeError::InvalidContent),
        })
    }
}

impl<'xml> DeserializeContent<'xml> for String {
    fn deserialize_content(d: &mut Deserializer<'xml>) -> DeResult<Self> {
        d.text(|t| {
            let string = t.unescape().map_err(invalid_xml)?;
            Ok(string.into())
        })
    }
}

impl<'xml> DeserializeContent<'xml> for i32 {
    fn deserialize_content(d: &mut Deserializer<'xml>) -> DeResult<Self> {
        d.text(|t| atoi::atoi::<Self>(t.as_ref()).ok_or(DeError::InvalidContent))
    }
}

impl<'xml> DeserializeContent<'xml> for i64 {
    fn deserialize_content(d: &mut Deserializer<'xml>) -> DeResult<Self> {
        d.text(|t| atoi::atoi::<Self>(t.as_ref()).ok_or(DeError::InvalidContent))
    }
}

impl<'xml> DeserializeContent<'xml> for dto::Event {
    fn deserialize_content(d: &mut Deserializer<'xml>) -> DeResult<Self> {
        String::deserialize_content(d).map(Self::from)
    }
}
