//! AWS restXml serializer
//!
//! See <https://smithy.io/2.0/aws/protocols/aws-restxml-protocol.html#xml-shape-serialization>
//!

use crate::dto::{self, Timestamp, TimestampFormat};
use crate::utils::format::*;

use std::fmt;
use std::io::Write;

use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use quick_xml::writer::Writer;
use stdx::str::StrExt;

/// A data type that can be serialized with AWS restXml serializer
pub trait Serialize {
    /// Serializes the data type
    ///
    /// # Errors
    /// Returns an error if the serialization fails
    fn serialize<W: Write>(&self, s: &mut Serializer<W>) -> SerResult;
}

/// A data type that can be serialized with AWS restXml serializer
pub trait SerializeContent {
    /// Serializes the content of the data type
    ///
    /// # Errors
    /// Returns an error if the serialization fails
    fn serialize_content<W: Write>(&self, s: &mut Serializer<W>) -> SerResult;
}

/// AWS restXml serializer
pub struct Serializer<W: Write> {
    /// inner writer
    inner: Writer<W>,
}

/// XML serialization error
#[derive(Debug, thiserror::Error)]
#[error("XML serialization error: {inner}")]
pub struct SerError {
    /// inner error
    inner: std::io::Error,
}

/// XML serialization result
pub type SerResult<T = (), E = SerError> = std::result::Result<T, E>;

impl<W: Write> Serializer<W> {
    /// Creates a new serializer
    pub fn new(w: W) -> Self {
        Self { inner: Writer::new(w) }
    }

    /// Writes an event
    pub fn event(&mut self, event: Event<'_>) -> SerResult {
        self.inner.write_event(event).map_err(|err| SerError { inner: err })
    }

    /// Writes an element
    pub fn element(&mut self, name: &str, f: impl FnOnce(&mut Self) -> SerResult) -> SerResult {
        self.event(start(name))?;
        f(self)?;
        self.event(end(name))
    }

    pub fn element_with_ns(&mut self, name: &str, xmlns: &str, f: impl FnOnce(&mut Self) -> SerResult) -> SerResult {
        self.event(start_with_ns(name, xmlns))?;
        f(self)?;
        self.event(end(name))
    }

    /// Writes an element with attributes
    pub fn element_with_attrs(
        &mut self,
        name: &str,
        attrs: &[(&str, &str)],
        f: impl FnOnce(&mut Self) -> SerResult,
    ) -> SerResult {
        let mut start = BytesStart::new(name);
        for (key, value) in attrs {
            start.push_attribute((*key, *value));
        }
        self.event(Event::Start(start))?;
        f(self)?;
        self.event(end(name))
    }

    /// Serializes a type
    ///
    /// # Errors
    /// Returns an error if the underlying writer returns an error
    pub fn content<T: SerializeContent + ?Sized>(&mut self, name: &str, val: &T) -> SerResult {
        self.element(name, |s| val.serialize_content(s))
    }

    pub fn write_raw_text(&mut self, raw: &str) -> SerResult {
        self.inner
            .get_mut()
            .write_all(raw.as_bytes())
            .map_err(|e| SerError { inner: e })
    }

    pub fn content_with_ns<T: SerializeContent + ?Sized>(&mut self, name: &str, xmlns: &str, val: &T) -> SerResult {
        self.element_with_ns(name, xmlns, |s| val.serialize_content(s))
    }

    pub fn content_with_attrs<T: SerializeContent + ?Sized>(&mut self, name: &str, attrs: &[(&str, &str)], val: &T) -> SerResult {
        self.element_with_attrs(name, attrs, |s| val.serialize_content(s))
    }

    /// Serializes a flattened `list`
    ///
    /// # Errors
    /// Returns an error if the underlying writer returns an error
    pub fn flattened_list<'a, T: SerializeContent + ?Sized + 'a>(
        &mut self,
        name: &str,
        iter: impl IntoIterator<Item = &'a T>,
    ) -> SerResult {
        for member in iter {
            self.content(name, member)?;
        }
        Ok(())
    }

    /// Serializes a `list`
    ///
    /// # Errors
    /// Returns an error if the underlying writer returns an error
    pub fn list<'a, T: SerializeContent + ?Sized + 'a>(
        &mut self,
        name: &str,
        member_name: &str,
        iter: impl IntoIterator<Item = &'a T>,
    ) -> SerResult {
        self.element(name, |s| {
            for member in iter {
                s.content(member_name, member)?;
            }
            Ok(())
        })
    }

    /// Writes `<?xml version="1.0" encoding="UTF-8"?>` to the xml
    ///
    /// # Errors
    /// Returns an error if the underlying writer returns an error
    pub fn decl(&mut self) -> SerResult {
        self.event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))
    }

    #[allow(clippy::missing_panics_doc)]
    pub fn timestamp(&mut self, name: &str, val: &Timestamp, fmt: TimestampFormat) -> SerResult {
        fmt_timestamp(val, fmt, |b| self.content(name, str::from_ascii_simd(b).unwrap()))
    }
}

impl<W: Write> fmt::Debug for Serializer<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Serializer").finish_non_exhaustive()
    }
}

impl SerializeContent for bool {
    fn serialize_content<W: Write>(&self, s: &mut Serializer<W>) -> SerResult {
        s.event(text(fmt_boolean(*self)))
    }
}

impl SerializeContent for i32 {
    fn serialize_content<W: Write>(&self, s: &mut Serializer<W>) -> SerResult {
        fmt_integer(*self, |t| s.event(text(t)))
    }
}

impl SerializeContent for i64 {
    fn serialize_content<W: Write>(&self, s: &mut Serializer<W>) -> SerResult {
        fmt_long(*self, |t| s.event(text(t)))
    }
}

impl SerializeContent for str {
    fn serialize_content<W: Write>(&self, s: &mut Serializer<W>) -> SerResult {
        s.event(text(self))
    }
}

impl SerializeContent for &'_ str {
    fn serialize_content<W: Write>(&self, s: &mut Serializer<W>) -> SerResult {
        s.event(text(self))
    }
}

impl SerializeContent for String {
    fn serialize_content<W: Write>(&self, s: &mut Serializer<W>) -> SerResult {
        s.event(text(self.as_str()))
    }
}

impl SerializeContent for dto::Event {
    fn serialize_content<W: Write>(&self, s: &mut Serializer<W>) -> SerResult {
        self.as_ref().serialize_content(s)
    }
}

/// start event
fn start(name: &str) -> Event<'_> {
    Event::Start(BytesStart::new(name))
}

fn start_with_ns<'a>(name: &'a str, xmlns: &'a str) -> Event<'a> {
    let mut e = BytesStart::new(name);
    e.push_attribute(("xmlns", xmlns));
    Event::Start(e)
}

/// end event
fn end(name: &str) -> Event<'_> {
    Event::End(BytesEnd::new(name))
}

/// text event
fn text(content: &str) -> Event<'_> {
    Event::Text(BytesText::new(content))
}
