/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! JSON serializer implementation.

use aws_smithy_schema::serde::{SerdeError, SerializableStruct, ShapeSerializer};
use aws_smithy_schema::Schema;
use aws_smithy_types::date_time::Format as TimestampFormat;
use aws_smithy_types::{BigDecimal, BigInteger, DateTime, Document};

use crate::codec::JsonCodecSettings;

use std::sync::Arc;

/// JSON serializer that implements the ShapeSerializer trait.
pub struct JsonSerializer {
    output: String,
    settings: Arc<JsonCodecSettings>,
    // Tracks whether a comma is needed before the next value in the current container.
    needs_comma: bool,
    // When true, the next write_string is a map key — emit "key": instead of ,"key"
    expecting_map_key: bool,
    // Nesting depth of write_map calls. When >0, prefix() restores expecting_map_key
    // after each value write so the next write_string is treated as a key.
    map_depth: usize,
}

impl JsonSerializer {
    /// Creates a new JSON serializer with the given settings.
    pub(crate) fn new(settings: Arc<JsonCodecSettings>) -> Self {
        Self {
            output: String::new(),
            settings,
            needs_comma: false,
            expecting_map_key: false,
            map_depth: 0,
        }
    }

    /// Finalizes the serialization and returns the output as bytes.
    pub fn finish(self) -> Vec<u8> {
        self.output.into_bytes()
    }

    /// Handles comma separators and member names before writing a value.
    /// When inside a map (map_depth > 0), restores expecting_map_key after
    /// the value so the next write_string is treated as a map key.
    fn prefix(&mut self, schema: &Schema) {
        if self.needs_comma {
            self.output.push(',');
        }
        if let Some(name) = self.field_name(schema) {
            self.output.push('"');
            self.output.push_str(&crate::escape::escape_string(name));
            self.output.push_str("\":");
        }
        self.needs_comma = true;
        // Inside a map, after writing a value the next write_string should be a key.
        // This is safe because write_string checks expecting_map_key *before* calling
        // prefix(), so this only affects the *next* write_string call.
        if self.map_depth > 0 {
            self.expecting_map_key = true;
        }
    }

    /// Resolves the JSON field name for a member schema.
    fn field_name<'a>(&self, schema: &'a Schema) -> Option<&'a str> {
        self.settings.member_to_field(schema)
    }

    /// Gets the timestamp format to use, respecting @timestampFormat trait.
    fn get_timestamp_format(&self, schema: &Schema) -> TimestampFormat {
        if let Some(ts_trait) = schema.timestamp_format() {
            return match ts_trait.format() {
                aws_smithy_schema::traits::TimestampFormat::EpochSeconds => {
                    TimestampFormat::EpochSeconds
                }
                aws_smithy_schema::traits::TimestampFormat::HttpDate => TimestampFormat::HttpDate,
                aws_smithy_schema::traits::TimestampFormat::DateTime => TimestampFormat::DateTime,
            };
        }
        self.settings.default_timestamp_format
    }

    fn write_json_value(&mut self, doc: &Document) {
        use crate::serialize::JsonValueWriter;
        let writer = JsonValueWriter::new(&mut self.output);
        writer.document(doc);
    }
}

impl aws_smithy_schema::codec::FinishSerializer for JsonSerializer {
    fn finish(self) -> Vec<u8> {
        self.output.into_bytes()
    }
}

impl ShapeSerializer for JsonSerializer {
    fn write_struct(
        &mut self,
        schema: &Schema,
        value: &dyn SerializableStruct,
    ) -> Result<(), SerdeError> {
        self.prefix(schema);
        self.output.push('{');
        let saved_comma = self.needs_comma;
        let saved_depth = self.map_depth;
        let saved_map_key = self.expecting_map_key;
        self.needs_comma = false;
        // Reset map state so struct members don't trigger map-key logic.
        // Restored after the struct body so an enclosing map resumes correctly.
        self.map_depth = 0;
        self.expecting_map_key = false;
        value.serialize_members(self)?;
        self.output.push('}');
        self.needs_comma = saved_comma;
        self.map_depth = saved_depth;
        self.expecting_map_key = saved_map_key;
        Ok(())
    }

    fn write_list(
        &mut self,
        schema: &Schema,
        write_elements: &dyn Fn(&mut dyn ShapeSerializer) -> Result<(), SerdeError>,
    ) -> Result<(), SerdeError> {
        self.prefix(schema);
        self.output.push('[');
        let saved = self.needs_comma;
        let saved_depth = self.map_depth;
        let saved_map_key = self.expecting_map_key;
        self.needs_comma = false;
        // Reset map state so list elements don't trigger map-key logic in prefix().
        self.map_depth = 0;
        self.expecting_map_key = false;
        write_elements(self)?;
        self.output.push(']');
        self.needs_comma = saved;
        self.map_depth = saved_depth;
        self.expecting_map_key = saved_map_key;
        Ok(())
    }

    fn write_map(
        &mut self,
        schema: &Schema,
        write_entries: &dyn Fn(&mut dyn ShapeSerializer) -> Result<(), SerdeError>,
    ) -> Result<(), SerdeError> {
        self.prefix(schema);
        self.output.push('{');
        let saved_comma = self.needs_comma;
        let saved_map_key = self.expecting_map_key;
        let saved_depth = self.map_depth;
        self.needs_comma = false;
        self.expecting_map_key = true;
        // Increment depth so prefix() knows to restore expecting_map_key after
        // each value write. write_string checks expecting_map_key *before* calling
        // prefix(), so the flag only affects the *next* write_string (the next key).
        self.map_depth += 1;
        write_entries(self)?;
        self.map_depth = saved_depth;
        self.output.push('}');
        self.needs_comma = saved_comma;
        self.expecting_map_key = saved_map_key;
        Ok(())
    }

    fn write_boolean(&mut self, schema: &Schema, value: bool) -> Result<(), SerdeError> {
        self.prefix(schema);
        self.output.push_str(if value { "true" } else { "false" });
        Ok(())
    }

    fn write_byte(&mut self, schema: &Schema, value: i8) -> Result<(), SerdeError> {
        use std::fmt::Write;
        self.prefix(schema);
        write!(&mut self.output, "{}", value).map_err(|e| SerdeError::WriteFailed {
            message: e.to_string(),
        })
    }

    fn write_short(&mut self, schema: &Schema, value: i16) -> Result<(), SerdeError> {
        use std::fmt::Write;
        self.prefix(schema);
        write!(&mut self.output, "{}", value).map_err(|e| SerdeError::WriteFailed {
            message: e.to_string(),
        })
    }

    fn write_integer(&mut self, schema: &Schema, value: i32) -> Result<(), SerdeError> {
        use std::fmt::Write;
        self.prefix(schema);
        write!(&mut self.output, "{}", value).map_err(|e| SerdeError::WriteFailed {
            message: e.to_string(),
        })
    }

    fn write_long(&mut self, schema: &Schema, value: i64) -> Result<(), SerdeError> {
        use std::fmt::Write;
        self.prefix(schema);
        write!(&mut self.output, "{}", value).map_err(|e| SerdeError::WriteFailed {
            message: e.to_string(),
        })
    }

    fn write_float(&mut self, schema: &Schema, value: f32) -> Result<(), SerdeError> {
        use std::fmt::Write;
        self.prefix(schema);
        if value.is_nan() {
            self.output.push_str("\"NaN\"");
            Ok(())
        } else if value.is_infinite() {
            if value.is_sign_positive() {
                self.output.push_str("\"Infinity\"");
            } else {
                self.output.push_str("\"-Infinity\"");
            }
            Ok(())
        } else {
            write!(&mut self.output, "{}", value).map_err(|e| SerdeError::WriteFailed {
                message: e.to_string(),
            })
        }
    }

    fn write_double(&mut self, schema: &Schema, value: f64) -> Result<(), SerdeError> {
        use std::fmt::Write;
        self.prefix(schema);
        if value.is_nan() {
            self.output.push_str("\"NaN\"");
            Ok(())
        } else if value.is_infinite() {
            if value.is_sign_positive() {
                self.output.push_str("\"Infinity\"");
            } else {
                self.output.push_str("\"-Infinity\"");
            }
            Ok(())
        } else {
            write!(&mut self.output, "{}", value).map_err(|e| SerdeError::WriteFailed {
                message: e.to_string(),
            })
        }
    }

    fn write_big_integer(&mut self, schema: &Schema, value: &BigInteger) -> Result<(), SerdeError> {
        self.prefix(schema);
        self.output.push_str(value.as_ref());
        Ok(())
    }

    fn write_big_decimal(&mut self, schema: &Schema, value: &BigDecimal) -> Result<(), SerdeError> {
        self.prefix(schema);
        self.output.push_str(value.as_ref());
        Ok(())
    }

    fn write_string(&mut self, schema: &Schema, value: &str) -> Result<(), SerdeError> {
        use crate::escape::escape_string;
        if self.expecting_map_key {
            // Map key: comma before (if not first entry), then "key":
            if self.needs_comma {
                self.output.push(',');
            }
            self.output.push('"');
            self.output.push_str(&escape_string(value));
            self.output.push_str("\":");
            // The next write is the value — no comma before it
            self.needs_comma = false;
            self.expecting_map_key = false;
        } else {
            self.prefix(schema);
            self.output.push('"');
            self.output.push_str(&escape_string(value));
            self.output.push('"');
        }
        Ok(())
    }

    fn write_blob(&mut self, schema: &Schema, value: &[u8]) -> Result<(), SerdeError> {
        use aws_smithy_types::base64;
        self.prefix(schema);
        let encoded = base64::encode(value);
        self.output.push('"');
        self.output.push_str(&encoded);
        self.output.push('"');
        Ok(())
    }

    fn write_timestamp(&mut self, schema: &Schema, value: &DateTime) -> Result<(), SerdeError> {
        self.prefix(schema);
        let format = self.get_timestamp_format(schema);
        let formatted = value.fmt(format).map_err(|e| SerdeError::WriteFailed {
            message: format!("failed to format timestamp: {e}"),
        })?;

        match format {
            TimestampFormat::EpochSeconds => {
                // Epoch seconds as number
                self.output.push_str(&formatted);
            }
            _ => {
                // Other formats as strings
                self.output.push('"');
                self.output.push_str(&formatted);
                self.output.push('"');
            }
        }
        Ok(())
    }

    fn write_document(&mut self, schema: &Schema, value: &Document) -> Result<(), SerdeError> {
        self.prefix(schema);
        self.write_json_value(value);
        Ok(())
    }

    fn write_null(&mut self, schema: &Schema) -> Result<(), SerdeError> {
        self.prefix(schema);
        self.output.push_str("null");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_schema::prelude::*;
    use aws_smithy_schema::ShapeType;

    #[test]
    fn test_write_boolean() {
        let mut ser = JsonSerializer::new(Arc::new(JsonCodecSettings::default()));
        ser.write_boolean(&BOOLEAN, true).unwrap();
        let output = ser.finish();
        assert_eq!(String::from_utf8(output).unwrap(), "true");
    }

    #[test]
    fn test_write_string() {
        let mut ser = JsonSerializer::new(Arc::new(JsonCodecSettings::default()));
        ser.write_string(&STRING, "hello").unwrap();
        let output = ser.finish();
        assert_eq!(String::from_utf8(output).unwrap(), "\"hello\"");
    }

    #[test]
    fn test_write_integer() {
        let mut ser = JsonSerializer::new(Arc::new(JsonCodecSettings::default()));
        ser.write_integer(&INTEGER, 42).unwrap();
        let output = ser.finish();
        assert_eq!(String::from_utf8(output).unwrap(), "42");
    }

    #[test]
    fn test_write_null() {
        let mut ser = JsonSerializer::new(Arc::new(JsonCodecSettings::default()));
        ser.write_null(&STRING).unwrap();
        let output = ser.finish();
        assert_eq!(String::from_utf8(output).unwrap(), "null");
    }

    #[test]
    fn test_write_list() {
        let mut ser = JsonSerializer::new(Arc::new(JsonCodecSettings::default()));
        let list_schema = aws_smithy_schema::Schema::new(
            aws_smithy_schema::shape_id!("test", "List"),
            aws_smithy_schema::ShapeType::List,
        );
        ser.write_list(&list_schema, &|s: &mut dyn ShapeSerializer| {
            s.write_integer(&INTEGER, 1)?;
            s.write_integer(&INTEGER, 2)?;
            s.write_integer(&INTEGER, 3)?;
            Ok(())
        })
        .unwrap();
        let output = String::from_utf8(ser.finish()).unwrap();
        assert_eq!(output, "[1,2,3]");
    }

    #[test]
    fn test_write_full_object() {
        use aws_smithy_schema::serde::SerializableStruct;

        static ACTIVE_MEMBER: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Struct"),
            aws_smithy_schema::ShapeType::Boolean,
            "active",
            0,
        );
        static NAME_MEMBER: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Struct"),
            aws_smithy_schema::ShapeType::String,
            "name",
            1,
        );
        static COUNT_MEMBER: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Struct"),
            aws_smithy_schema::ShapeType::Integer,
            "count",
            2,
        );
        static PRICE_MEMBER: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Struct"),
            aws_smithy_schema::ShapeType::Float,
            "price",
            3,
        );
        static ITEMS_MEMBER: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Struct"),
            aws_smithy_schema::ShapeType::List,
            "items",
            4,
        );

        struct TestObject;
        impl SerializableStruct for TestObject {
            fn serialize_members(&self, s: &mut dyn ShapeSerializer) -> Result<(), SerdeError> {
                s.write_boolean(&ACTIVE_MEMBER, true)?;
                s.write_string(&NAME_MEMBER, "test")?;
                s.write_integer(&COUNT_MEMBER, 42)?;
                s.write_float(&PRICE_MEMBER, 3.15)?;
                s.write_list(&ITEMS_MEMBER, &|s| {
                    s.write_integer(&INTEGER, 1)?;
                    s.write_integer(&INTEGER, 2)?;
                    Ok(())
                })?;
                Ok(())
            }
        }

        let struct_schema = aws_smithy_schema::Schema::new(
            aws_smithy_schema::shape_id!("test", "Struct"),
            aws_smithy_schema::ShapeType::Structure,
        );
        let mut ser = JsonSerializer::new(Arc::new(JsonCodecSettings::default()));
        ser.write_struct(&struct_schema, &TestObject).unwrap();
        let output = String::from_utf8(ser.finish()).unwrap();
        assert_eq!(
            output,
            r#"{"active":true,"name":"test","count":42,"price":3.15,"items":[1,2]}"#
        );
    }

    #[test]
    fn test_nested_complex_serialization() {
        use aws_smithy_schema::serde::SerializableStruct;

        // Member schemas
        static ID: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "User"),
            aws_smithy_schema::ShapeType::Long,
            "id",
            0,
        );
        static NAME: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "User"),
            aws_smithy_schema::ShapeType::String,
            "name",
            1,
        );
        static SCORES: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "User"),
            aws_smithy_schema::ShapeType::List,
            "scores",
            2,
        );
        static ADDRESS: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "User"),
            aws_smithy_schema::ShapeType::Structure,
            "address",
            3,
        );
        static COMPANIES: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "User"),
            aws_smithy_schema::ShapeType::List,
            "companies",
            4,
        );
        static TAGS: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "User"),
            aws_smithy_schema::ShapeType::Map,
            "tags",
            5,
        );
        static STREET: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Address"),
            aws_smithy_schema::ShapeType::String,
            "street",
            0,
        );
        static CITY: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Address"),
            aws_smithy_schema::ShapeType::String,
            "city",
            1,
        );
        static ZIP: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Address"),
            aws_smithy_schema::ShapeType::Integer,
            "zip",
            2,
        );
        static COMP_NAME: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Company"),
            aws_smithy_schema::ShapeType::String,
            "name",
            0,
        );
        static EMPLOYEES: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Company"),
            aws_smithy_schema::ShapeType::List,
            "employees",
            1,
        );
        static METADATA: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Company"),
            aws_smithy_schema::ShapeType::Map,
            "metadata",
            2,
        );
        static ACTIVE: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Company"),
            aws_smithy_schema::ShapeType::Boolean,
            "active",
            3,
        );

        struct AddressStruct;
        impl SerializableStruct for AddressStruct {
            fn serialize_members(&self, s: &mut dyn ShapeSerializer) -> Result<(), SerdeError> {
                s.write_string(&STREET, "123 Main St")?;
                s.write_string(&CITY, "Seattle")?;
                s.write_integer(&ZIP, 98101)?;
                Ok(())
            }
        }

        struct CompanyStruct {
            name: &'static str,
            employees: &'static [&'static str],
            metadata: &'static [(&'static str, i32)],
            active: bool,
        }
        impl SerializableStruct for CompanyStruct {
            fn serialize_members(&self, s: &mut dyn ShapeSerializer) -> Result<(), SerdeError> {
                s.write_string(&COMP_NAME, self.name)?;
                s.write_list(&EMPLOYEES, &|s| {
                    for e in self.employees {
                        s.write_string(&STRING, e)?;
                    }
                    Ok(())
                })?;
                s.write_map(&METADATA, &|s| {
                    for (k, v) in self.metadata {
                        s.write_string(&::aws_smithy_schema::prelude::STRING, k)?;
                        s.write_integer(&::aws_smithy_schema::prelude::INTEGER, *v)?;
                    }
                    Ok(())
                })?;
                s.write_boolean(&ACTIVE, self.active)?;
                Ok(())
            }
        }

        struct UserStruct;
        impl SerializableStruct for UserStruct {
            fn serialize_members(&self, s: &mut dyn ShapeSerializer) -> Result<(), SerdeError> {
                s.write_long(&ID, 12345)?;
                s.write_string(&NAME, "John Doe")?;
                s.write_list(&SCORES, &|s| {
                    s.write_double(&DOUBLE, 95.5)?;
                    s.write_double(&DOUBLE, 87.3)?;
                    s.write_double(&DOUBLE, 92.1)?;
                    Ok(())
                })?;
                s.write_struct(&ADDRESS, &AddressStruct)?;
                s.write_list(&COMPANIES, &|s| {
                    let struct_schema = Schema::new(
                        aws_smithy_schema::shape_id!("test", "Company"),
                        aws_smithy_schema::ShapeType::Structure,
                    );
                    s.write_struct(
                        &struct_schema,
                        &CompanyStruct {
                            name: "TechCorp",
                            employees: &["Alice", "Bob"],
                            metadata: &[("founded", 2010), ("size", 500)],
                            active: true,
                        },
                    )?;
                    s.write_struct(
                        &struct_schema,
                        &CompanyStruct {
                            name: "StartupInc",
                            employees: &["Charlie"],
                            metadata: &[("founded", 2020)],
                            active: false,
                        },
                    )?;
                    Ok(())
                })?;
                s.write_map(&TAGS, &|s| {
                    s.write_string(&::aws_smithy_schema::prelude::STRING, "role")?;
                    s.write_string(&::aws_smithy_schema::prelude::STRING, "admin")?;
                    s.write_string(&::aws_smithy_schema::prelude::STRING, "level")?;
                    s.write_string(&::aws_smithy_schema::prelude::STRING, "senior")?;
                    Ok(())
                })?;
                Ok(())
            }
        }

        let struct_schema = Schema::new(
            aws_smithy_schema::shape_id!("test", "User"),
            aws_smithy_schema::ShapeType::Structure,
        );
        let mut ser = JsonSerializer::new(Arc::new(JsonCodecSettings::default()));
        ser.write_struct(&struct_schema, &UserStruct).unwrap();
        let output = ser.finish();
        // Expected compact JSON (br# avoids escape noise)
        let expected: &[u8] = br#"{"id":12345,"name":"John Doe","scores":[95.5,87.3,92.1],"address":{"street":"123 Main St","city":"Seattle","zip":98101},"companies":[{"name":"TechCorp","employees":["Alice","Bob"],"metadata":{"founded":2010,"size":500},"active":true},{"name":"StartupInc","employees":["Charlie"],"metadata":{"founded":2020},"active":false}],"tags":{"role":"admin","level":"senior"}}"#;
        assert_eq!(output, expected);
    }

    #[test]
    fn test_json_name_serialization() {
        use aws_smithy_schema::serde::SerializableStruct;

        static FOO_MEMBER: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "MyStruct"),
            aws_smithy_schema::ShapeType::String,
            "foo",
            0,
        );
        // bar has @jsonName("Baz")
        static BAR_MEMBER: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "MyStruct"),
            aws_smithy_schema::ShapeType::Integer,
            "bar",
            1,
        )
        .with_json_name("Baz");

        struct TestStruct;
        impl SerializableStruct for TestStruct {
            fn serialize_members(&self, s: &mut dyn ShapeSerializer) -> Result<(), SerdeError> {
                s.write_string(&FOO_MEMBER, "hello")?;
                s.write_integer(&BAR_MEMBER, 42)?;
                Ok(())
            }
        }

        // With use_json_name=true (default), "bar" should serialize as "Baz"
        let struct_schema = Schema::new(
            aws_smithy_schema::shape_id!("test", "MyStruct"),
            aws_smithy_schema::ShapeType::Structure,
        );
        let mut ser = JsonSerializer::new(Arc::new(JsonCodecSettings::default()));
        ser.write_struct(&struct_schema, &TestStruct).unwrap();
        let output = String::from_utf8(ser.finish()).unwrap();
        assert_eq!(output, r#"{"foo":"hello","Baz":42}"#);

        // With use_json_name=false, "bar" should stay as "bar"
        let mut ser = JsonSerializer::new(Arc::new(
            JsonCodecSettings::builder().use_json_name(false).build(),
        ));
        ser.write_struct(&struct_schema, &TestStruct).unwrap();
        let output = String::from_utf8(ser.finish()).unwrap();
        assert_eq!(output, r#"{"foo":"hello","bar":42}"#);
    }

    #[test]
    fn struct_inside_map_serializes_member_names_correctly() {
        // Regression test: when a struct is a map value, the map's expecting_map_key
        // flag must not leak into the struct's member serialization.
        use aws_smithy_schema::serde::{SerializableStruct, ShapeSerializer};

        static INNER_NAME: Schema = Schema::new_member(
            aws_smithy_schema::shape_id!("test", "Inner"),
            ShapeType::String,
            "name",
            0,
        );
        static INNER_MEMBERS: &[&Schema] = &[&INNER_NAME];
        static INNER_SCHEMA: Schema = Schema::new_struct(
            aws_smithy_schema::shape_id!("test", "Inner"),
            ShapeType::Structure,
            INNER_MEMBERS,
        );

        struct Inner;
        impl SerializableStruct for Inner {
            fn serialize_members(
                &self,
                ser: &mut dyn ShapeSerializer,
            ) -> Result<(), aws_smithy_schema::serde::SerdeError> {
                ser.write_string(&INNER_NAME, "Alice")
            }
        }

        static MAP_SCHEMA: Schema = Schema::new(
            aws_smithy_schema::shape_id!("test", "MyMap"),
            ShapeType::Map,
        );

        let mut ser = JsonSerializer::new(Arc::new(JsonCodecSettings::default()));
        ser.write_map(&MAP_SCHEMA, &|ser| {
            ser.write_string(&aws_smithy_schema::prelude::STRING, "key1")?;
            ser.write_struct(&INNER_SCHEMA, &Inner)?;
            Ok(())
        })
        .unwrap();
        let output = String::from_utf8(ser.finish()).unwrap();
        assert_eq!(output, r#"{"key1":{"name":"Alice"}}"#);
    }
}
