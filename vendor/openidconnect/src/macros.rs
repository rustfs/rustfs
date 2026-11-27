/// Copied from oauth2-rs crate (not part of that crate's stable public interface).
macro_rules! new_type {
    // Convenience pattern without an impl.
    (
        $(#[$attr:meta])*
        $name:ident(
            $(#[$type_attr:meta])*
            $type:ty
        )
    ) => {
        new_type![
            @new_type_pub $(#[$attr])*,
            $name(
                $(#[$type_attr])*
                $type
            ),
            concat!(
                "Create a new `",
                stringify!($name),
                "` to wrap the given `",
                stringify!($type),
                "`."
            ),
            impl {}
        ];
    };
    // Convenience pattern without an impl.
    (
        $(#[$attr:meta])*
        pub(crate) $name:ident(
            $(#[$type_attr:meta])*
            $type:ty
        )
    ) => {
        new_type![
            @new_type_pub_crate $(#[$attr])*,
            $name(
                $(#[$type_attr])*
                $type
            ),
            concat!(
                "Create a new `",
                stringify!($name),
                "` to wrap the given `",
                stringify!($type),
                "`."
            ),
            impl {}
        ];
    };
    // Main entry point with an impl.
    (
        $(#[$attr:meta])*
        $name:ident(
            $(#[$type_attr:meta])*
            $type:ty
        )
        impl {
            $($item:tt)*
        }
    ) => {
        new_type![
            @new_type_pub $(#[$attr])*,
            $name(
                $(#[$type_attr])*
                $type
            ),
            concat!(
                "Create a new `",
                stringify!($name),
                "` to wrap the given `",
                stringify!($type),
                "`."
            ),
            impl {
                $($item)*
            }
        ];
    };
    // Main entry point with an impl.
    (
        $(#[$attr:meta])*
        pub(crate) $name:ident(
            $(#[$type_attr:meta])*
            $type:ty
        )
        impl {
            $($item:tt)*
        }
    ) => {
        new_type![
            @new_type_pub_crate $(#[$attr])*,
            $name(
                $(#[$type_attr])*
                $type
            ),
            concat!(
                "Create a new `",
                stringify!($name),
                "` to wrap the given `",
                stringify!($type),
                "`."
            ),
            impl {
                $($item)*
            }
        ];
    };
    // Actual implementation, after stringifying the #[doc] attr.
    (
        @new_type_pub $(#[$attr:meta])*,
        $name:ident(
            $(#[$type_attr:meta])*
            $type:ty
        ),
        $new_doc:expr,
        impl {
            $($item:tt)*
        }
    ) => {
        $(#[$attr])*
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $name(
            $(#[$type_attr])*
            $type
        );
        impl $name {
            $($item)*

            #[allow(dead_code)]
            #[doc = $new_doc]
            pub fn new(s: $type) -> Self {
                $name(s)
            }
        }
        impl std::ops::Deref for $name {
            type Target = $type;
            fn deref(&self) -> &$type {
                &self.0
            }
        }
        impl From<$name> for $type {
            fn from(t: $name) -> $type {
                t.0
            }
        }
    };
    // Actual implementation, after stringifying the #[doc] attr.
    (
        @new_type_pub_crate $(#[$attr:meta])*,
        $name:ident(
            $(#[$type_attr:meta])*
            $type:ty
        ),
        $new_doc:expr,
        impl {
            $($item:tt)*
        }
    ) => {
        $(#[$attr])*
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub(crate) struct $name(
            $(#[$type_attr])*
            $type
        );
        impl $name {
            $($item)*

            #[doc = $new_doc]
            pub const fn new(s: $type) -> Self {
                $name(s)
            }
        }
        impl std::ops::Deref for $name {
            type Target = $type;
            fn deref(&self) -> &$type {
                &self.0
            }
        }
        impl From<$name> for $type {
            fn from(t: $name) -> $type {
                t.0
            }
        }
    };
}

/// Copied from oauth2-rs crate (not part of that crate's stable public interface).
macro_rules! new_secret_type {
    (
        $(#[$attr:meta])*
        $name:ident($type:ty)
    ) => {
        new_secret_type![
            $(#[$attr])*
            $name($type)
            impl {}
        ];
    };
    (
        $(#[$attr:meta])*
        $name:ident($type:ty)
        impl {
            $($item:tt)*
        }
    ) => {
        new_secret_type![
            $(#[$attr])*,
            $name($type),
            concat!(
                "Create a new `",
                stringify!($name),
                "` to wrap the given `",
                stringify!($type),
                "`."
            ),
            concat!("Get the secret contained within this `", stringify!($name), "`."),
            impl {
                $($item)*
            }
        ];
    };
    (
        $(#[$attr:meta])*,
        $name:ident($type:ty),
        $new_doc:expr,
        $secret_doc:expr,
        impl {
            $($item:tt)*
        }
    ) => {
        $(
            #[$attr]
        )*
        #[cfg_attr(feature = "timing-resistant-secret-traits", derive(Eq))]
        pub struct $name($type);
        impl $name {
            $($item)*

            #[doc = $new_doc]
            pub fn new(s: $type) -> Self {
                $name(s)
            }

            #[doc = $secret_doc]
            ///
            /// # Security Warning
            ///
            /// Leaking this value may compromise the security of the OAuth2 flow.
            pub fn secret(&self) -> &$type { &self.0 }

            #[doc = $secret_doc]
            ///
            /// # Security Warning
            ///
            /// Leaking this value may compromise the security of the OAuth2 flow.
            pub fn into_secret(self) -> $type { self.0 }
        }
        impl Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
                write!(f, concat!(stringify!($name), "([redacted])"))
            }
        }

        #[cfg(any(test, feature = "timing-resistant-secret-traits"))]
        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                <sha2::Sha256 as sha2::Digest>::digest(&self.0)
                  == <sha2::Sha256 as sha2::Digest>::digest(&other.0)
            }
        }

        #[cfg(feature = "timing-resistant-secret-traits")]
        impl std::hash::Hash for $name {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                <sha2::Sha256 as sha2::Digest>::digest(&self.0).hash(state)
            }
        }

    };
}

/// Creates a URL-specific new type
///
/// Types created by this macro enforce during construction that the contained value represents a
/// syntactically valid URL. However, comparisons and hashes of these types are based on the string
/// representation given during construction, disregarding any canonicalization performed by the
/// underlying `Url` struct. OpenID Connect requires certain URLs (e.g., ID token issuers) to be
/// compared exactly, without canonicalization.
///
/// In addition to the raw string representation, these types include a `url` method to retrieve a
/// parsed `Url` struct.
macro_rules! new_url_type {
    // Convenience pattern without an impl.
    (
        $(#[$attr:meta])*
        $name:ident
    ) => {
        new_url_type![
            @new_type_pub $(#[$attr])*,
            $name,
            concat!("Create a new `", stringify!($name), "` from a `String` to wrap a URL."),
            concat!("Create a new `", stringify!($name), "` from a `Url` to wrap a URL."),
            concat!("Return this `", stringify!($name), "` as a parsed `Url`."),
            impl {}
        ];
    };
    // Main entry point with an impl.
    (
        $(#[$attr:meta])*
        $name:ident
        impl {
            $($item:tt)*
        }
    ) => {
        new_url_type![
            @new_type_pub $(#[$attr])*,
            $name,
            concat!("Create a new `", stringify!($name), "` from a `String` to wrap a URL."),
            concat!("Create a new `", stringify!($name), "` from a `Url` to wrap a URL."),
            concat!("Return this `", stringify!($name), "` as a parsed `Url`."),
            impl {
                $($item)*
            }
        ];
    };
    // Actual implementation, after stringifying the #[doc] attr.
    (
        @new_type_pub $(#[$attr:meta])*,
        $name:ident,
        $new_doc:expr,
        $from_url_doc:expr,
        $url_doc:expr,
        impl {
            $($item:tt)*
        }
    ) => {
        $(#[$attr])*
        #[derive(Clone)]
        pub struct $name(url::Url, String);
        impl $name {
            #[doc = $new_doc]
            pub fn new(url: String) -> Result<Self, ::url::ParseError> {
                Ok($name(url::Url::parse(&url)?, url))
            }
            #[doc = $from_url_doc]
            pub fn from_url(url: url::Url) -> Self {
                let s = url.to_string();
                Self(url, s)
            }
            #[doc = $url_doc]
            pub fn url(&self) -> &url::Url {
                return &self.0;
            }
            $($item)*
        }
        impl std::ops::Deref for $name {
            type Target = String;
            fn deref(&self) -> &String {
                &self.1
            }
        }
        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> Result<(), ::std::fmt::Error> {
                write!(f, "{}", self.1)
            }
        }
        impl From<$name> for url::Url {
            fn from(t: $name) -> url::Url {
                t.0
            }
        }
        impl ::std::fmt::Debug for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> Result<(), ::std::fmt::Error> {
                let mut debug_trait_builder = f.debug_tuple(stringify!($name));
                debug_trait_builder.field(&self.1);
                debug_trait_builder.finish()
            }
        }
        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::de::Deserializer<'de>,
            {
                struct UrlVisitor;
                impl<'de> ::serde::de::Visitor<'de> for UrlVisitor {
                    type Value = $name;

                    fn expecting(
                        &self,
                        formatter: &mut ::std::fmt::Formatter
                    ) -> ::std::fmt::Result {
                        formatter.write_str(stringify!($name))
                    }

                    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                    where
                        E: ::serde::de::Error,
                    {
                        $name::new(v.to_string()).map_err(E::custom)
                    }
                }
                deserializer.deserialize_str(UrlVisitor {})
            }
        }
        impl ::serde::Serialize for $name {
            fn serialize<SE>(&self, serializer: SE) -> Result<SE::Ok, SE::Error>
            where
                SE: ::serde::Serializer,
            {
                serializer.serialize_str(&self.1)
            }
        }
        impl ::std::hash::Hash for $name {
            fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) -> () {
                ::std::hash::Hash::hash(&(self.1), state);
            }
        }
        impl Ord for $name {
            fn cmp(&self, other: &$name) -> ::std::cmp::Ordering {
                self.1.cmp(&other.1)
            }
        }
        impl PartialOrd for $name {
            fn partial_cmp(&self, other: &$name) -> Option<::std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl PartialEq for $name {
            fn eq(&self, other: &$name) -> bool {
                self.1 == other.1
            }
        }
        impl Eq for $name {}
    };
}

macro_rules! serialize_fields {
    (@case $self:ident $map:ident Option(Seconds($field:ident))) => {
        if let Some(ref $field) = $self.$field {
            $map.serialize_entry(stringify!($field), &$field.as_secs())?;
        }
    };
    (@case $self:ident $map:ident Option(DateTime(Seconds($field:ident)))) => {
        if let Some(ref $field) = $self.$field {
            $map.serialize_entry(stringify!($field), &crate::helpers::Timestamp::from_utc(&$field))?;
        }
    };
    (@case $self:ident $map:ident Option($field:ident)) => {
        if let Some(ref $field) = $self.$field {
            $map.serialize_entry(stringify!($field), $field)?;
        }
    };
    (@case $self:ident $map:ident LanguageTag($field:ident)) => {
        if let Some(ref field_map) = $self.$field {
            use itertools::sorted;
            let sorted_field_map = sorted(field_map.iter());
            for (language_tag_opt, $field) in sorted_field_map {
                if let Some(ref language_tag) = language_tag_opt {
                    $map.serialize_entry(
                        &format!(concat!(stringify!($field), "#{}"), language_tag.as_ref()),
                        &$field
                    )?;
                } else {
                    $map.serialize_entry(stringify!($field), &$field)?;
                }
            }
        }
    };
    (@case $self:ident $map:ident $field:ident) => {
        $map.serialize_entry(stringify!($field), &$self.$field)?;
    };
    // Main entry point
    (
        $self:ident -> $serializer:ident {
            $([$($entry:tt)+])+
        }
    ) => {
        let mut map = $serializer.serialize_map(None)?;
        $(
            serialize_fields![@case $self map $($entry)+];
        )+
        map.end()
    };
}

macro_rules! field_getters {
    (@case [$doc:expr] $vis:vis $self:ident [$zero:expr] $field:ident Option < bool >) => {
        #[doc = $doc]
        $vis fn $field(&$self) -> Option<bool> {
            $zero.$field
        }
    };
    (@case [$doc:expr] $vis:vis $self:ident [$zero:expr] $field:ident Option < bool > { $($body:tt)+ }) => {
        #[doc = $doc]
        $vis fn $field(&$self) -> Option<bool> {
            $($body)+
        }
    };
    (@case [$doc:expr] $vis:vis $self:ident [$zero:expr] $field:ident Option < DateTime < Utc >>) => {
        #[doc = $doc]
        $vis fn $field(&$self) -> Option<DateTime<Utc>> {
            $zero.$field
        }
    };
    (@case [$doc:expr] $vis:vis $self:ident [$zero:expr] $field:ident Option < DateTime < Utc >> { $($body:tt)+ }) => {
        #[doc = $doc]
        $vis fn $field(&$self) -> Option<DateTime<Utc>> {
            $($body)+
        }
    };
    (@case [$doc:expr] $vis:vis $self:ident [$zero:expr] $field:ident Option < $type:ty >) => {
        #[doc = $doc]
        $vis fn $field(&$self) -> Option<&$type> {
            $zero.$field.as_ref()
        }
    };
    (@case [$doc:expr] $vis:vis $self:ident [$zero:expr] $field:ident Option < $type:ty > { $($body:tt)+ }) => {
        #[doc = $doc]
        $vis fn $field(&$self) -> Option<$type> {
            $($body)+
        }
    };
    (@case [$doc:expr] $vis:vis $self:ident [$zero:expr] $field:ident DateTime < Utc >) => {
        #[doc = $doc]
        $vis fn $field(&$self) -> DateTime<Utc> {
            $zero.$field
        }
    };
    (@case [$doc:expr] $vis:vis $self:ident [$zero:expr] $field:ident $type:ty) => {
        #[doc = $doc]
        $vis fn $field(&$self) -> &$type {
            &$zero.$field
        }
    };
    (@case [$doc:expr] $vis:vis $self:ident [$zero:expr] $field:ident $type:ty { $($body:tt)+ }) => {
        #[doc = $doc]
        $vis fn $field(&$self) -> $type {
            $($body)+
        }
    };
    (@case [$doc:expr] $self:ident [$zero:expr] $field:ident() Option < bool >) => {
        #[doc = $doc]
        fn $field(&$self) -> Option<bool> {
            $zero.$field()
        }
    };
    (@case [$doc:expr] $self:ident [$zero:expr] $field:ident() Option < bool > { $($body:tt)+ }) => {
        #[doc = $doc]
        fn $field(&$self) -> Option<bool> {
            $($body)+
        }
    };
    (@case [$doc:expr] $self:ident [$zero:expr] $field:ident() Option < DateTime < Utc >>) => {
        #[doc = $doc]
        fn $field(&$self) -> Option<DateTime<Utc>> {
            $zero.$field()
        }
    };
    (@case [$doc:expr] $self:ident [$zero:expr] $field:ident() Option < DateTime < Utc >> { $($body:tt)+ }) => {
        #[doc = $doc]
        fn $field(&$self) -> Option<DateTime<Utc>> {
            $($body)+
        }
    };
    (@case [$doc:expr] $self:ident [$zero:expr] $field:ident() Option < $type:ty >) => {
        #[doc = $doc]
        fn $field(&$self) -> Option<&$type> {
            $zero.$field()
        }
    };
    (@case [$doc:expr] $self:ident [$zero:expr] $field:ident() Option < $type:ty > { $($body:tt)+ }) => {
        #[doc = $doc]
        fn $field(&$self) -> Option<$type> {
            $($body)+
        }
    };
    (@case [$doc:expr] $self:ident [$zero:expr] $field:ident() DateTime < Utc >) => {
        #[doc = $doc]
        fn $field(&$self) -> DateTime<Utc> {
            $zero.$field()
        }
    };
    (@case [$doc:expr] $self:ident [$zero:expr] $field:ident() $type:ty) => {
        #[doc = $doc]
        fn $field(&$self) -> &$type {
            &$zero.$field()
        }
    };
    (@case [$doc:expr] $self:ident [$zero:expr] $field:ident() $type:ty { $($body:tt)+ }) => {
        #[doc = $doc]
        fn $field(&$self) -> $type {
            $($body)+
        }
    };
    // Main entry points
    (
        $vis:vis $self:ident [$zero:expr] [$doc:expr] {
            $(
                $field:ident[$($entry:tt)+] [$doc_field:expr],
            )+
        }
    ) => {
        $(
            field_getters![
                @case
                [concat!("Returns the `", $doc_field, "` ", $doc, ".")]
                $vis $self [$zero] $field $($entry)+
            ];
        )+
    };
    (
        $vis:vis $self:ident [$zero:expr]() [$doc:expr] {
            $(
                $field:ident[$($entry:tt)+] [$doc_field:expr],
            )+
        }
    ) => {
        $(
            field_getters![
                @case
                [concat!("Returns the `", $doc_field, "` ", $doc, ".")]
                $vis $self [$zero] $field() $($entry)+
            ];
        )+
    };
}

macro_rules! field_setters {
    (@case [$doc:expr] $vis:vis $self:ident [$zero:expr] $setter:ident $field:ident $type:ty [$doc_field:expr]) => {
        field_setters![
            @case2
            [concat!("Sets the `", $doc_field, "` ", $doc, ".")]
            $vis $self [$zero] $setter $field $type
        ];
    };
    (@case2 [$doc:expr] $vis:vis $self:ident [$zero:expr] $setter:ident $field:ident $type:ty) => {
        #[doc = $doc]
        $vis fn $setter(
            mut $self,
            $field: $type
        ) -> Self {
            $zero.$field = $field;
            $self
        }
    };
    // Main entry point
    (
        $vis:vis $self:ident [$zero:expr] [$doc:expr] {
            $setter:ident -> $field:ident[$($entry:tt)+] [$doc_field:expr]
        }
    ) => {
        field_setters![
            @case [$doc] $vis $self [$zero] $setter $field $($entry)+ [$doc_field]
        ];
    };
}

macro_rules! field_getters_setters {
    (
        @single $vis:vis $self:ident [$zero:expr] [$doc:expr]
        [$setter:ident -> $field:ident[$($entry:tt)+] [$field_doc:expr], $($rest:tt)*]
    ) => {
        field_getters![$vis $self [$zero] [$doc] { $field[$($entry)+] [$field_doc], }];
        field_setters![
            $vis $self [$zero] [$doc] { $setter -> $field[$($entry)+] [$field_doc] }
        ];
        field_getters_setters![@single $vis $self [$zero] [$doc] [$($rest)*]];
    };
    (
        @single $vis:vis $self:ident [$zero:expr]() [$doc:expr]
        [$setter:ident -> $field:ident[$($entry:tt)+] [$field_doc:expr], $($rest:tt)*]
    ) => {
        field_getters![$vis $self [$zero]() [$doc] { $field[$($entry)+] [$field_doc], }];
        field_setters![
            $vis $self [$zero] [$doc] { $setter -> $field[$($entry)+] [$field_doc] }
        ];
        field_getters_setters![@single $vis $self [$zero]() [$doc] [$($rest)*]];
    };
    (
        @single $vis:vis $self:ident [$zero:expr] [$doc:expr]
        [$setter:ident -> $field:ident[$($entry:tt)+], $($rest:tt)*]
    ) => {
        field_getters![$vis $self [$zero] [$doc] { $field[$($entry)+] [stringify!($field)], }];
        field_setters![
            $vis $self [$zero] [$doc] { $setter -> $field[$($entry)+] [stringify!($field)] }
        ];
        field_getters_setters![@single $vis $self [$zero] [$doc] [$($rest)*]];
    };
    (
        @single $vis:vis $self:ident [$zero:expr]() [$doc:expr]
        [$setter:ident -> $field:ident[$($entry:tt)+], $($rest:tt)*]
    ) => {
        field_getters![$vis $self [$zero]() [$doc] { $field[$($entry)+] [stringify!($field)], }];
        field_setters![
            $vis $self [$zero] [$doc] { $setter -> $field[$($entry)+] [stringify!($field)] }
        ];
        field_getters_setters![@single $vis $self [$zero]() [$doc] [$($rest)*]];
    };
    // Base case.
    (@single $vis:vis $self:ident [$zero:expr] [$doc:expr] []) => {};
    // Main entry points.
    (
        $vis:vis $self:ident [$zero:expr] [$doc:expr] {
            $setter:ident -> $field:ident[$($entry:tt)+] $($rest:tt)*
        }
    ) => {
        field_getters_setters![
            @single
            $vis $self [$zero] [$doc] [$setter -> $field[$($entry)+] $($rest)*]
        ];
    };
    (
        $vis:vis $self:ident [$zero:expr]() [$doc:expr] {
            $setter:ident -> $field:ident[$($entry:tt)+] $($rest:tt)*
        }
    ) => {
        field_getters_setters![
            @single
            $vis $self [$zero]() [$doc] [$setter -> $field[$($entry)+] $($rest)*]
        ];
    };
}

macro_rules! deserialize_from_str {
    ($type:path) => {
        impl<'de> serde::Deserialize<'de> for $type {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::de::Deserializer<'de>,
            {
                let variant_str = String::deserialize(deserializer)?;
                Ok(Self::from_str(&variant_str))
            }
        }
    };
}

macro_rules! serialize_as_str {
    ($type:path) => {
        impl serde::ser::Serialize for $type {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::ser::Serializer,
            {
                serializer.serialize_str(self.as_ref())
            }
        }
    };
}
