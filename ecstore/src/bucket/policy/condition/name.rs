// 定义Name枚举类型
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Name {
    StringEquals,
    StringNotEquals,
    StringEqualsIgnoreCase,
    StringNotEqualsIgnoreCase,
    StringLike,
    StringNotLike,
    BinaryEquals,
    IpAddress,
    NotIpAddress,
    Null,
    Bool,
    NumericEquals,
    NumericNotEquals,
    NumericLessThan,
    NumericLessThanEquals,
    NumericGreaterThan,
    NumericGreaterThanIfExists,
    NumericGreaterThanEquals,
    DateEquals,
    DateNotEquals,
    DateLessThan,
    DateLessThanEquals,
    DateGreaterThan,
    DateGreaterThanEquals,
    ForAllValues,
    ForAnyValue,
}

impl Name {
    pub fn as_str(&self) -> &'static str {
        match self {
            Name::StringEquals => "StringEquals",
            Name::StringNotEquals => "StringNotEquals",
            Name::StringEqualsIgnoreCase => "StringEqualsIgnoreCase",
            Name::StringNotEqualsIgnoreCase => "StringNotEqualsIgnoreCase",
            Name::StringLike => "StringLike",
            Name::StringNotLike => "StringNotLike",
            Name::BinaryEquals => "BinaryEquals",
            Name::IpAddress => "IpAddress",
            Name::NotIpAddress => "NotIpAddress",
            Name::Null => "Null",
            Name::Bool => "Bool",
            Name::NumericEquals => "NumericEquals",
            Name::NumericNotEquals => "NumericNotEquals",
            Name::NumericLessThan => "NumericLessThan",
            Name::NumericLessThanEquals => "NumericLessThanEquals",
            Name::NumericGreaterThan => "NumericGreaterThan",
            Name::NumericGreaterThanIfExists => "NumericGreaterThanIfExists",
            Name::NumericGreaterThanEquals => "NumericGreaterThanEquals",
            Name::DateEquals => "DateEquals",
            Name::DateNotEquals => "DateNotEquals",
            Name::DateLessThan => "DateLessThan",
            Name::DateLessThanEquals => "DateLessThanEquals",
            Name::DateGreaterThan => "DateGreaterThan",
            Name::DateGreaterThanEquals => "DateGreaterThanEquals",
            Name::ForAllValues => "ForAllValues",
            Name::ForAnyValue => "ForAnyValue",
        }
    }
}

// impl ToString for Name {
//     fn to_string(&self) -> String {
//         self.as_str().to_string()
//     }
// }

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
