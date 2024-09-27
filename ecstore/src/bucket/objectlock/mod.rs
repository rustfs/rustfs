#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RetMode {
    Govenance,
    Compliance,
}

// 为RetMode实现FromStr trait，方便从字符串创建枚举实例
impl std::str::FromStr for RetMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "GOVERNANCE" => Ok(RetMode::Govenance),
            "COMPLIANCE" => Ok(RetMode::Compliance),
            _ => Err(format!("Invalid RetMode: {}", s)),
        }
    }
}

#[derive(Debug)]
pub struct DefaultRetention {
    pub mode: RetMode,
    pub days: Option<usize>,
    pub years: Option<usize>,
}

#[derive(Debug)]
pub struct Rule {
    pub default_retention: DefaultRetention,
}

#[derive(Debug)]
pub struct Config {
    pub object_lock_enabled: String,
    pub rule: Option<Rule>,
}
