use shadow_rs::shadow;
use std::process::Command;

shadow!(build);

type VersionParseResult = Result<(u32, u32, u32, Option<String>), Box<dyn std::error::Error>>;

#[allow(clippy::const_is_empty)]
pub fn get_version() -> String {
    // 获取最新的 tag
    if let Ok(latest_tag) = get_latest_tag() {
        // 检查当前 commit 是否比最新 tag 更新
        if is_head_newer_than_tag(&latest_tag) {
            // 如果当前 commit 更新，则提升版本号
            if let Ok(new_version) = increment_version(&latest_tag) {
                return format!("refs/tags/{new_version}");
            }
        }

        // 如果当前 commit 就是最新 tag，或者版本提升失败，返回当前 tag
        return format!("refs/tags/{latest_tag}");
    }

    // 如果没有 tag，使用原来的逻辑
    if !build::TAG.is_empty() {
        format!("refs/tags/{}", build::TAG)
    } else if !build::SHORT_COMMIT.is_empty() {
        format!("@{}", build::SHORT_COMMIT)
    } else {
        format!("refs/tags/{}", build::PKG_VERSION)
    }
}

/// 获取最新的 git tag
fn get_latest_tag() -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("git").args(["describe", "--tags", "--abbrev=0"]).output()?;

    if output.status.success() {
        let tag = String::from_utf8(output.stdout)?;
        Ok(tag.trim().to_string())
    } else {
        Err("Failed to get latest tag".into())
    }
}

/// 检查当前 HEAD 是否比指定的 tag 更新
fn is_head_newer_than_tag(tag: &str) -> bool {
    let output = Command::new("git")
        .args(["merge-base", "--is-ancestor", tag, "HEAD"])
        .output();

    match output {
        Ok(result) => result.status.success(),
        Err(_) => false,
    }
}

/// 提升版本号（增加 patch 版本）
fn increment_version(version: &str) -> Result<String, Box<dyn std::error::Error>> {
    // 解析版本号，例如 "1.0.0-alpha.19" -> (1, 0, 0, Some("alpha.19"))
    let (major, minor, patch, pre_release) = parse_version(version)?;

    // 如果有预发布标识符，则增加预发布版本号
    if let Some(pre) = pre_release {
        if let Some(new_pre) = increment_pre_release(&pre) {
            return Ok(format!("{major}.{minor}.{patch}-{new_pre}"));
        }
    }

    // 否则增加 patch 版本号
    Ok(format!("{major}.{minor}.{}", patch + 1))
}

/// 解析版本号
pub fn parse_version(version: &str) -> VersionParseResult {
    let parts: Vec<&str> = version.split('-').collect();
    let base_version = parts[0];
    let pre_release = if parts.len() > 1 { Some(parts[1..].join("-")) } else { None };

    let version_parts: Vec<&str> = base_version.split('.').collect();
    if version_parts.len() < 3 {
        return Err("Invalid version format".into());
    }

    let major: u32 = version_parts[0].parse()?;
    let minor: u32 = version_parts[1].parse()?;
    let patch: u32 = version_parts[2].parse()?;

    Ok((major, minor, patch, pre_release))
}

/// 增加预发布版本号
fn increment_pre_release(pre_release: &str) -> Option<String> {
    // 处理形如 "alpha.19" 的预发布版本
    let parts: Vec<&str> = pre_release.split('.').collect();
    if parts.len() == 2 {
        if let Ok(num) = parts[1].parse::<u32>() {
            return Some(format!("{}.{}", parts[0], num + 1));
        }
    }

    // 处理形如 "alpha19" 的预发布版本
    if let Some(pos) = pre_release.rfind(|c: char| c.is_alphabetic()) {
        let prefix = &pre_release[..=pos];
        let suffix = &pre_release[pos + 1..];
        if let Ok(num) = suffix.parse::<u32>() {
            return Some(format!("{prefix}{}", num + 1));
        }
    }

    None
}

/// Clean version string - removes common prefixes
pub fn clean_version(version: &str) -> String {
    version
        .trim()
        .trim_start_matches("refs/tags/")
        .trim_start_matches('v')
        .trim_start_matches("RELEASE.")
        .trim_start_matches('@')
        .to_string()
}

/// Compare two versions to determine if the latest is newer
pub fn is_newer_version(current: &str, latest: &str) -> Result<bool, Box<dyn std::error::Error>> {
    // Clean version numbers, remove prefixes like "v", "RELEASE.", etc.
    let current_clean = clean_version(current);
    let latest_clean = clean_version(latest);

    // If versions are the same, no update is needed
    if current_clean == latest_clean {
        return Ok(false);
    }

    // Try semantic version comparison using parse_version
    match (parse_version(&current_clean), parse_version(&latest_clean)) {
        (Ok(current_parts), Ok(latest_parts)) => Ok(compare_version_parts(&current_parts, &latest_parts)),
        (Err(_), _) | (_, Err(_)) => {
            // If semantic version comparison fails, use string comparison
            Ok(latest_clean > current_clean)
        }
    }
}

/// Compare two version parts tuples (major, minor, patch, pre_release)
fn compare_version_parts(current: &(u32, u32, u32, Option<String>), latest: &(u32, u32, u32, Option<String>)) -> bool {
    let (cur_major, cur_minor, cur_patch, cur_pre) = current;
    let (lat_major, lat_minor, lat_patch, lat_pre) = latest;

    // Compare major version
    if lat_major != cur_major {
        return lat_major > cur_major;
    }

    // Compare minor version
    if lat_minor != cur_minor {
        return lat_minor > cur_minor;
    }

    // Compare patch version
    if lat_patch != cur_patch {
        return lat_patch > cur_patch;
    }

    // Compare pre-release versions
    match (cur_pre, lat_pre) {
        (None, None) => false,    // Same version
        (Some(_), None) => true,  // Pre-release < release
        (None, Some(_)) => false, // Release > pre-release
        (Some(cur_pre), Some(lat_pre)) => {
            // Both are pre-release, compare them
            compare_pre_release(cur_pre, lat_pre)
        }
    }
}

/// Compare pre-release versions
fn compare_pre_release(current: &str, latest: &str) -> bool {
    // Split by dots and compare each part
    let current_parts: Vec<&str> = current.split('.').collect();
    let latest_parts: Vec<&str> = latest.split('.').collect();

    for (cur_part, lat_part) in current_parts.iter().zip(latest_parts.iter()) {
        // Try to parse as numbers first
        match (cur_part.parse::<u32>(), lat_part.parse::<u32>()) {
            (Ok(cur_num), Ok(lat_num)) => {
                if cur_num != lat_num {
                    return lat_num > cur_num;
                }
            }
            _ => {
                // If not numbers, compare as strings
                if cur_part != lat_part {
                    return lat_part > cur_part;
                }
            }
        }
    }

    // If all compared parts are equal, longer version is newer
    latest_parts.len() > current_parts.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_version() {
        // 测试标准版本解析
        let (major, minor, patch, pre_release) = parse_version("1.0.0").unwrap();
        assert_eq!(major, 1);
        assert_eq!(minor, 0);
        assert_eq!(patch, 0);
        assert_eq!(pre_release, None);

        // 测试预发布版本解析
        let (major, minor, patch, pre_release) = parse_version("1.0.0-alpha.19").unwrap();
        assert_eq!(major, 1);
        assert_eq!(minor, 0);
        assert_eq!(patch, 0);
        assert_eq!(pre_release, Some("alpha.19".to_string()));
    }

    #[test]
    fn test_increment_pre_release() {
        // 测试 alpha.19 -> alpha.20
        assert_eq!(increment_pre_release("alpha.19"), Some("alpha.20".to_string()));

        // 测试 beta.5 -> beta.6
        assert_eq!(increment_pre_release("beta.5"), Some("beta.6".to_string()));

        // 测试无法解析的情况
        assert_eq!(increment_pre_release("unknown"), None);
    }

    #[test]
    fn test_increment_version() {
        // 测试预发布版本递增
        assert_eq!(increment_version("1.0.0-alpha.19").unwrap(), "1.0.0-alpha.20");

        // 测试标准版本递增
        assert_eq!(increment_version("1.0.0").unwrap(), "1.0.1");
    }

    #[test]
    fn test_version_format() {
        // 测试版本格式是否以 refs/tags/ 开头
        let version = get_version();
        assert!(version.starts_with("refs/tags/") || version.starts_with("@"));

        // 如果是 refs/tags/ 格式，应该包含版本号
        if let Some(version_part) = version.strip_prefix("refs/tags/") {
            assert!(!version_part.is_empty());
        }
    }

    #[test]
    fn test_current_version_output() {
        // 显示当前版本输出
        let version = get_version();
        println!("Current version: {version}");

        // 验证版本格式
        assert!(version.starts_with("refs/tags/") || version.starts_with("@"));

        // 如果是 refs/tags/ 格式，验证版本号不为空
        if let Some(version_part) = version.strip_prefix("refs/tags/") {
            assert!(!version_part.is_empty());
            println!("Version part: {version_part}");
        }
    }

    #[test]
    fn test_clean_version() {
        assert_eq!(clean_version("v1.0.0"), "1.0.0");
        assert_eq!(clean_version("RELEASE.1.0.0"), "1.0.0");
        assert_eq!(clean_version("@1.0.0"), "1.0.0");
        assert_eq!(clean_version("1.0.0"), "1.0.0");
        assert_eq!(clean_version("refs/tags/1.0.0-alpha.17"), "1.0.0-alpha.17");
        assert_eq!(clean_version("refs/tags/v1.0.0"), "1.0.0");
    }

    #[test]
    fn test_is_newer_version() {
        // Test semantic version comparison
        assert!(is_newer_version("1.0.0", "1.0.1").unwrap());
        assert!(is_newer_version("1.0.0", "1.1.0").unwrap());
        assert!(is_newer_version("1.0.0", "2.0.0").unwrap());
        assert!(!is_newer_version("1.0.1", "1.0.0").unwrap());
        assert!(!is_newer_version("1.0.0", "1.0.0").unwrap());

        // Test version comparison with pre-release identifiers
        assert!(is_newer_version("1.0.0-alpha.1", "1.0.0-alpha.2").unwrap());
        assert!(is_newer_version("1.0.0-alpha.17", "1.0.1").unwrap());
        assert!(is_newer_version("refs/tags/1.0.0-alpha.16", "refs/tags/1.0.0-alpha.17").unwrap());
        assert!(!is_newer_version("refs/tags/1.0.0-alpha.17", "refs/tags/1.0.0-alpha.16").unwrap());

        // Test pre-release vs release comparison
        assert!(is_newer_version("1.0.0-alpha.1", "1.0.0").unwrap());
        assert!(is_newer_version("1.0.0-beta.1", "1.0.0").unwrap());
        assert!(!is_newer_version("1.0.0", "1.0.0-alpha.1").unwrap());
        assert!(!is_newer_version("1.0.0", "1.0.0-beta.1").unwrap());

        // Test pre-release version ordering
        assert!(is_newer_version("1.0.0-alpha.1", "1.0.0-alpha.2").unwrap());
        assert!(is_newer_version("1.0.0-alpha.19", "1.0.0-alpha.20").unwrap());
        assert!(is_newer_version("1.0.0-alpha.1", "1.0.0-beta.1").unwrap());
        assert!(is_newer_version("1.0.0-beta.1", "1.0.0-rc.1").unwrap());

        // Test complex pre-release versions
        assert!(is_newer_version("1.0.0-alpha.1.2", "1.0.0-alpha.1.3").unwrap());
        assert!(is_newer_version("1.0.0-alpha.1", "1.0.0-alpha.1.1").unwrap());
        assert!(!is_newer_version("1.0.0-alpha.1.3", "1.0.0-alpha.1.2").unwrap());
    }

    #[test]
    fn test_compare_version_parts() {
        // Test basic version comparison
        assert!(compare_version_parts(&(1, 0, 0, None), &(1, 0, 1, None)));
        assert!(compare_version_parts(&(1, 0, 0, None), &(1, 1, 0, None)));
        assert!(compare_version_parts(&(1, 0, 0, None), &(2, 0, 0, None)));
        assert!(!compare_version_parts(&(1, 0, 1, None), &(1, 0, 0, None)));

        // Test pre-release vs release
        assert!(compare_version_parts(&(1, 0, 0, Some("alpha.1".to_string())), &(1, 0, 0, None)));
        assert!(!compare_version_parts(&(1, 0, 0, None), &(1, 0, 0, Some("alpha.1".to_string()))));

        // Test pre-release comparison
        assert!(compare_version_parts(
            &(1, 0, 0, Some("alpha.1".to_string())),
            &(1, 0, 0, Some("alpha.2".to_string()))
        ));
        assert!(compare_version_parts(
            &(1, 0, 0, Some("alpha.19".to_string())),
            &(1, 0, 0, Some("alpha.20".to_string()))
        ));
        assert!(compare_version_parts(
            &(1, 0, 0, Some("alpha.1".to_string())),
            &(1, 0, 0, Some("beta.1".to_string()))
        ));
    }

    #[test]
    fn test_compare_pre_release() {
        // Test numeric pre-release comparison
        assert!(compare_pre_release("alpha.1", "alpha.2"));
        assert!(compare_pre_release("alpha.19", "alpha.20"));
        assert!(!compare_pre_release("alpha.2", "alpha.1"));

        // Test string pre-release comparison
        assert!(compare_pre_release("alpha.1", "beta.1"));
        assert!(compare_pre_release("beta.1", "rc.1"));
        assert!(!compare_pre_release("beta.1", "alpha.1"));

        // Test complex pre-release comparison
        assert!(compare_pre_release("alpha.1.2", "alpha.1.3"));
        assert!(compare_pre_release("alpha.1", "alpha.1.1"));
        assert!(!compare_pre_release("alpha.1.3", "alpha.1.2"));
    }
}
