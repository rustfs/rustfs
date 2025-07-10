use shadow_rs::shadow;
shadow!(build);

#[allow(clippy::const_is_empty)]
pub fn get_version() -> String {
    if !build::TAG.is_empty() {
        build::TAG.to_string()
    } else if !build::SHORT_COMMIT.is_empty() {
        format!("@{}", build::SHORT_COMMIT)
    } else {
        build::PKG_VERSION.to_string()
    }
}
