fn main() -> shadow_rs::SdResult<()> {
    shadow_rs::ShadowBuilder::builder().build()?;
    Ok(())
}
