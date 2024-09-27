use rule::Rule;

mod and;
mod filter;
mod rule;
mod tag;

#[derive(Debug)]
pub struct Config {
    rules: Vec<Rule>,
    role_arn: String,
}
