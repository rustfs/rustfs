#[derive(Clone, Debug, Default)]
pub struct LockArgs {
    pub uid: String,
    pub resources: Vec<String>,
    pub owner: String,
    pub source: String,
    pub quorum: usize,
}
