use std::ops::Deref;

use ecstore::store::ECStore;

#[derive(Clone)]
pub struct ObjectApi(Option<ECStore>);

impl Deref for ObjectApi {
    type Target = Option<ECStore>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ObjectApi {
    pub fn new(t: Option<ECStore>) -> Self {
        Self(t)
    }
}
