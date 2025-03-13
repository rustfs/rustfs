use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::datasource::listing::ListingTable;

use crate::data_source::data_source::TableHandle;

use super::TableHandleProvider;

#[derive(Default)]
pub struct BaseTableProvider {}

impl TableHandleProvider for BaseTableProvider {
    fn build_table_handle(&self, provider: Arc<ListingTable>) -> DFResult<TableHandle> {
        Ok(TableHandle::External(provider))
    }
}
