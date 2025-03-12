use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::config::{CsvOptions, JsonOptions};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;

use crate::data_source::data_source::TableHandle;

use super::TableHandleProvider;

pub enum FileType {
    Csv,
    Parquet,
    Json,
    Unknown,
}

#[derive(Default)]
pub struct BaseTableProvider {
    file_type: FileType,
}

impl BaseTableProvider {
    pub fn new(file_type: FileType) -> Self {
        Self { file_type }
    }
}

impl TableHandleProvider for BaseTableProvider {
    async fn build_table_handle(&self, session_state: &SessionState, table_name: &str) -> DFResult<TableHandle> {
        let table_path = ListingTableUrl::parse(table_name)?;
        let listing_options = match self.file_type {
            FileType::Csv => {
                let file_format = CsvFormat::default().with_options(CsvOptions::default().with_has_header(false));
                ListingOptions::new(Arc::new(file_format)).with_file_extension(".csv")
            }
            FileType::Parquet => {
                let file_format = ParquetFormat::new();
                ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet")
            }
            FileType::Json => {
                let file_format = JsonFormat::default();
                ListingOptions::new(Arc::new(file_format)).with_file_extension(".json")
            }
            FileType::Unknown => {
                return Err(DataFusionError::NotImplemented("not support this file type".to_string()));
            }
        };

        let resolve_schema = listing_options.infer_schema(session_state, &table_path).await?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(resolve_schema);
        let provider = Arc::new(ListingTable::try_new(config)?);

        Ok(TableHandle::External(provider))
    }
}
