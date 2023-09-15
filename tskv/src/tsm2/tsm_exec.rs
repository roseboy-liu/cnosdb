use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::datasource::physical_plan::parquet::page_filter::PagePruningPredicate;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetFileReaderFactory};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use trace::debug;

/// Execution plan for scanning one or more Tsm partitions
#[derive(Debug, Clone)]
pub struct TsmExec {
    /// Override for `Self::with_pushdown_filters`. If None, uses
    /// values from base_config
    pushdown_filters: Option<bool>,
    /// Override for `Self::with_reorder_filters`. If None, uses
    /// values from base_config
    reorder_filters: Option<bool>,
    /// Override for `Self::with_enable_page_index`. If None, uses
    /// values from base_config
    enable_page_index: Option<bool>,
    /// Base configuration for this scan
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<LexOrdering>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional predicate for row filtering during parquet scan
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Optional predicate for pruning row groups
    pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Optional predicate for pruning pages
    page_pruning_predicate: Option<Arc<PagePruningPredicate>>,
    /// Optional hint for the size of the parquet metadata
    metadata_size_hint: Option<usize>,
    /// Optional user defined parquet file reader factory
    parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
}

impl TsmExec {
    /// Create a new Parquet reader execution plan provided file list and schema.
    pub fn new(
        base_config: FileScanConfig,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metadata_size_hint: Option<usize>,
    ) -> Self {
        debug!(
            "Creating TsmExec, files: {:?}, projection {:?}, predicate: {:?}, limit: {:?}",
            base_config.file_groups, base_config.projection, predicate, base_config.limit
        );

        let metrics = ExecutionPlanMetricsSet::new();
        let predicate_creation_errors =
            MetricBuilder::new(&metrics).global_counter("num_predicate_creation_errors");

        let file_schema = &base_config.file_schema;
        let pruning_predicate = predicate
            .clone()
            .and_then(|predicate_expr| {
                match PruningPredicate::try_new(predicate_expr, file_schema.clone()) {
                    Ok(pruning_predicate) => Some(Arc::new(pruning_predicate)),
                    Err(e) => {
                        debug!("Could not create pruning predicate for: {e}");
                        predicate_creation_errors.add(1);
                        None
                    }
                }
            })
            .filter(|p| !p.allways_true());

        let page_pruning_predicate = predicate.as_ref().and_then(|predicate_expr| {
            match PagePruningPredicate::try_new(predicate_expr, file_schema.clone()) {
                Ok(pruning_predicate) => Some(Arc::new(pruning_predicate)),
                Err(e) => {
                    debug!(
                        "Could not create page pruning predicate for '{:?}': {}",
                        pruning_predicate, e
                    );
                    predicate_creation_errors.add(1);
                    None
                }
            }
        });

        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();

        Self {
            pushdown_filters: None,
            reorder_filters: None,
            enable_page_index: None,
            base_config,
            projected_schema,
            projected_statistics,
            projected_output_ordering,
            metrics,
            predicate,
            pruning_predicate,
            page_pruning_predicate,
            metadata_size_hint,
            parquet_file_reader_factory: None,
        }
    }
}
