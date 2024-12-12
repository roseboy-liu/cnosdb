use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use models::arrow::stream::{BoxStream, ParallelMergeStream};
use models::meta_data::VnodeInfo;
use tokio::runtime::Runtime;
use trace::span_ext::SpanExt;
use trace::{Span, SpanContext};

use super::table_scan::LocalTskvTableScanStream;
use super::tag_scan::LocalTskvTagScanStream;
use crate::error::TskvResult;
use crate::reader::{QueryOption, SendableTskvRecordBatchStream};
use crate::EngineRef;

pub struct QueryExecutor {
    option: QueryOption,
    runtime: Arc<Runtime>,
    kv_inst: EngineRef,
}

impl QueryExecutor {
    pub fn new(option: QueryOption, runtime: Arc<Runtime>, kv_inst: EngineRef) -> Self {
        Self {
            option,
            runtime,
            kv_inst,
        }
    }

    pub fn local_node_executor(
        &self,
        vnodes: Vec<VnodeInfo>,
        span_context: Option<&SpanContext>,
    ) -> TskvResult<SendableTskvRecordBatchStream> {
        let mut streams: Vec<BoxStream<TskvResult<RecordBatch>>> = Vec::with_capacity(vnodes.len());

        vnodes.into_iter().for_each(|vnode| {
            let input = Box::pin(LocalTskvTableScanStream::new(
                vnode.id,
                self.option.clone(),
                self.kv_inst.clone(),
                self.runtime.clone(),
                Span::from_context(
                    format!("LocalTskvTableScanStream ({})", vnode.id),
                    span_context,
                ),
            ));

            streams.push(input);
        });

        let parallel_merge_stream = ParallelMergeStream::new(Some(self.runtime.clone()), streams);

        Ok(Box::pin(parallel_merge_stream))
    }

    pub fn local_node_tag_scan(
        &self,
        vnodes: Vec<VnodeInfo>,
        span_context: Option<&SpanContext>,
    ) -> TskvResult<SendableTskvRecordBatchStream> {
        let mut streams = Vec::with_capacity(vnodes.len());
        vnodes.into_iter().for_each(|vnode| {
            let stream = LocalTskvTagScanStream::new(
                vnode.id,
                self.option.clone(),
                self.kv_inst.clone(),
                Span::from_context(
                    format!("LocalTskvTagScanStream ({})", vnode.id),
                    span_context,
                ),
            );
            streams.push(Box::pin(stream) as SendableTskvRecordBatchStream);
        });

        let parallel_merge_stream = ParallelMergeStream::new(Some(self.runtime.clone()), streams);

        Ok(Box::pin(parallel_merge_stream))
    }
}
