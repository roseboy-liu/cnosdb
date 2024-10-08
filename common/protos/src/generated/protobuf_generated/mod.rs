pub mod kv_service;
pub mod raft_service;
pub mod logproto;
pub mod jaeger_api_v2;
#[path = "jaeger.storage.v1.rs"]
pub mod jaeger_storage_v1;
#[path = "opentelemetry.proto.common.rs"]
pub mod common;
#[path = "opentelemetry.proto.resource.rs"]
pub mod resource;
#[path = "opentelemetry.proto.trace.rs"]
pub mod trace;
#[path = "opentelemetry.proto.collector.trace.rs"]
pub mod trace_service;
