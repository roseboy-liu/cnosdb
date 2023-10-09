use std::collections::BTreeMap;
use models::SeriesId;
use crate::tsm2::page::Page;

pub(crate) mod page;
mod reader;
mod scan_config;
mod statistics;
mod tsm_exec;
mod types;
pub mod writer;

pub(crate) const MAX_BLOCK_VALUES: u32 = 1000;

const HEADER_SIZE: usize = 5;
const INDEX_META_SIZE: usize = 11;
const BLOCK_META_SIZE: usize = 44;
const BLOOM_FILTER_SIZE: usize = 64;
const BLOOM_FILTER_BITS: u64 = 512; // 64 * 8
const FOOTER_SIZE: usize = BLOOM_FILTER_SIZE + 8; // 72

pub type TsmWriteData = BTreeMap<String, BTreeMap<SeriesId, Vec<Page>>>; // (table, (series_id, pages))
