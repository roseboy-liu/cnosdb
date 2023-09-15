use models::predicate::domain::TimeRange;
use models::schema::TableColumn;
use models::ValueType;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{DeserializeSnafu, Result, SerializeSnafu};

pub struct Page {
    pub(crate) bytes: bytes::Bytes,
    pub(crate) meta: PageMeta,
}

#[derive(Default, Serialize, Deserialize)]
pub struct PageMeta {
    pub(crate) num_values: u32,
    pub(crate) column: Option<TableColumn>,
    pub(crate) time_range: Option<TimeRange>,
    pub(crate) statistic: Option<PageStatistics>,
}

#[derive(Serialize, Deserialize)]
pub struct PageStatistics {
    pub(crate) primitive_type: ValueType,
    pub(crate) null_count: Option<i64>,
    pub(crate) distinct_count: Option<i64>,
    pub(crate) max_value: Option<Vec<u8>>,
    pub(crate) min_value: Option<Vec<u8>>,
}

#[derive(Default, Serialize, Deserialize)]
pub struct PageWriteSpec {
    pub(crate) offset: u64,
    pub(crate) size: usize,
    pub(crate) meta: PageMeta,
}

/// A chunk of data for a series at least two columns
#[derive(Default, Serialize, Deserialize)]
pub struct Chunk {
    pages: Vec<PageWriteSpec>,
}

impl Chunk {
    pub fn new() -> Self {
        Self { pages: Vec::new() }
    }
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self).context(SerializeSnafu)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context(DeserializeSnafu)
    }
    pub fn push(&mut self, page: PageWriteSpec) {
        self.pages.push(page);
    }
    pub fn time_range(&self) -> TimeRange {
        let mut time_range = TimeRange::none();
        for page in self.pages.iter() {
            time_range.merge(&page.meta.time_range);
        }
        time_range
    }
}

#[derive(Serialize, Deserialize)]
pub struct ChunkWriteSpec {
    pub(crate) series: String,
    pub(crate) chunk_offset: u64,
    pub(crate) chunk_size: usize,
    pub(crate) statics: ChunkStatics,
}

/// ChunkStatics
#[derive(Serialize, Deserialize)]
pub struct ChunkStatics {
    pub(crate) time_range: TimeRange,
}

/// A group of chunks for a table
#[derive(Default, Serialize, Deserialize)]
pub struct ChunkGroup {
    pub(crate) chunks: Vec<ChunkWriteSpec>,
}

impl ChunkGroup {
    pub fn new() -> Self {
        Self { chunks: Vec::new() }
    }
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self).context(SerializeSnafu)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context(DeserializeSnafu)
    }

    pub fn push(&mut self, chunk: ChunkWriteSpec) {
        self.chunks.push(chunk);
    }

    pub fn len(&self) -> usize {
        self.chunks.len()
    }
    pub fn time_range(&self) -> TimeRange {
        let mut time_range = TimeRange::none();
        for chunk in self.chunks.iter() {
            time_range.merge(&chunk.statics.time_range);
        }
        time_range
    }
}

pub type TableId = u64;

#[derive(Serialize, Deserialize)]
pub struct ChunkGroupWriteSpec {
    // pub(crate) id: TableId,
    pub(crate) name: String,
    // pub(crate) table: TableSchema,
    pub(crate) chunk_group_offset: u64,
    pub(crate) chunk_group_size: usize,
    pub(crate) time_range: TimeRange,
    pub(crate) count: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ChunkGroupMeta {
    pub(crate) tables: Vec<ChunkGroupWriteSpec>,
}

impl ChunkGroupMeta {
    pub fn new() -> Self {
        Self { tables: Vec::new() }
    }
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self).context(SerializeSnafu)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context(DeserializeSnafu)
    }

    pub fn push(&mut self, table: ChunkGroupWriteSpec) {
        self.tables.push(table);
    }
    pub fn len(&self) -> usize {
        self.tables.len()
    }
    pub fn time_range(&self) -> TimeRange {
        let mut time_range = TimeRange::none();
        for table in self.tables.iter() {
            time_range.merge(&table.time_range);
        }
        time_range
    }
}

// pub const FOOTER_SIZE: i64 = ;

#[derive(Serialize, Deserialize)]
pub struct Footer {
    pub(crate) version: u8,
    pub(crate) time_range: TimeRange,
    //8 + 8
    pub(crate) table: TableMeta,
    // series: SeriesMeta,
}

impl Footer {
    pub fn new(version: u8, time_range: TimeRange, table: TableMeta) -> Self {
        Self {
            version,
            time_range,
            table,
        }
    }
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self).context(SerializeSnafu)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context(DeserializeSnafu)
    }
}

///  7 + 8 + 8 = 23
#[derive(Serialize, Deserialize)]
pub struct TableMeta {
    // bloomfilter: Vec<u8>,
    // 7 Byte
    chunk_group_offset: u64,
    chunk_group_size: usize,
}

impl TableMeta {
    pub fn new(chunk_group_offset: u64, chunk_group_size: usize) -> Self {
        Self {
            chunk_group_offset,
            chunk_group_size,
        }
    }
}

/// 16 + 8 + 8 = 32
pub struct SeriesMeta {
    // bloomfilter: Vec<u8>,
    // 16 Byte
    chunk_offset: u64,
    chunk_size: u64,
}
