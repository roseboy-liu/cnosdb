use models::predicate::domain::TimeRange;
use models::schema::TableColumn;
use models::{PhysicalDType, SeriesId};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::Error;

pub struct Page {
    pub(crate) bytes: bytes::Bytes,
    pub(crate) meta: PageMeta,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PageMeta {
    pub(crate) num_values: u32,
    pub(crate) column: TableColumn,
    pub(crate) time_range: TimeRange,
    pub(crate) statistic: PageStatistics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageStatistics {
    pub(crate) primitive_type: PhysicalDType,
    pub(crate) null_count: Option<i64>,
    pub(crate) distinct_count: Option<i64>,
    pub(crate) max_value: Option<Vec<u8>>,
    pub(crate) min_value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PageWriteSpec {
    pub(crate) offset: u64,
    pub(crate) size: usize,
    pub(crate) meta: PageMeta,
}

impl PageWriteSpec {
    pub fn new(offset: u64, size: usize, meta: PageMeta) -> Self {
        Self { offset, size, meta }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn meta(&self) -> &PageMeta {
        &self.meta
    }
}

/// A chunk of data for a series at least two columns
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Chunk {
    pages: Vec<PageWriteSpec>,
}

impl Chunk {
    pub fn new() -> Self {
        Self { pages: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn pages(&self) -> &[PageWriteSpec] {
        &self.pages
    }
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| Error::Serialize { source: e.into() })
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| Error::Deserialize { source: e.into() })
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkWriteSpec {
    pub(crate) series_id: SeriesId,
    pub(crate) chunk_offset: u64,
    pub(crate) chunk_size: usize,
    pub(crate) statics: ChunkStatics,
}

impl ChunkWriteSpec {
    pub fn new(
        series_id: SeriesId,
        chunk_offset: u64,
        chunk_size: usize,
        statics: ChunkStatics,
    ) -> Self {
        Self {
            series_id,
            chunk_offset,
            chunk_size,
            statics,
        }
    }

    pub fn series_id(&self) -> SeriesId {
        self.series_id
    }

    pub fn chunk_offset(&self) -> u64 {
        self.chunk_offset
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn statics(&self) -> &ChunkStatics {
        &self.statics
    }
}

/// ChunkStatics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkStatics {
    pub(crate) time_range: TimeRange,
}

/// A group of chunks for a table
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct ChunkGroup {
    pub(crate) chunks: Vec<ChunkWriteSpec>,
}

impl ChunkGroup {
    pub fn new() -> Self {
        Self { chunks: Vec::new() }
    }
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| Error::Serialize { source: e.into() })
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| Error::Deserialize { source: e.into() })
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

    pub fn chunks(&self) -> &[ChunkWriteSpec] {
        &self.chunks
    }
}

pub type TableId = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkGroupWriteSpec {
    // pub(crate) id: TableId,
    pub(crate) name: String,
    // pub(crate) table: TableSchema,
    pub(crate) chunk_group_offset: u64,
    pub(crate) chunk_group_size: usize,
    pub(crate) time_range: TimeRange,
    pub(crate) count: usize,
}

impl ChunkGroupWriteSpec {
    pub fn new(
        name: String,
        chunk_group_offset: u64,
        chunk_group_size: usize,
        time_range: TimeRange,
        count: usize,
    ) -> Self {
        Self {
            name,
            chunk_group_offset,
            chunk_group_size,
            time_range,
            count,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn chunk_group_offset(&self) -> u64 {
        self.chunk_group_offset
    }

    pub fn chunk_group_size(&self) -> usize {
        self.chunk_group_size
    }

    pub fn time_range(&self) -> &TimeRange {
        &self.time_range
    }

    pub fn count(&self) -> usize {
        self.count
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkGroupMeta {
    pub(crate) tables: Vec<ChunkGroupWriteSpec>,
}

impl Default for ChunkGroupMeta {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkGroupMeta {
    pub fn new() -> Self {
        Self { tables: Vec::new() }
    }
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| Error::Serialize { source: e.into() })
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| Error::Deserialize { source: e.into() })
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

    pub fn tables(&self) -> &[ChunkGroupWriteSpec] {
        &self.tables
    }
}

// pub const FOOTER_SIZE: i64 = ;

#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Footer {
    pub(crate) version: u8,
    pub(crate) time_range: TimeRange,
    //8 + 8
    pub(crate) table: TableMeta,
    pub(crate) series: SeriesMeta,
}

impl Footer {
    pub fn new(version: u8, time_range: TimeRange, table: TableMeta, series: SeriesMeta) -> Self {
        Self {
            version,
            time_range,
            table,
            series,
        }
    }

    pub fn table(&self) -> &TableMeta {
        &self.table
    }

    pub fn series(&self) -> &SeriesMeta {
        &self.series
    }

    pub fn time_range(&self) -> &TimeRange {
        &self.time_range
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| Error::Serialize { source: e.into() })
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| Error::Deserialize { source: e.into() })
    }
}

///  7 + 8 + 8 = 23
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct TableMeta {
    // todo: bloomfilter, store table object id
    // bloom_filter: Vec<u8>,
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

    pub fn chunk_group_offset(&self) -> u64 {
        self.chunk_group_offset
    }

    pub fn chunk_group_size(&self) -> usize {
        self.chunk_group_size
    }
}

/// 16 + 8 + 8 = 32
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SeriesMeta {
    bloom_filter: Vec<u8>,
    // 16 Byte
    chunk_offset: u64,
    chunk_size: u64,
}

impl SeriesMeta {
    pub fn new(bloom_filter: Vec<u8>, chunk_offset: u64, chunk_size: u64) -> Self {
        Self {
            bloom_filter,
            chunk_offset,
            chunk_size,
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| Error::Serialize { source: e.into() })
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| Error::Deserialize { source: e.into() })
    }

    pub fn bloom_filter(&self) -> &Vec<u8> {
        &self.bloom_filter
    }

    pub fn chunk_offset(&self) -> u64 {
        self.chunk_offset
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }
}

#[cfg(test)]
mod test {
    use models::predicate::domain::TimeRange;
    use utils::BloomFilter;

    use crate::tsm2::page::{Footer, SeriesMeta, TableMeta};
    use crate::tsm2::BLOOM_FILTER_BITS;

    #[test]
    fn test1() {
        let table_meta = TableMeta {
            chunk_group_offset: 100,
            chunk_group_size: 100,
        };
        let expect_footer = Footer::new(
            1,
            TimeRange {
                min_ts: 0,
                max_ts: 100,
            },
            table_meta,
            SeriesMeta::new(
                BloomFilter::new(BLOOM_FILTER_BITS).bytes().to_vec(),
                100,
                100,
            ),
        );
        let bytess = expect_footer.serialize().unwrap();
        println!("bytes: {:?}", bytess.len());
        let footer = Footer::deserialize(&bytess).unwrap();
        assert_eq!(footer, expect_footer);
    }
}
