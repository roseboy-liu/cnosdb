use std::collections::BTreeMap;

use models::codec::Encoding;
use models::field_value::FieldVal;
use models::predicate::domain::TimeRange;
use models::schema::{ColumnType, TableColumn};
use models::ValueType;
use snafu::ResultExt;
use utils::bitset::BitSet;

use crate::error::IOSnafu;
use crate::file_system::file::cursor::FileCursor;
use crate::tsm::codec::{
    get_bool_codec, get_f64_codec, get_i64_codec, get_str_codec, get_u64_codec,
};
use crate::tsm2::page::{
    Chunk, ChunkGroup, ChunkGroupMeta, ChunkGroupWriteSpec, ChunkStatics, ChunkWriteSpec, Footer,
    Page, PageMeta, PageStatistics, PageWriteSpec, TableMeta,
};
use crate::Result;

// #[derive(Debug, Clone)]
// pub enum Array {
//     F64(Vec<Option<f64>>),
//     I64(Vec<Option<i64>>),
//     U64(Vec<Option<u64>>),
//     String(Vec<Option<MiniVec<u8>>>),
//     Bool(Vec<Option<bool>>),
// }
//
// impl Array {
//     pub fn push(&mut self, value: Option<FieldVal>) {
//         match (self, value) {
//             (Array::F64(array), Some(FieldVal::Float(v))) => array.push(Some(v)),
//             (Array::F64(array), None) => array.push(None),
//
//             (Array::I64(array), Some(FieldVal::Integer(v))) => array.push(Some(v)),
//             (Array::I64(array), None) => array.push(None),
//
//             (Array::U64(array), Some(FieldVal::Unsigned(v))) => array.push(Some(v)),
//             (Array::U64(array), None) => array.push(None),
//
//             (Array::String(array), Some(FieldVal::Bytes(v))) => array.push(Some(v)),
//             (Array::String(array), None) => array.push(None),
//
//             (Array::Bool(array), Some(FieldVal::Boolean(v))) => array.push(Some(v)),
//             (Array::Bool(array), None) => array.push(None),
//             _ => { panic!("invalid type: array type does not match filed type") }
//         }
//     }
// }
//
//
// #[derive(Debug, Clone)]
// pub enum Array2 {
//     F64(Vec<Option<f64>>),
//     I64(Vec<Option<i64>>),
//     U64(Vec<Option<u64>>),
//     String(Vec<Option<MiniVec<u8>>>),
//     Bool(Vec<Option<bool>>),
// }
//
// impl Array2 {
//     pub fn push(&mut self, value: Option<FieldVal>) {
//         match (self, value) {
//             (Array2::F64(array), Some(FieldVal::Float(v))) => array.push(Some(v)),
//             (Array2::F64(array), None) => array.push(None),
//
//             (Array2::I64(array), Some(FieldVal::Integer(v))) => array.push(Some(v)),
//             (Array2::I64(array), None) => array.push(None),
//
//             (Array2::U64(array), Some(FieldVal::Unsigned(v))) => array.push(Some(v)),
//             (Array2::U64(array), None) => array.push(None),
//
//             (Array2::String(array), Some(FieldVal::Bytes(v))) => array.push(Some(v)),
//             (Array2::String(array), None) => array.push(None),
//
//             (Array2::Bool(array), Some(FieldVal::Boolean(v))) => array.push(Some(v)),
//             (Array2::Bool(array), None) => array.push(None),
//             _ => { panic!("invalid type: array type does not match filed type") }
//         }
//     }
// }

/// max size 1024
#[derive(Debug, Clone)]
pub struct Column {
    pub column_type: ColumnType,
    pub valid: BitSet,
    pub data: ColumnData,
}

impl Column {
    pub fn new(row_count: usize, column_type: ColumnType) -> Column {
        let mut valid = BitSet::new();
        valid.append_unset(row_count);

        let data = match column_type {
            ColumnType::Tag => {
                ColumnData::String(vec![String::new(); row_count], String::new(), String::new())
            }
            ColumnType::Time(_) => ColumnData::I64(vec![0; row_count], i64::MAX, i64::MIN),
            ColumnType::Field(field_type) => match field_type {
                ValueType::Float => ColumnData::F64(vec![0.0; row_count], f64::MAX, f64::MIN),
                ValueType::Integer => ColumnData::I64(vec![0; row_count], i64::MAX, i64::MIN),
                ValueType::Unsigned => ColumnData::U64(vec![0; row_count], u64::MAX, u64::MIN),
                ValueType::Boolean => ColumnData::Bool(vec![false; row_count], false, true),
                ValueType::String => {
                    ColumnData::String(vec![String::new(); row_count], String::new(), String::new())
                }
                _ => {
                    panic!("invalid type: field type does not match filed type")
                }
            },
        };
        Self {
            column_type,
            valid,
            data,
        }
    }
    pub fn push(&mut self, value: Option<FieldVal>) {
        match (&mut self.data, value) {
            (ColumnData::F64(ref mut value, min, max), Some(FieldVal::Float(val))) => {
                value.push(val);
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                self.valid.append_set(1);
            }
            (ColumnData::F64(ref mut value, min, max), None) => {
                value.push(0.0);
                self.valid.append_unset(1);
            }
            (ColumnData::I64(ref mut value, min, max), Some(FieldVal::Integer(val))) => {
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                value.push(val);
                self.valid.append_set(1);
            }
            (ColumnData::I64(ref mut value, min, max), None) => {
                value.push(0);
                self.valid.append_unset(1);
            }
            (ColumnData::U64(ref mut value, min, max), Some(FieldVal::Unsigned(val))) => {
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                value.push(val);
                self.valid.append_set(1);
            }
            (ColumnData::U64(ref mut value, min, max), None) => {
                value.push(0);
                self.valid.append_unset(1);
            }
            //todo: need to change string to Bytes type in ColumnData
            (ColumnData::String(ref mut value, min, max), Some(FieldVal::Bytes(val))) => {
                let val = String::from_utf8(val.to_vec()).unwrap();
                if *max < val {
                    *max = val.clone();
                }
                if *min > val {
                    *min = val.clone();
                }
                value.push(val);
                self.valid.append_set(1);
            }
            (ColumnData::String(ref mut value, min, max), None) => {
                value.push(String::new());
                self.valid.append_unset(1);
            }
            (ColumnData::Bool(ref mut value, min, max), Some(FieldVal::Boolean(val))) => {
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                value.push(val);
                self.valid.append_set(1);
            }
            (ColumnData::Bool(ref mut value, min, max), None) => {
                value.push(false);
                self.valid.append_unset(1);
            }
            _ => {
                panic!("Column type mismatch")
            }
        }
    }
    pub fn col_to_page(&self, desc: TableColumn) -> Page {
        let len = self.valid.byte_len() as u32;
        let mut buf = vec![];
        let (min, max) = match &self.data {
            ColumnData::F64(array, min, max) => {
                let encoder = get_f64_codec(desc.encoding);
                encoder.encode(&array, &mut buf).unwrap();
                (Vec::from(min.to_le_bytes()), Vec::from(max.to_le_bytes()))
            }
            ColumnData::I64(array, min, max) => {
                let encoder = get_i64_codec(desc.encoding);
                encoder.encode(&array, &mut buf).unwrap();
                (Vec::from(min.to_le_bytes()), Vec::from(max.to_le_bytes()))
            }
            ColumnData::U64(array, min, max) => {
                let encoder = get_u64_codec(desc.encoding);
                encoder.encode(&array, &mut buf).unwrap();
                (Vec::from(min.to_le_bytes()), Vec::from(max.to_le_bytes()))
            }
            ColumnData::String(array, min, max) => {
                let encoder = get_str_codec(desc.encoding);
                encoder
                    .encode(array.iter().into_bytes().collect(), &mut buf)
                    .unwrap();
                (min.into_bytes(), max.into_bytes())
            }
            ColumnData::Bool(array, min, max) => {
                let encoder = get_bool_codec(desc.encoding);
                encoder.encode(&array, &mut buf).unwrap();
                (vec![*min as u8], vec![*max as u8])
            }
        };
        let mut data = vec![];
        data.extend_from_slice(&len.to_be_bytes());
        data.extend_from_slice(self.valid.bytes());
        data.extend_from_slice(&buf);
        let bytes = bytes::Bytes::from(data);
        let meta = PageMeta {
            num_values: self.valid.len() as u32,
            column: desc,
            time_range: None,
            statistic: PageStatistics {
                primitive_type: ValueType::Unknown,
                null_count: None,
                distinct_count: None,
                max_value: Some(max),
                min_value: Some(min),
            },
        };
        Page { bytes, meta }
    }
}

#[derive(Debug, Clone)]
pub enum ColumnData {
    ///   array   min, max
    F64(Vec<f64>, f64, f64),
    I64(Vec<i64>, i64, i64),
    U64(Vec<u64>, u64, u64),
    String(Vec<String>, String, String),
    Bool(Vec<bool>, bool, bool),
}

#[derive(Debug, Clone)]
pub struct DataBlock2 {
    ts: Vec<i64>,
    cols: Vec<Column>,
    cols_desc: Vec<TableColumn>,
}

impl DataBlock2 {
    pub fn new(ts: Vec<i64>, cols: Vec<Column>, cols_desc: Vec<TableColumn>) -> Self {
        DataBlock2 {
            ts,
            cols,
            cols_desc,
        }
    }
    //todo dont forgot build time column to pages
    pub fn block_to_page(&self) -> Vec<Page> {
        let mut pages = vec![];
        for (col, desc) in self.cols.iter().zip(self.cols_desc) {
            pages.push(col.col_to_page(desc));
        }
        pages
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum State {
    Initialised,
    Started,
    Finished,
}

pub enum Version {
    V1,
}

pub struct WriteOptions {
    version: Version,
    write_statistics: bool,
    encode: Encoding,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            version: Version::V1,
            write_statistics: true,
            encode: Encoding::Null,
        }
    }
}

const HEADER_LEN: u64 = 5;
const TSM_MAGIC: [u8; 4] = 0x12CDA16_u32.to_be_bytes();
const VERSION: [u8; 1] = [1];

pub struct Tsm2Writer {
    writer: FileCursor,
    options: WriteOptions,
    /// <table < series, Chunk>>
    page_specs: BTreeMap<String, BTreeMap<String, Chunk>>,
    /// <table, ChunkGroup>
    chunk_specs: BTreeMap<String, ChunkGroup>,
    /// [ChunkGroupWriteSpec]
    chunk_group_specs: ChunkGroupMeta,
    footer: Footer,
    state: State,
}

//MutableRecordBatch
impl Tsm2Writer {
    pub fn new(writer: FileCursor) -> Self {
        Self {
            writer,
            options: Default::default(),
            page_specs: Default::default(),
            chunk_specs: Default::default(),
            state: State::Initialised,
        }
    }
    pub async fn write_header(&mut self) -> Result<usize> {
        self.writer
            .write_vec([TSM_MAGIC.to_vec(), VERSION.to_vec()])
            .await
            .context(IOSnafu)
    }

    /// todo: write footer
    pub async fn write_footer(&mut self) -> Result<usize> {
        self.writer
            .write(&self.footer.serialize()?)
            .await
            .context(IOSnafu)
    }

    pub async fn write_chunk_group(&mut self) -> Result<()> {
        for (table, group) in &self.chunk_specs {
            let chunk_group_offset = self.writer.pos();
            let buf = group.serialize()?;
            let chunk_group_size = self.writer.write(&buf).await?;
            let chunk_group_spec = ChunkGroupWriteSpec {
                name: table.clone(),
                chunk_group_offset,
                chunk_group_size,
                time_range: group.time_range(),
                /// The number of chunks in the group.
                count: 0,
            };
            self.chunk_group_specs.push(chunk_group_spec);
        }
        Ok(())
    }

    pub async fn write_chunk_group_specs(&mut self) -> Result<()> {
        let chunk_group_specs_offset = self.writer.pos();
        let buf = self.chunk_group_specs.serialize()?;
        let chunk_group_specs_size = self.writer.write(&buf).await?;
        let time_range = self.chunk_group_specs.time_range();
        let footer = Footer {
            version: 2_u8,
            time_range,
            table: TableMeta::new(chunk_group_specs_offset, chunk_group_specs_size),
        };
        self.footer = footer;
        Ok(())
    }

    pub async fn write_chunk(&mut self) -> Result<()> {
        for (table, group) in &self.page_specs {
            for (series, chunk) in group {
                let chunk_offset = self.writer.pos();
                let buf = chunk.serialize()?;
                let chunk_size = self.writer.write(&buf).await?;
                let time_range = chunk.time_range();
                let chunk_spec = ChunkWriteSpec {
                    series: series.clone(),
                    chunk_offset,
                    chunk_size,
                    statics: ChunkStatics { time_range },
                };
                self.chunk_specs
                    .entry(table.clone())
                    .or_default()
                    .push(chunk_spec);
            }
        }
        Ok(())
    }
    pub async fn write_data(
        &mut self,
        groups: BTreeMap<String, BTreeMap<String, Vec<Page>>>,
    ) -> Result<()> {
        if self.state == State::Initialised {
            self.write_header().await?;
        }

        // write page data
        for (table, group) in groups {
            for (series, pages) in group {
                let mut time_range = TimeRange::none();
                for page in pages {
                    let offset = self.writer.pos();
                    let size = self.writer.write(&page.bytes).await?;
                    time_range.merge(&page.meta.time_range.unwrap());
                    let spec = PageWriteSpec {
                        offset,
                        size,
                        meta: page.meta,
                    };
                    self.page_specs
                        .entry(table.clone())
                        .or_default()
                        .entry(series.clone())
                        .or_default()
                        .push(spec);
                }
            }
        }
        Ok(())
    }
}
