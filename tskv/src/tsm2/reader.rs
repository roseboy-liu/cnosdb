use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use datafusion::parquet::data_type::AsBytes;
use models::predicate::domain::TimeRange;
use models::{ColumnId, SeriesId};
use parking_lot::RwLock;

use crate::error::Result;
use crate::file_system::file::async_file::AsyncFile;
use crate::file_system::file::IFile;
use crate::file_system::file_manager;
use crate::file_utils;
use crate::tsm::TsmTombstone;
use crate::tsm2::page::{Chunk, ChunkGroup, ChunkGroupMeta, Footer, Page, PageMeta, PageWriteSpec};
use crate::tsm2::{TsmWriteData, FOOTER_SIZE};

pub struct TSM2MetaData {
    footer: Footer,

    chunk_group_meta: ChunkGroupMeta,

    ///
    chunk_group: BTreeMap<String, ChunkGroup>,
    ///  table_name -> chunk_group
    chunk: BTreeMap<SeriesId, Chunk>,
}

impl TSM2MetaData {
    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    pub fn chunk_group_meta(&self) -> &ChunkGroupMeta {
        &self.chunk_group_meta
    }

    pub fn chunk_group(&self) -> &BTreeMap<String, ChunkGroup> {
        &self.chunk_group
    }

    pub fn chunk(&self) -> &BTreeMap<SeriesId, Chunk> {
        &self.chunk
    }

    pub fn table_schema(&self /*table: String*/) -> SchemaRef {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct TSM2Reader {
    file_location: PathBuf,
    file_id: u64,
    reader: Option<Arc<AsyncFile>>,
    tsm_meta: Option<Arc<TSM2MetaData>>,
    tombstone: Option<Arc<RwLock<TsmTombstone>>>,
}

impl TSM2Reader {
    pub fn open(tsm_path: impl AsRef<Path>) -> Result<Self> {
        let path = tsm_path.as_ref().to_path_buf();
        let file_id = file_utils::get_tsm_file_id_by_path(&path)?;

        Ok(Self {
            file_location: path,
            file_id,
            reader: None,
            tsm_meta: None,
            tombstone: None,
        })
    }
    pub async fn reader(&mut self) -> Result<Arc<AsyncFile>> {
        match &self.reader {
            None => {
                let reader = Arc::new(file_manager::open_file(&self.file_location).await?);
                self.reader = Some(reader.clone());
                Ok(reader)
            }
            Some(reader) => Ok(reader.clone()),
        }
    }

    pub fn is_meta_loaded(&self) -> bool {
        self.tsm_meta.is_some()
    }

    pub fn fid(&self) -> u64 {
        self.file_id
    }
    pub async fn load_tsm_meta(&mut self) -> Result<Arc<TSM2MetaData>> {
        if let Some(meta) = self.tsm_meta.as_ref() {
            return Ok(meta.clone());
        }
        let footer = self.load_footer().await?;
        let chunk_group_meta = self.load_chunk_group_meta(&footer).await?;
        let chunk_group = self.load_chunk_groups(&chunk_group_meta).await?;
        let chunk = self.load_chunk(&chunk_group).await?;
        let meta = Arc::new(TSM2MetaData {
            footer,
            chunk_group_meta,
            chunk_group,
            chunk,
        });
        self.tsm_meta = Some(meta.clone());
        Ok(meta)
    }

    async fn load_footer(&mut self) -> Result<Footer> {
        let reader = self.reader().await?;
        let pos = reader.len() - (FOOTER_SIZE as u64);
        let mut buffer = vec![0u8; FOOTER_SIZE];
        reader.read_at(pos, &mut buffer).await?;
        Footer::deserialize(&buffer)
    }

    async fn load_chunk_group_meta(&mut self, footer: &Footer) -> Result<ChunkGroupMeta> {
        let pos = footer.table.chunk_group_offset();
        let mut buffer = vec![0u8; footer.table.chunk_group_size()];
        self.reader().await?.read_at(pos, &mut buffer).await?; // read chunk group meta
        let specs = ChunkGroupMeta::deserialize(&buffer)?;
        Ok(specs)
    }

    async fn load_chunk_groups(
        &mut self,
        chunk_group_meta: &ChunkGroupMeta,
    ) -> Result<BTreeMap<String, ChunkGroup>> {
        let mut specs = BTreeMap::new();
        for chunk in chunk_group_meta.tables() {
            let pos = chunk.chunk_group_offset();
            let mut buffer = vec![0u8; chunk.chunk_group_size()];
            self.reader().await?.read_at(pos, &mut buffer).await?; // read chunk group meta
            let group = ChunkGroup::deserialize(&buffer)?;
            specs.insert(chunk.name().to_string(), group);
        }
        Ok(specs)
    }

    async fn load_chunk(
        &mut self,
        chunk_group: &BTreeMap<String, ChunkGroup>,
    ) -> Result<BTreeMap<SeriesId, Chunk>> {
        let mut chunks = BTreeMap::new();
        for group in chunk_group.values() {
            for chunk_spec in group.chunks() {
                let pos = chunk_spec.chunk_offset();
                let mut buffer = vec![0u8; chunk_spec.chunk_size()];
                self.reader().await?.read_at(pos, &mut buffer).await?;
                let chunk = Chunk::deserialize(&buffer)?;
                chunks.insert(chunk_spec.series_id, chunk);
            }
        }
        Ok(chunks)
    }

    fn footer(&mut self) -> Option<&Footer> {
        match self.tsm_meta {
            Some(ref meta) => Some(meta.footer()),
            None => None,
        }
    }

    // fn chunk_group_meta(&mut self) -> Option<&ChunkGroupMeta> {
    //     match self.tsm_meta {
    //         Some(ref meta) => Some(meta.chunk_group_meta()),
    //         None => {
    //             None
    //         }
    //     }
    // }
    //
    // pub fn chunk_group(&mut self) -> Option<&BTreeMap<String, ChunkGroup>> {
    //     match self.tsm_meta {
    //         Some(ref meta) => Some(meta.chunk_group()),
    //         None => {
    //             None
    //         }
    //     }
    // }
    //
    //
    // fn chunk(&mut self) -> Option<&BTreeMap<String, BTreeMap<SeriesId, Chunk>>> {
    //     match self.tsm_meta {
    //         Some(ref meta) => Some(meta.chunk()),
    //         None => {
    //             None
    //         }
    //     }
    // }

    pub async fn statistics(
        &mut self,
        series_ids: &[SeriesId],
        time_range: TimeRange,
    ) -> Result<BTreeMap<SeriesId, Vec<PageMeta>>> {
        let meta = self.load_tsm_meta().await?;
        let mut map = BTreeMap::new();
        for series_id in series_ids {
            let mut pages = vec![];
            if meta.footer().is_series_exist(series_id) {
                if let Some(chunk) = meta.chunk().get(series_id) {
                    // chunks.push(chunk);
                    for page in chunk.pages() {
                        if page.meta.time_range.overlaps(&time_range) {
                            pages.push(page.meta());
                        }
                    }
                }
            }
            map.insert(*series_id, pages);
        }
        Ok(map)
    }

    pub async fn read_pages(
        &mut self,
        series_ids: &[SeriesId],
        column_id: &[ColumnId],
    ) -> Result<Vec<Page>> {
        let mut res = Vec::new();
        let meta = self.load_tsm_meta().await?;
        let footer = meta.footer();
        let bloom_filter = footer.series().bloom_filter();
        let reader = self.reader().await?;
        for sid in series_ids {
            if !bloom_filter.contains((*sid).as_bytes()) {
                continue;
            }
            if let Some(chunk) = meta.chunk().get(sid) {
                for page in chunk.pages() {
                    if column_id.contains(&page.meta.column.id) {
                        let page = read_page(reader.clone(), page).await?;
                        res.push(page);
                    }
                }
            }
        }
        Ok(res)
    }

    // pub async fn read(&mut self) -> Result<TsmWriteData> {
    //     let reader = self.reader().await?;
    //     // must load tsm meta before
    //     let chunk = self.chunk().unwrap();
    //     let pages = read_pages(reader, chunk).await?;
    //     Ok(pages)
    // }
}

pub async fn read_pages(
    reader: Arc<AsyncFile>,
    chunk: &BTreeMap<String, BTreeMap<SeriesId, Chunk>>,
) -> Result<TsmWriteData> {
    let mut pages = TsmWriteData::new();
    for (table_name, table_chunks) in chunk {
        let mut table_pages = BTreeMap::new();
        for (series_id, chunk) in table_chunks {
            let mut chunk_pages = Vec::new();
            for page_spec in chunk.pages() {
                let pos = page_spec.offset();
                let mut buffer = vec![0u8; page_spec.size()];
                reader.read_at(pos, &mut buffer).await?;
                let page = Page {
                    meta: page_spec.meta().clone(),
                    bytes: Bytes::from(buffer),
                };
                chunk_pages.push(page);
            }
            table_pages.insert(*series_id, chunk_pages);
        }
        pages.insert(table_name.clone(), table_pages);
    }
    Ok(pages)
}

pub async fn read_page(reader: Arc<AsyncFile>, page_spec: &PageWriteSpec) -> Result<Page> {
    let pos = page_spec.offset();
    let mut buffer = vec![0u8; page_spec.size()];
    reader.read_at(pos, &mut buffer).await?;
    let page = Page {
        meta: page_spec.meta(),
        bytes: Bytes::from(buffer),
    };
    Ok(page)
}
