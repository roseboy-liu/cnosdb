use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::parquet::data_type::AsBytes;
use tokio::sync::RwLock;
use models::{ColumnId, SeriesId};
use utils::BloomFilter;

use crate::error::Result;
use crate::file_system::file::async_file::AsyncFile;
use crate::file_system::file::IFile;
use crate::file_system::file_manager;
use crate::{Error, file_utils};
use crate::tsm2::page::{Chunk, ChunkGroup, ChunkGroupMeta, Footer, Page, PageWriteSpec};
use crate::tsm2::{TsmWriteData, FOOTER_SIZE};

struct TsmMeta {
    footer: Option<Footer>,
    chunk_group_meta: Option<ChunkGroupMeta>,
    chunk_group: Option<BTreeMap<String, ChunkGroup>>, // table_name -> chunk_group
    chunk: Option<BTreeMap<String, BTreeMap<SeriesId, Chunk>>>,
}

impl Default for TsmMeta {
    fn default() -> Self {
        Self {
            footer: None,
            chunk_group_meta: None,
            chunk_group: None,
            chunk: None,
        }
    }
}

#[derive(Clone)]
pub struct TSM2Reader {
    file_id: u64,
    reader: Arc<AsyncFile>,

    footer: Footer,
    chunk_group_meta: ChunkGroupMeta,
    chunk_group: BTreeMap<String, ChunkGroup>, // table_name -> chunk_group
    chunk: BTreeMap<String, BTreeMap<SeriesId, Chunk>>,
}

impl TSM2Reader {
    pub async fn open(tsm_path: impl AsRef<Path>) -> Result<Self> {
        let path = tsm_path.as_ref().to_path_buf();
        let file_id = file_utils::get_tsm_file_id_by_path(&path)?;
        let reader = Arc::new(file_manager::open_file(tsm_path).await?);
        let footer = read_footer(reader.clone()).await?;
        let chunk_group_meta = read_chunk_group_meta(reader.clone(), &footer).await?;
        let chunk_group = read_chunk_groups(reader.clone(), &chunk_group_meta).await?;
        let chunk = read_chunk(reader.clone(), &chunk_group).await?;
        Ok(Self {
            file_id,
            reader,
            footer,
            chunk_group_meta,
            chunk_group,
            chunk,
        })
    }


    pub fn footer(&self) -> &Footer {
        &self.footer
    }


    pub fn chunk_group_meta(&self) -> &ChunkGroupMeta {
        &self.chunk_group_meta
    }

    pub fn chunk_group(&self) -> &BTreeMap<String, ChunkGroup> {
        &self.chunk_group
    }

    pub fn chunk(&self) -> &BTreeMap<String, BTreeMap<SeriesId, Chunk>> {
        &self.chunk
    }


    pub async fn read_pages(
        &mut self,
        series_ids: &[SeriesId],
        column_id: &[ColumnId],
    ) -> Result<Vec<Page>> {
        let mut res = Vec::new();
        let footer = self.footer();
        let bloom_filter = BloomFilter::with_data(footer.series().bloom_filter());
        let reader = self.reader.clone();
        for sid in series_ids {
            if !bloom_filter.contains(&sid.as_bytes()) {
                continue
            }
            let chunk = self.chunk().iter().filter_map(|(_, chunk)| chunk.get(sid)).collect::<Vec<_>>();
            for c in chunk {
                for pages in c.pages() {
                    if column_id.contains(&pages.meta.column.id) {
                        let page = read_page(reader.clone(), pages).await?;
                        res.push(page);
                    }
                }
            }
        }
        Ok(res)
    }

    pub async fn read_all_pages(&self) -> Result<TsmWriteData> {
        let chunk = self.chunk();
        let pages = read_pages(self.reader.clone(), chunk).await?;
        Ok(pages)
    }

    pub async fn read_series_pages(&self, series_id: SeriesId) -> Result<Vec<Page>> {
        let chunk = self.chunk();
        let meta = self.chunk_group_meta();
        let mut res = Vec::new();
        let reader = self.reader.clone();
        for (_, chunk) in chunk {
            if let Some(chunk) = chunk.get(&series_id) {
                for page in chunk.pages() {
                    let page = read_page(reader.clone(), page).await?;
                    res.push(page);
                }
                break;
            }
        }
        Ok(res)
    }
}

impl Debug for TSM2Reader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TSMReader")
            .field("file_id", &self.file_id)
            .field("footer", &self.footer)
            .field("chunk_group_meta", &self.chunk_group_meta)
            .field("chunk_group", &self.chunk_group)
            .field("chunk", &self.chunk)
            .finish()
    }
}

pub async fn read_footer(reader: Arc<AsyncFile>) -> Result<Footer> {
    let pos = reader.len() - (FOOTER_SIZE as u64);
    let mut buffer = vec![0u8; FOOTER_SIZE];
    reader.read_at(pos, &mut buffer).await?;
    Footer::deserialize(&buffer)
}

pub async fn read_chunk_group_meta(
    reader: Arc<AsyncFile>,
    footer: &Footer,
) -> Result<ChunkGroupMeta> {
    let pos = footer.table.chunk_group_offset();
    let mut buffer = vec![0u8; footer.table.chunk_group_size()];
    reader.read_at(pos, &mut buffer).await?; // read chunk group meta
    let specs = ChunkGroupMeta::deserialize(&buffer)?;
    Ok(specs)
}

pub async fn read_chunk_groups(
    reader: Arc<AsyncFile>,
    chunk_group_meta: &ChunkGroupMeta,
) -> Result<BTreeMap<String, ChunkGroup>> {
    let mut specs = BTreeMap::new();
    for (_, chunk) in chunk_group_meta.tables() {
        let pos = chunk.chunk_group_offset();
        let mut buffer = vec![0u8; chunk.chunk_group_size()];
        reader.read_at(pos, &mut buffer).await?; // read chunk group meta
        let group = ChunkGroup::deserialize(&buffer)?;
        specs.insert(chunk.name().to_string(), group);
    }
    Ok(specs)
}

pub async fn read_chunk(
    reader: Arc<AsyncFile>,
    chunk_group: &BTreeMap<String, ChunkGroup>,
) -> Result<BTreeMap<String, BTreeMap<SeriesId, Chunk>>> {
    let mut chunks = BTreeMap::new();
    for (table_name, group) in chunk_group {
        let mut table_chunks = BTreeMap::new();
        for chunk_spec in group.chunks() {
            let pos = chunk_spec.chunk_offset();
            let mut buffer = vec![0u8; chunk_spec.chunk_size()];
            reader.read_at(pos, &mut buffer).await?;
            let chunk = Chunk::deserialize(&buffer)?;
            table_chunks.insert(chunk_spec.series_id, chunk);
        }
        chunks.insert(table_name.clone(), table_chunks);
    }
    Ok(chunks)
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
            table_pages.insert(series_id, chunk_pages);
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
        meta: page_spec.meta().clone(),
        bytes: Bytes::from(buffer),
    };
    Ok(page)
}
