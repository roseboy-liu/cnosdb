use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::parquet::data_type::AsBytes;
use models::{ColumnId, SeriesId};
use utils::BloomFilter;

use crate::error::Result;
use crate::file_system::file::async_file::AsyncFile;
use crate::file_system::file::IFile;
use crate::file_system::file_manager;
use crate::file_utils;
use crate::tsm2::page::{Chunk, ChunkGroup, ChunkGroupMeta, Footer, Page, PageWriteSpec};
use crate::tsm2::{TsmWriteData, FOOTER_SIZE};

pub struct TSM2Reader {
    file_id: u64,
    reader: Arc<AsyncFile>,

    footer: Option<Footer>,
    chunk_group_meta: Option<ChunkGroupMeta>,
    chunk_group: Option<BTreeMap<String, ChunkGroup>>, // table_name -> chunk_group
    chunk: Option<BTreeMap<String, BTreeMap<SeriesId, Chunk>>>,
}

impl TSM2Reader {
    pub async fn open(tsm_path: impl AsRef<Path>) -> Result<Self> {
        let path = tsm_path.as_ref().to_path_buf();
        let file_id = file_utils::get_tsm_file_id_by_path(&path)?;
        let reader = Arc::new(file_manager::open_file(tsm_path).await?);
        Ok(Self {
            file_id,
            reader,
            footer: None,
            chunk_group_meta: None,
            chunk_group: None,
            chunk: None,
        })
    }

    pub async fn prepare_footer(&mut self) -> Result<()> {
        if self.footer.is_none() {
            self.footer = Some(read_footer(self.reader.clone()).await?);
        }
        Ok(())
    }

    pub async fn prepare(&mut self) -> Result<()> {
        if self.footer.is_none() {
            self.footer = Some(read_footer(self.reader.clone()).await?);
        }
        if self.chunk_group_meta.is_none() {
            self.chunk_group_meta =
                Some(read_chunk_group_meta(self.reader.clone(), self.footer.as_ref().unwrap()).await?);
        }
        if self.chunk_group.is_none() {
            self.chunk_group = Some(
                read_chunk_groups(self.reader.clone(), self.chunk_group_meta.as_ref().unwrap())
                    .await?,
            );
        }
        if self.chunk.is_none() {
            self.chunk = Some(
                read_chunk(self.reader.clone(), self.chunk_group.as_ref().unwrap().clone()).await?,
            );
        }
        Ok(())
    }

    pub async fn read_pages(
        &mut self,
        series_ids: &[SeriesId],
        column_id: &[ColumnId],
    ) -> Result<Vec<Page>> {
        let mut res = Vec::new();
        self.prepare_footer().await?;
        if let Some(ref footer) = self.footer {
            let bloom_filter = BloomFilter::with_data(footer.series.bloom_filter());
            for sid in series_ids {
                if bloom_filter.contains(&sid.as_bytes()) {
                    self.prepare().await?;
                    if let Some(ref chunk) = self.chunk {
                        for cid in column_id {
                            for (_, chunks) in chunk {
                                if let Some(chunk) = chunks.get(sid) {
                                    for pages in chunk.pages() {
                                        if pages.meta.column.id == *cid {
                                            let page = read_page(self.reader.clone(), pages).await?;
                                            res.push(page);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(res)
    }

    pub async fn read(&mut self) -> Result<TsmWriteData> {
        let footer = read_footer(self.reader.clone()).await?;
        let chunk_group_meta = read_chunk_group_meta(self.reader.clone(), &footer).await?;
        let chunk_group = read_chunk_groups(self.reader.clone(), &chunk_group_meta).await?;
        let chunk = read_chunk(self.reader.clone(), &chunk_group).await?;
        let pages = read_pages(self.reader.clone(), chunk).await?;
        Ok(pages)
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
    for chunk in chunk_group_meta.tables() {
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
    chunk: BTreeMap<String, BTreeMap<SeriesId, Chunk>>,
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
        pages.insert(table_name, table_pages);
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
