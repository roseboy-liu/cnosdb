use std::io::{IoSlice, SeekFrom};
use std::path::{Path, PathBuf};

use num_traits::ToPrimitive;
use snafu::ResultExt;

use super::{
    file_crc_source_len, Reader, RecordDataType, FILE_MAGIC_NUMBER, FILE_MAGIC_NUMBER_LEN,
    RECORD_MAGIC_NUMBER,
};
use crate::error::{self, Error, Result};
use crate::file_system::file::cursor::FileCursor;
use crate::file_system::file_manager;

pub struct Writer {
    path: PathBuf,
    cursor: FileCursor,
    footer: Option<Vec<u8>>, // FILE_FOOTER_LEN,
    file_size: u64,
}

impl Writer {
    pub async fn open(path: impl AsRef<Path>, _data_type: RecordDataType) -> Result<Self> {
        let path = path.as_ref();
        let mut cursor: FileCursor = file_manager::open_create_file(path).await?.into();
        let mut file_size = 0_u64;
        let footer = if cursor.is_empty() {
            // For new file, write file magic number, next write is at 4.
            file_size += cursor
                .write(&FILE_MAGIC_NUMBER.to_be_bytes())
                .await
                .context(error::IOSnafu)? as u64;
            cursor.try_flush(0).await?;
            None
        } else {
            let footer_data = match Reader::read_footer(&path).await {
                Ok((_, f)) => Some(f),
                Err(Error::NoFooter) => None,
                Err(e) => {
                    trace::error!(
                        "Failed to read footer of record_file '{}': {e}",
                        path.display()
                    );
                    return Err(e);
                }
            };

            // For writed file, skip to footer position, next write is at (file.len() - footer_len).
            // Note that footer_len may be zero.
            let seek_pos_end = footer_data.as_ref().map(|f| f.len()).unwrap_or(0);
            // TODO: truncate this file using seek_pos_end.
            cursor
                .seek(SeekFrom::End(-(seek_pos_end as i64)))
                .await
                .context(error::IOSnafu)?;

            footer_data
        };
        Ok(Writer {
            path: path.to_path_buf(),
            cursor,
            footer,
            file_size,
        })
    }

    // Writes record data and returns the written data size.
    pub async fn write_record(
        &mut self,
        data_version: u8,
        data_type: u8,
        data: &[&[u8]],
    ) -> Result<usize> {
        let data_len: usize = data.iter().map(|d| (*d).len()).sum();
        let data_len = match data_len.to_u32() {
            Some(v) => v,
            None => {
                return Err(Error::InvalidParam {
                    reason: format!(
                        "record(type: {}) length ({}) is not a valid u32, ignore this record",
                        data_type,
                        data.len()
                    ),
                });
            }
        };

        // Build record header and hash.
        let mut hasher = crc32fast::Hasher::new();
        let data_meta = [data_version, data_type];
        hasher.update(&data_meta);
        let data_len = data_len.to_be_bytes();
        hasher.update(&data_len);
        for d in data.iter() {
            hasher.update(d);
        }
        let data_crc = hasher.finalize().to_be_bytes();

        let magic_number = RECORD_MAGIC_NUMBER.to_be_bytes();
        let mut write_buf: Vec<IoSlice> = Vec::with_capacity(6);
        write_buf.push(IoSlice::new(&magic_number));
        write_buf.push(IoSlice::new(&data_meta));
        write_buf.push(IoSlice::new(&data_len));
        write_buf.push(IoSlice::new(&data_crc));
        for d in data {
            write_buf.push(IoSlice::new(d));
        }

        // Write record header and record data.
        let written_size =
            self.cursor
                .write_vec(&write_buf)
                .await
                .map_err(|e| Error::WriteFile {
                    path: self.path.clone(),
                    source: e,
                })?;
        self.file_size += written_size as u64;
        self.cursor.try_flush(0).await?;
        Ok(written_size)
    }

    pub async fn write_footer(&mut self, mut footer: Vec<u8>) -> Result<usize> {
        self.sync().await?;
        let pos = self.cursor.pos();
        // Get file crc
        self.cursor
            .seek(SeekFrom::Start(FILE_MAGIC_NUMBER_LEN as u64))
            .await?;
        let len = file_crc_source_len(self.file_size(), 0_usize);
        let buf = self.cursor.read(len).await.map_err(|e| Error::ReadFile {
            path: self.path.clone(),
            source: e,
        })?;
        let crc = crc32fast::hash(&buf);

        // Set file crc to footer
        footer[4..8].copy_from_slice(&crc.to_be_bytes());
        self.cursor.seek(SeekFrom::Start(pos)).await?;
        let res = self
            .cursor
            .write(&footer)
            .await
            .map_err(|e| Error::WriteFile {
                path: self.path.clone(),
                source: e,
            })?;
        self.cursor
            .try_flush(0)
            .await
            .map_err(|e| Error::WriteFile {
                path: self.path.clone(),
                source: e,
            })?;
        self.footer = Some(footer);
        Ok(res)
    }

    pub fn footer(&self) -> Option<Vec<u8>> {
        self.footer.clone()
    }

    pub async fn sync(&mut self) -> Result<()> {
        self.cursor.sync_all().await.context(error::SyncFileSnafu)
    }

    pub async fn close(&mut self) -> Result<()> {
        self.sync().await
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }
}

#[cfg(test)]
mod test {
    use serial_test::serial;

    use super::Writer;
    use crate::error::Result;
    use crate::file_system::file_manager;
    use crate::record_file::reader::test::{test_reader, test_reader_read_one};
    use crate::record_file::{RecordDataType, FILE_FOOTER_LEN};

    #[tokio::test]
    #[serial]
    async fn test_writer() -> Result<()> {
        let path = "/tmp/test/record_file/1/test.log";
        if file_manager::try_exists(path) {
            std::fs::remove_file(path).unwrap();
        }
        let mut w = Writer::open(&path, RecordDataType::Summary).await.unwrap();
        let data_vec = vec![b"hello".to_vec(); 10];
        for d in data_vec.iter() {
            let size = w.write_record(1, 1, &[d.as_slice()]).await?;
            println!("Writed new record(1, 1, {:?}) {} bytes", d, size);
        }
        w.write_footer([0_u8; FILE_FOOTER_LEN].to_vec()).await?;
        w.close().await?;

        println!("Testing read one record.");
        test_reader_read_one(&path, 23, b"hello").await;
        println!("Testing read all record.");
        test_reader(&path, &data_vec).await;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_writer_truncated() -> Result<()> {
        let path = "/tmp/test/record_file/2/test.log";
        if file_manager::try_exists(path) {
            std::fs::remove_file(path).unwrap();
        }
        let mut w = Writer::open(&path, RecordDataType::Summary).await.unwrap();
        let data_vec = vec![b"hello".to_vec(); 10];
        for d in data_vec.iter() {
            let size = w.write_record(1, 1, &[d.as_slice()]).await?;
            println!("Writed new record(1, 1, {:?}) {} bytes", d, size);
        }
        // Do not write footer.
        w.close().await?;

        println!("Testing read one record.");
        test_reader_read_one(&path, 23, b"hello").await;
        println!("Testing read all record.");
        test_reader(&path, &data_vec).await;
        Ok(())
    }
}
