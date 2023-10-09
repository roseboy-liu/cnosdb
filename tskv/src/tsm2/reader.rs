use std::io::SeekFrom;

use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::error::Result;
use crate::tsm2::page::Footer;
use crate::tsm2::FOOTER_SIZE;

pub async fn read_metadata<R: AsyncRead + AsyncSeek + Send + Unpin>(
    reader: &mut R,
) -> Result<Footer> {
    reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64))).await?;
    let mut buffer = [0u8; FOOTER_SIZE];
    reader.read_exact(&mut buffer).await?;
    Footer::deserialize(&buffer)
}


