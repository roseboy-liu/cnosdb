use std::io::SeekFrom;

use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::error::Result;
use crate::tsm2::page::{Footer, FOOTER_SIZE};

pub async fn read_metadata<R: AsyncRead + AsyncSeek + Send + std::marker::Unpin>(
    reader: &mut R,
) -> Result<Footer> {
    reader.seek(SeekFrom::End(-FOOTER_SIZE)).await?;
    let mut buffer = [0u8; FOOTER_SIZE as usize];
    reader.read_exact(&mut buffer).await?;
    Ok(Footer::from(&buffer))
}
