use tracing::*;

use fuser_async::cache;
use fuser_async::rwlockoption::RwLockOptionGuard;

use super::{DataBlockCache, Error};

pub struct FileHandle<C: DataBlockCache> {
    pub id: u64,
    pub client: reqwest::Client,
    pub signed_url: reqwest::Url,
    pub cache: Option<RwLockOptionGuard<C>>,
    pub size: u64,
}
impl<C: DataBlockCache> FileHandle<C> {
    fn n_blocks(&self) -> u64 {
        self.cache
            .as_ref()
            .map(|cache| (self.size as f64 / cache.stats().block_size as f64).ceil() as u64)
            .unwrap_or_default()
    }
    async fn get_block(
        &self,
        id: u64,
    ) -> Result<tokio::sync::RwLockReadGuard<cache::Block>, Error> {
        let blocks = if let Some(blocks) = &self.cache {
            blocks
        } else {
            return Err(Error::NoCache);
        };
        if let Some(bl) = blocks.get(id).await {
            return Ok(bl);
        }
        let data = blocks
            .insert_lock(id, async {
                debug!(
                    handle = self.id,
                    block = id,
                    "Fetching block {}/{}",
                    id,
                    self.n_blocks(),
                );
                let bs = blocks.stats().block_size;
                let offset = bs * id;
                let data = self.fetch(offset, bs as u32).await?;
                anyhow::ensure!(data.len() <= bs as usize);
                Ok(data)
            })
            .await?;
        // TODO: Prefetching?
        Ok(data)
    }
    async fn fetch_impl(&self, offset: u64, size: u32) -> Result<bytes::Bytes, Error> {
        if offset >= self.size {
            return Err(Error::Base(fuser_async::Error::InvalidArgument));
        }
        let mut size = size as u64;
        size = size.min(self.size - offset);

        let start = std::time::Instant::now();
        let resp = self
            .client
            .get(self.signed_url.clone())
            .header(
                "Range",
                // End is inclusive
                &format!("bytes={}-{}", offset, offset + size - 1),
            )
            .send()
            .await?
            .error_for_status()?;
        let data = resp.bytes().await?;
        info!(
            speed_mb_s = data.len() as f64 / 1e6 / start.elapsed().as_secs_f64(),
            size, "Fetched from S3 server"
        );
        Ok(data)
    }
    async fn fetch(&self, offset: u64, size: u32) -> Result<bytes::Bytes, Error> {
        debug!(
            offset,
            size,
            url = %self.signed_url,
            total_size = self.size,
            "Fetching from s3 server"
        );
        super::retry(
            || async move { self.fetch_impl(offset, size).await },
            "fetching",
        )
        .await
    }

    pub async fn get(&self, offset: u64, size: u32) -> Result<bytes::Bytes, Error> {
        let start = offset;
        let end = offset + size as u64;

        if let Some(blocks) = &self.cache {
            let mut data = bytes::BytesMut::zeroed(size as usize);

            let mut pos = 0;
            let bs = blocks.stats().block_size;
            for bl in (start / bs)..=(end / bs) {
                let block_start = bl * bs;
                let block = self.get_block(bl).await?;
                let block = &block.data;

                let block_end = block_start + block.len() as u64;

                let start_rel = start.checked_sub(block_start).unwrap_or_default();
                let end_rel = if block_end < end {
                    block.len()
                } else {
                    (end - block_start) as usize
                };
                let block = &block[start_rel as usize..end_rel as usize];
                data[pos..pos + block.len()].copy_from_slice(block);
                pos += block.len();
            }
            if data.len() != size as usize {
                return Err(Error::ShortRead {
                    read: size as usize,
                    expected: data.len(),
                });
            }
            Ok(data.freeze())
        } else {
            self.fetch(offset, size).await
        }
    }
}
