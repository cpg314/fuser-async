use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use rusty_s3::S3Action;
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::*;

use super::{DataBlockCache, Error, S3Data, HANDLE_EXPIRATION_S};

pub const BUFFER_SIZE: usize = 500_000_000;
const MAX_PARTS: u16 = 10_000;
const MIN_PART_SIZE: u64 = 5_000_000;

struct BufferPool {
    size: usize,
}
#[async_trait::async_trait]
impl deadpool::managed::Manager for BufferPool {
    type Type = bytes::BytesMut;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        Ok(Self::Type::with_capacity(self.size))
    }
    async fn recycle(&self, buf: &mut Self::Type) -> deadpool::managed::RecycleResult<Self::Error> {
        buf.clear();
        // This should not reallocate
        // https://docs.rs/bytes/latest/bytes/struct.BytesMut.html#method.reserve
        buf.reserve(BUFFER_SIZE);
        Ok(())
    }
}
#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn parts_test() -> Result<(), Error> {
        let mut parts = Parts::default();
        assert_eq!(50, parts.add(150, 10)?);
        assert_eq!(1, parts.add(0, 10)?);
        assert_eq!(25, parts.add(100, 10)?);
        assert_eq!(2, parts.add(10, 10)?);
        assert_eq!(37, parts.add(120, 10)?);
        assert_eq!(51, parts.add(160, 10)?);
        println!("{}", parts);

        let mut parts = Parts::default();
        for i in 0..(MAX_PARTS - 1) {
            parts.add(i as u64, 1)?;
        }
        assert!(parts.add(MAX_PARTS as u64 - 1, 0).is_err());
        Ok(())
    }
}
#[derive(Debug)]
struct WritePart {
    size: u64,
    part: u16,
    etag: String,
}
#[derive(Default, Debug)]
struct Parts(BTreeMap<u64 /* offset */, WritePart>);
impl std::fmt::Display for Parts {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for (offset, part) in &self.0 {
            write!(
                f,
                "Part {}: [{}, {}), size {} MB ",
                part.part,
                offset,
                offset + part.size,
                part.size as f64 / 1e6
            )?;
        }
        Ok(())
    }
}

impl Parts {
    fn size_mb(&self) -> f64 {
        self.0.values().map(|x| x.size as f64 / 1e6).sum::<f64>()
    }
    fn add(&mut self, offset: u64, size: u64) -> Result<u16, Error> {
        debug!(
            "Adding offset {} and size {} to {} existing parts, total size {} MB",
            offset,
            size,
            self.0.len(),
            self.size_mb()
        );
        let before = self.0.range(..offset).last();
        let after = self.0.range((offset + 1)..).next();
        let mut contiguous = None;
        if let Some(before) = &before {
            let before_end = before.0 + before.1.size;
            #[allow(clippy::comparison_chain)]
            if before_end > offset {
                return Err(Error::WriteTwice);
            } else if before_end == offset {
                contiguous = Some(before.1.part + 1);
            }
        }
        if let Some(after) = &after {
            let new_end = offset + size;
            if new_end > *after.0 {
                return Err(Error::WriteTwice);
            } else if *after.0 > 1 && *after.0 == new_end {
                contiguous = Some(after.1.part - 1);
            }
        }
        let parts_avail = before.map(|b| b.1.part + 1).unwrap_or_default()
            ..after.map(|a| a.1.part - 1).unwrap_or(MAX_PARTS);

        if parts_avail.is_empty() {
            return Err(Error::FindPart);
        }
        let len = (parts_avail.len() as u16).min(100);
        let mut part = parts_avail.start + if len % 2 == 0 { len / 2 } else { (len - 1) / 2 };
        if let Some(contiguous) = contiguous {
            if parts_avail.contains(&contiguous) {
                part = contiguous;
            }
        }
        if offset == 0 && parts_avail.contains(&0) {
            part = 1;
        }

        if part == MAX_PARTS {
            return Err(Error::FindPart);
        }

        self.0.insert(
            offset,
            WritePart {
                etag: Default::default(),
                size,
                part,
            },
        );
        Ok(part)
    }
}
pub struct FileWriteInode {
    pub path: PathBuf,
    pub write: Mutex<Status>,
}
pub enum Status {
    Writing(Write),
    Done(u64),
}
pub struct Write {
    path: PathBuf,
    s3_data: Arc<S3Data>,
    upload_id: String,
    parts: Mutex<Parts>,
    buffer: Mutex<Option<Buffer>>,
    buffer_pool: deadpool::managed::Pool<BufferPool>,
    #[allow(clippy::type_complexity)]
    tasks: Mutex<Vec<tokio::task::JoinHandle<Result<(u64, String), Error>>>>,
}
struct Buffer {
    offset: u64,
    inner: deadpool::managed::Object<BufferPool>,
}

impl FileWriteInode {
    pub async fn finish(&self) -> Result<u64, Error> {
        let mut status = self.write.lock().await;
        let w = std::mem::replace(&mut *status, Status::Done(0));
        if let Status::Writing(w) = w {
            let size = w.finish().await?;
            *status = Status::Done(size);
            Ok(size)
        } else {
            Err(Error::AlreadyClosed)
        }
    }
    pub async fn add_data(&self, offset: u64, data: bytes::Bytes) -> Result<(), Error> {
        let mut status = self.write.lock().await;
        if let Status::Writing(w) = &mut *status {
            w.add_data(offset, data).await
        } else {
            Err(fuser_async::Error::BadFileDescriptor.into())
        }
    }
    pub async fn new<C: DataBlockCache>(path: &Path, s3_data: Arc<S3Data>) -> Result<Self, Error> {
        let path = Path::new(&s3_data.root).join(path);
        debug!(?path, "Starting file upload");
        let action = rusty_s3::actions::CreateMultipartUpload::new(
            &s3_data.bucket,
            Some(&s3_data.credentials),
            path.to_str().unwrap(),
        );
        let signed_url = action.sign(std::time::Duration::from_secs(HANDLE_EXPIRATION_S));
        let body = s3_data
            .client
            .post(signed_url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let resp = rusty_s3::actions::CreateMultipartUpload::parse_response(&body)
            .map_err(|e| Error::S3Error(format!("Failed to deserialize response: {}", e)))?;

        let upload_id = resp.upload_id().to_string();
        debug!(?path, upload_id, "File upload started");
        Ok(Self {
            path: path.clone(),
            write: Mutex::new(Status::Writing(Write {
                path,
                s3_data,
                buffer: Default::default(),
                buffer_pool: deadpool::managed::Pool::builder(BufferPool { size: BUFFER_SIZE })
                    .max_size(4)
                    .build()
                    .map_err(|_| {
                        Error::Base(fuser_async::Error::io("Failed to create buffer pool"))
                    })?,
                upload_id,
                parts: Default::default(),
                tasks: Default::default(),
            })),
        })
    }
}
impl Write {
    pub async fn add_data(&self, offset: u64, data: bytes::Bytes) -> Result<(), Error> {
        debug!(offset, size = data.len(), "Adding data to file");

        let mut buffer = self.buffer.lock().await;
        let upload_current = buffer.as_ref().map_or(false, |buffer| {
            buffer.inner.len() + data.len() > buffer.inner.capacity()
                || buffer.offset + buffer.inner.len() as u64 != offset
        });
        if upload_current {
            debug!("Uploading current buffer because of non-contiguity or over capacity");
            // When `buffer` is dropped by the task, the buffer will be released to the pool
            let buffer = buffer.take().unwrap();
            self.upload_part(buffer).await?;
        }

        match &mut *buffer {
            Some(buffer) => {
                debug!(
                    buffer_len = buffer.inner.len(),
                    buffer_cap = buffer.inner.capacity(),
                    size = data.len(),
                    "Adding data to the current buffer"
                );
                buffer.inner.extend(data);
            }
            None => {
                debug!(size = data.len(), "Adding data to a new buffer");
                let mut inner = self.buffer_pool.get().await.map_err(|_| {
                    Error::Base(fuser_async::Error::io("Failed to get buffer from pool"))
                })?;
                inner.extend(data);
                *buffer = Some(Buffer { offset, inner });
            }
        }
        Ok(())
    }
    async fn upload_part(&self, mut buffer: Buffer) -> Result<(), Error> {
        let start = std::time::Instant::now();
        debug!(
            size = buffer.inner.len(),
            buffer.offset, self.upload_id, "Uploading part"
        );
        let size = buffer.inner.len() as u64;

        let part = {
            let mut parts = self.parts.lock().await;
            parts.add(buffer.offset, size)?
        };

        debug!(self.upload_id, part, "Determined part number");

        let action = rusty_s3::actions::UploadPart::new(
            &self.s3_data.bucket,
            Some(&self.s3_data.credentials),
            self.path.to_str().unwrap(),
            part,
            &self.upload_id,
        );
        let url = action.sign(std::time::Duration::from_secs(HANDLE_EXPIRATION_S));
        let s3_data = self.s3_data.clone();
        let task = tokio::task::spawn(async move {
            let data = buffer.inner.split().freeze();
            let upload = || async {
                Ok::<_, Error>(
                    s3_data
                        .client
                        .put(url.clone())
                        .body(data.clone())
                        .send()
                        .await?
                        .error_for_status()?,
                )
            };

            let resp = super::retry(upload, "uploading").await?;
            let etag = resp
                .headers()
                .get(reqwest::header::ETAG)
                .and_then(|h| h.to_str().ok())
                .ok_or_else(|| Error::S3Error("Missing or invalid etag".into()))?
                .to_string();
            let speed_mb_s = size as f64 / 1e6 / start.elapsed().as_secs_f64();
            info!(duration=?start.elapsed(), part, etag, size, speed_mb_s, "Finished uploading part");
            Ok((buffer.offset, etag))
        });
        let mut tasks = self.tasks.lock().await;
        tasks.push(task);
        Ok(())
    }
    pub async fn finish(self) -> Result<u64, Error> {
        debug!(self.upload_id, "Completing upload");
        let mut buffer = self.buffer.lock().await;
        if let Some(buffer) = buffer.take() {
            debug!("Uploading remaining buffer");
            self.upload_part(buffer).await?;
        }
        let tasks = self.tasks.into_inner();
        let mut parts = self.parts.lock().await;
        debug!("{}", parts);
        debug!("Total file size: {} MB", parts.size_mb());
        let parts = &mut parts.0;
        debug!("Waiting for {} uploads to complete", parts.len());
        for t in tasks {
            let (offset, etag) = t.await.map_err(|e| {
                Error::Base(fuser_async::Error::IO(format!(
                    "Failed to upload data: {}",
                    e
                )))
            })??;
            let part = parts.get_mut(&offset).unwrap();
            part.etag = etag;
        }
        let size = parts.iter().map(|(_, p)| p.size).sum::<u64>();
        for (p1, p2) in parts.iter().tuple_windows() {
            if p1.0 + p1.1.size != *p2.0 {
                return Err(Error::S3Error(format!(
                    "Non-continugous parts: {:?}",
                    parts
                )));
            }
        }
        if !parts.contains_key(&0) {
            return Err(Error::S3Error("Start of file not uploaded".into()));
        }
        if parts.iter().rev().skip(1).any(|p| p.1.size < MIN_PART_SIZE) {
            return Err(Error::S3Error(format!(
                "All chunks but the last must be > {} bytes",
                MIN_PART_SIZE
            )));
        }
        let action = rusty_s3::actions::CompleteMultipartUpload::new(
            &self.s3_data.bucket,
            Some(&self.s3_data.credentials),
            self.path.to_str().unwrap(),
            &self.upload_id,
            parts.values().map(|x| x.etag.as_str()),
        );
        let url = action.sign(std::time::Duration::from_secs(HANDLE_EXPIRATION_S));
        // rusty_s3 does not support an arbitrary part number, so we create the request body
        // ourselves.
        #[derive(Serialize)]
        struct CompleteMultipartUpload {
            #[serde(rename = "Part")]
            parts: Vec<Part>,
        }

        #[derive(Serialize)]
        struct Part {
            #[serde(rename = "$value")]
            nodes: Vec<Node>,
        }

        #[derive(Serialize)]
        enum Node {
            ETag(String),
            PartNumber(u16),
        }
        let body = CompleteMultipartUpload {
            parts: parts
                .iter()
                .map(|(_, p)| Part {
                    nodes: vec![Node::PartNumber(p.part), Node::ETag(p.etag.clone())],
                })
                .collect(),
        };
        let body = quick_xml::se::to_string(&body)
            .map_err(|_| Error::S3Error("Failed to serialize completion body".into()))?;
        let finalize = || async {
            self.s3_data
                .client
                .post(url.clone())
                .body(body.clone())
                .send()
                .await?
                .error_for_status()
        };
        super::retry(finalize, "finalizing").await?;
        debug!(self.upload_id, "Finished uploading file");
        Ok(size)
    }
}
