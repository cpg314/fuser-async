use std::collections::HashSet;
use std::path::{Path, PathBuf};

use futures::stream::StreamExt;
use rusty_s3::S3Action;
use tracing::*;

use fuser_async::{utils, Refresh};

use super::{inode, DataBlockCache, S3Filesystem, CACHE_CLEANUP_S};

type FileEntry = (PathBuf, u64);

#[derive(thiserror::Error, Debug)]
pub enum RefreshError {
    #[error("Failed to acquire internal semaphore: {0}")]
    AcquireSemaphore(#[from] tokio::sync::AcquireError),
    #[error("Failed to access backend: {0}")]
    AccessingBackend(#[from] reqwest::Error),
    #[error("Failed deserializing XML response: {0}")]
    DeserializeBackendResponse(String),
    #[error("Failed parsing response: {0}")]
    ParseBackendResponse(#[from] std::string::FromUtf8Error),
}

impl<C: DataBlockCache> S3Filesystem<C> {
    async fn get_file_entries(&self) -> Result<Vec<FileEntry>, RefreshError> {
        let mut action =
            rusty_s3::actions::ListObjectsV2::new(&self.s3.bucket, Some(&self.s3.credentials));

        let fc = &self.s3.fetch_config;
        let semaphore =
            std::sync::Arc::new(tokio::sync::Semaphore::new(fc.listing_parallelism.max(1)));
        #[derive(Default)]
        struct TaskResult {
            folders: Vec<PathBuf>,
            files: Vec<FileEntry>,
        }
        let s3 = self.s3.clone();
        let s3_root = self.s3.root.clone();
        let task = |folder: PathBuf| async move {
            let depth = folder.components().count();

            let semaphore = semaphore.clone();
            let _permit = semaphore.acquire().await?;
            let parallelize = fc.listing_parallelism_depth.map_or(false, |p| depth <= p);
            {
                let query = action.query_mut();
                query.remove("delimiter");
                if parallelize {
                    query.insert("delimiter", "/");
                }
                if folder != Path::new("/") {
                    query.insert("prefix", folder.to_str().unwrap().to_string());
                };
            }
            let mut results = TaskResult::default();
            // Loop over pages
            let mut page = 0;
            loop {
                page += 1;
                let signed_url = action.sign(std::time::Duration::from_secs(3600));
                debug!(page, ?parallelize, depth, ?folder, "Reading folder",);

                let resp = s3.client.get(signed_url).send().await?.error_for_status()?;
                let mut parsed =
                    rusty_s3::actions::ListObjectsV2::parse_response(&resp.text().await?)
                        .map_err(|e| RefreshError::DeserializeBackendResponse(e.to_string()))?;
                for c in &mut parsed.contents {
                    if c.key.contains('%') {
                        c.key = urlencoding::decode(&c.key)?.into_owned();
                    }
                }
                debug!(
                    ?folder,
                    "Got {} files and {} folders",
                    parsed.contents.len(),
                    parsed.common_prefixes.len(),
                );
                let folder_str = folder.to_str().unwrap();
                results.folders.extend(
                    parsed
                        .common_prefixes
                        .into_iter()
                        .filter_map(|p| p.prefix.strip_prefix(folder_str).map(|f| folder.join(f))),
                );
                results.files.extend(
                    parsed
                        .contents
                        .into_iter()
                        .map(|o| (PathBuf::from(o.key.trim_start_matches(&s3_root)), o.size)),
                );
                if let Some(token) = parsed.next_continuation_token {
                    let query = action.query_mut();
                    // The rusty_s3 documentation is incorrect, and `insert` concatenates...
                    query.remove("continuation-token");
                    query.insert("continuation-token", token);
                } else {
                    break;
                }
            }

            Result::<_, RefreshError>::Ok(results)
        };

        let mut tasks = futures::stream::FuturesUnordered::new();
        tasks.push(task.clone()(PathBuf::from(&self.s3.root)));

        let mut entries: Vec<_> = vec![];
        while let Some(results) = tasks.next().await {
            let results = results?;
            entries.extend(results.files);
            tasks.extend(results.folders.into_iter().map(|f| task.clone()(f)));
        }
        Ok(entries)
    }
}

#[async_trait::async_trait]
impl<C: DataBlockCache> Refresh for S3Filesystem<C> {
    type Error = RefreshError;
    async fn cleanup(&self) -> Result<(), RefreshError> {
        debug!("Clearing cache");
        let now = utils::unix_timestamp();
        let inodes = self.inodes.read().await;
        for inode in inodes.0.values() {
            if let inode::Inode::File(inode) = inode {
                let last_release_s = now
                    .checked_sub(inode.last_release.load(std::sync::atomic::Ordering::SeqCst))
                    .unwrap_or(u64::MAX);
                if inode.handles.read().await.is_empty()
                    && (inode.cache.is_some().unwrap_or(false)
                        || inode.cache_direct.is_some().unwrap_or(false))
                    && last_release_s > CACHE_CLEANUP_S
                {
                    debug!(path = ?inode.path, "Clearing cache from inode");
                    for c in [&inode.cache, &inode.cache_direct] {
                        if let Err(e) = c.clear().await {
                            warn!(path = ?inode.path, "Failed to clear cache from inode: {}", e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn refresh(&mut self) -> Result<(), RefreshError> {
        info!("Refreshing S3 directory listing");
        let start = std::time::Instant::now();
        let contents = self.get_file_entries().await?;
        info!(
            "Got {} file entries in {:?}",
            contents.len(),
            start.elapsed()
        );

        let mut inodes = self.inodes.write().await;
        let paths: HashSet<PathBuf> = inodes.0.values().map(|i| i.path()).cloned().collect();

        let mut next_ino = inodes.next_inode();
        for (p, size) in contents {
            if paths.contains(&p) {
                continue;
            }
            let mut parent = if let Some(parent) = p.parent() {
                parent.to_path_buf()
            } else {
                continue;
            };

            // File inode
            let mut ino = next_ino;
            inodes.0.insert(
                next_ino,
                inode::Inode::File(inode::FileInode::new(&p, size)),
            );
            next_ino += 1;

            loop {
                // Add to directory table, and if necessary create new directory inode
                // Given that there is no notion of directory in S3, only prefixes,
                // we need to ensure that all parent directories are also added,
                // in case we have directories whose only children are other
                // directories.
                if let Some(v) = self.directory_table.get_mut(&parent) {
                    v.push(ino);
                    break;
                } else {
                    // Parent directory inode not present yet
                    let dir_ino = if parent == Path::new("") {
                        fuser::FUSE_ROOT_ID
                    } else {
                        let ino = next_ino;
                        next_ino += 1;
                        ino
                    };
                    inodes
                        .0
                        .insert(dir_ino, inode::Inode::Directory(parent.clone()));
                    self.directory_table.insert(parent.clone(), vec![ino]);
                    ino = dir_ino;
                    parent = if let Some(parent) = parent.parent() {
                        parent.to_path_buf()
                    } else {
                        break;
                    };
                }
            }
        }
        if inodes.0.is_empty() {
            inodes
                .0
                .insert(fuser::FUSE_ROOT_ID, inode::Inode::Directory(PathBuf::new()));
            self.directory_table.insert(PathBuf::new(), vec![]);
        }
        debug!(
            "After refresh, inode table has {} entries and directory table has {} entries",
            inodes.0.len(),
            self.directory_table.len()
        );
        Ok(())
    }
}
