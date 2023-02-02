use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use rusty_s3::S3Action;
use tokio::sync::RwLock;
use tracing::*;

use fuser_async::rwlockoption::RwLockOption;
use fuser_async::{utils, DirEntry};

use super::{
    handle::FileHandle, write::FileWriteInode, DataBlockCache, Error, S3Data, S3Filesystem,
    HANDLE_EXPIRATION_S,
};

const CACHE_DIRECT_BLOCK_SIZE_MB: u64 = 2;
pub const CACHE_DIRECT_SIZE_MB: u64 = 100 * CACHE_DIRECT_BLOCK_SIZE_MB;

pub enum Inode<C: DataBlockCache> {
    File(FileInode<C>),
    FileWrite(FileWriteInode),
    Directory(PathBuf),
}

impl<C: DataBlockCache> Inode<C> {
    pub fn path(&self) -> &PathBuf {
        match self {
            Inode::File(FileInode { path, .. }) => path,
            Inode::FileWrite(f) => &f.path,
            Inode::Directory(p) => p,
        }
    }
    pub fn name(&self) -> &OsStr {
        self.path().file_name().unwrap()
    }
    pub fn dir_entry(&self, ino: u64) -> DirEntry {
        DirEntry {
            inode: ino,
            name: self.name().to_str().unwrap().into(),
            file_type: match self {
                Self::File(..) | Self::FileWrite(..) => fuser::FileType::RegularFile,
                Self::Directory(..) => fuser::FileType::Directory,
            },
        }
    }
    pub async fn attrs(&self, ino: u64, fs: &S3Filesystem<C>) -> Result<fuser::FileAttr, Error> {
        match self {
            Inode::File(FileInode { size, .. }) => Ok(utils::file_attr(ino, *size, UNIX_EPOCH)),
            Inode::FileWrite(..) => Ok(utils::file_attr(ino, 0, UNIX_EPOCH)),
            Inode::Directory(p) => Ok(utils::dir_attr(
                ino,
                fs.get_directory_from_path(p)?.len() as u32 + 2,
                UNIX_EPOCH,
            )),
        }
    }
}

pub struct FileInode<C: DataBlockCache> {
    // TODO: Only store the filename
    pub path: PathBuf,
    pub size: u64,
    pub handles: RwLock<BTreeMap<u64, FileHandle<C>>>,
    pub cache: RwLockOption<C>,
    pub cache_direct: RwLockOption<C>,
    pub last_release: std::sync::atomic::AtomicU64,
}
impl<C: DataBlockCache> FileInode<C> {
    pub(crate) fn new(path: &Path, size: u64) -> Self {
        Self {
            path: path.into(),
            size,
            handles: RwLock::default(),
            cache: RwLockOption::new(),
            cache_direct: RwLockOption::new(),
            last_release: std::sync::atomic::AtomicU64::new(utils::unix_timestamp()),
        }
    }
    // The O_NONBLOCK flag will use blocks from the `cache_direct` pool instead of the `cache` pool.
    // The O_DIRECT flag will disable caching altogether. This is dangerous.
    pub(crate) async fn new_handle(&self, s3_data: &Arc<S3Data>, flags: i32) -> Result<u64, Error> {
        let path = PathBuf::from(&s3_data.root).join(&self.path);
        debug!(
            "Creating new handle for {:?} (absolute path: {:?})",
            self.path, path
        );
        let action = rusty_s3::actions::GetObject::new(
            &s3_data.bucket,
            Some(&s3_data.credentials),
            path.to_str().unwrap(),
        );
        let direct = (flags & libc::O_DIRECT) != 0;
        let nonblock = (flags & libc::O_NONBLOCK) != 0;
        let fetch_size_bytes = s3_data.fetch_config.fetch_size_bytes();
        let signed_url = action.sign(std::time::Duration::from_secs(HANDLE_EXPIRATION_S));
        let cache = if self.size < fetch_size_bytes {
            // Direct fetching
            None
        } else if nonblock {
            // Fetching with smaller blocks
            Some(
                self.cache_direct
                    .get_or_try_init(|| async {
                        C::new(
                            (CACHE_DIRECT_SIZE_MB).min(self.size),
                            CACHE_DIRECT_BLOCK_SIZE_MB * 1e6 as u64,
                            self.size,
                        )
                    })
                    .await?,
            )
        } else if direct {
            // Direct fetching
            None
        } else {
            // Fetching with regular blocks
            Some(
                self.cache
                    .get_or_try_init(|| async {
                        C::new(
                            (s3_data.fetch_config.file_cache_mb).min(self.size),
                            fetch_size_bytes,
                            self.size,
                        )
                    })
                    .await?,
            )
        };

        let mut handles = self.handles.write().await;
        let id = handles.keys().last().copied().unwrap_or_default() + 1;

        let handle = FileHandle {
            id,
            signed_url,
            cache,
            client: s3_data.client.clone(),
            size: self.size,
        };

        handles.insert(id, handle);
        debug!(handle = id, "Added file handle, total {}", handles.len());
        Ok(id)
    }
    pub async fn release_handle(&self, fh: u64) -> Result<(), Error> {
        let mut handles = self.handles.write().await;
        handles.remove(&fh);
        Ok(())
    }
}
