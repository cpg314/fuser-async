use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fuser::TimeOrNow;
use tokio::sync::RwLock;
use tracing::*;

use super::{error::Error, Filesystem};

const TTL: Duration = Duration::from_secs(5);

/// Wrapper around a [`Filesystem`], implementing [`fuser::Filesystem`].
pub struct FilesystemFUSE<T> {
    inner: Arc<RwLock<T>>,
    handle: tokio::runtime::Handle,
}
impl<T> Clone for FilesystemFUSE<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            handle: self.handle.clone(),
        }
    }
}

impl<T: Filesystem + Send + Sync> FilesystemFUSE<T> {
    /// Create from a [`Filesystem`].
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
            handle: tokio::runtime::Handle::current(),
        }
    }
    /// Return a read lock guard on the inner [`Filesystem`].
    pub async fn inner(&self) -> tokio::sync::RwLockReadGuard<T> {
        let inner = &*self.inner;
        inner.read().await
    }
    /// Return a write lock guard on the inner [`Filesystem`].
    pub async fn inner_mut(&self) -> tokio::sync::RwLockWriteGuard<T> {
        let inner = &*self.inner;
        inner.write().await
    }
    /// Try to return a read-write lock guard on the inner [`Filesystem`].
    /// Fails if the are already locks (e.g. ongoing filesystem operations).
    pub async fn try_inner_mut(
        &self,
    ) -> Result<tokio::sync::RwLockWriteGuard<T>, tokio::sync::TryLockError> {
        self.inner.try_write()
    }
}
impl<T: Filesystem + Send + Sync + 'static> fuser::Filesystem for FilesystemFUSE<T>
where
    T::Error: std::fmt::Display,
{
    fn init(
        &mut self,
        _req: &fuser::Request,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        Ok(())
    }
    fn destroy(&mut self) {
        debug!("Cleaning up filesystem");
        let mut inner = self.inner.clone();
        self.handle.spawn(async move {
            if let Err(e) = inner.destroy().await {
                error!("Error while destroying filesystem: {}", e);
            }
        });
    }
    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        debug!(ino, "Opening file handle for inode");

        let inner = self.inner.clone();
        self.handle.spawn(async move {
            match inner.open(ino, flags).await {
                Ok(fh) => {
                    reply.opened(fh, fuser::consts::FOPEN_KEEP_CACHE);
                }
                Err(e) => {
                    warn!(ino, "Error when opening file: {}", e);
                    let e: Error = e.into();
                    reply.error((&e).into());
                }
            }
        });
    }
    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(ino, fh, "Releasing file handle");

        let inner = self.inner.clone();
        self.handle.spawn(async move {
            match inner.clone().release(ino, fh).await {
                Ok(_) => {
                    reply.ok();
                }
                Err(e) => {
                    warn!(ino, fh, "Error when closing file: {}", e);
                    let e: Error = e.into();
                    reply.error((&e).into());
                }
            }
        });
    }
    fn lookup(
        &mut self,
        _req: &fuser::Request,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        debug!(parent, ?name, "Looking up directory entry");
        let inner = self.inner.clone();
        let name = name.to_owned();
        self.handle.spawn(async move {
            match inner.lookup(parent, &name).await {
                Ok(attr) => {
                    debug!("Sending directory entry response");
                    reply.entry(&TTL, &attr, 0);
                }
                Err(e) => {
                    debug!(
                        parent,
                        "Error when looking up entry {:?} in directory: {}", name, e
                    );
                    let e: Error = e.into();
                    reply.error((&e).into());
                }
            }
        });
    }
    fn getattr(&mut self, _req: &fuser::Request, ino: u64, reply: fuser::ReplyAttr) {
        debug!(ino, "Getting attribute");
        let inner = self.inner.clone();
        self.handle.spawn(async move {
            match inner.getattr(ino).await {
                Ok(attr) => {
                    reply.attr(&TTL, &attr);
                }
                Err(e) => {
                    warn!("Error when getting inode attribute: {}", e);
                    let e: Error = e.into();
                    reply.error((&e).into());
                }
            }
        });
    }
    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        debug!(ino, "Setting attributes");
        // TODO: Check for ignored attributes
        let mut inner = self.inner.clone();
        self.handle.spawn(async move {
            match inner.setattr(ino, size).await {
                Ok(attr) => {
                    reply.attr(&TTL, &attr);
                }
                Err(e) => {
                    warn!("Error when setting attributes: {}", e);
                    let e: Error = e.into();
                    reply.error((&e).into());
                }
            };
        });
    }
    fn readdir(
        &mut self,
        _req: &fuser::Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        debug!(ino, "Reading directory");
        if offset < 0 {
            reply.error((&Error::InvalidArgument).into());
            return;
        }
        let inner = self.inner.clone();
        self.handle.spawn(async move {
            match inner.readdir(ino, offset as u64).await {
                Ok(it) => {
                    for (mut i, e) in it.enumerate() {
                        i += offset as usize;
                        if reply.add(e.inode, (i + 1) as i64, e.file_type, &e.name) {
                            break;
                        }
                    }
                    reply.ok();
                }
                Err(e) => {
                    warn!("Error when reading directory: {}", e);
                    let e: Error = e.into();
                    reply.error((&e).into());
                }
            };
        });
    }
    fn read(
        &mut self,
        _req: &fuser::Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let start = std::time::Instant::now();
        debug!(ino, fh, "Reading {} bytes from {}", size, offset);
        let inner = self.inner.clone();
        self.handle.spawn(async move {
            if offset < 0 {
                error!("Negative offsets not supported");
                reply.error(libc::EINVAL);
                return;
            }
            match inner.read(ino, fh, offset, size).await {
                Ok(data) => {
                    debug!(
                        ino,
                        fh,
                        speed_ms_s = data.len() as f64 / 1e6 / start.elapsed().as_secs_f64(),
                        "Read {} bytes from {}",
                        data.len(),
                        offset,
                    );
                    reply.data(&data);
                }
                Err(e) => {
                    error!(ino, fh, "Error when reading file: {}", e);
                    let e: Error = e.into();
                    reply.error((&e).into());
                }
            }
        });
    }
    fn write(
        &mut self,
        _req: &fuser::Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let start = std::time::Instant::now();
        debug!(ino, fh, "Writing {} bytes from {}", data.len(), offset);
        let inner = self.inner.clone();
        let data = bytes::Bytes::copy_from_slice(data);
        self.handle.spawn(async move {
            if offset < 0 {
                error!("Negative offsets not supported");
                reply.error(libc::EINVAL);
                return;
            }
            match inner.write(ino, fh, data, offset).await {
                Ok(written) => {
                    debug!(
                        ino,
                        fh,
                        speed_ms_s = written as f64 / 1e6 / start.elapsed().as_secs_f64(),
                        "Wrote {} bytes from {}",
                        written,
                        offset,
                    );
                    reply.written(written);
                }
                Err(e) => {
                    error!(ino, fh, "Error when writing file: {}", e);
                    let e: Error = e.into();
                    reply.error((&e).into());
                }
            }
        });
    }
    fn create(
        &mut self,
        _req: &fuser::Request,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        debug!("Creating file");
        let mut inner = self.inner.clone();
        let name = name.into();
        self.handle.spawn(async move {
            match inner.create(parent, name, mode, umask, flags).await {
                Ok((attr, fh)) => {
                    reply.created(&TTL, &attr, 0, fh, 0);
                }
                Err(e) => {
                    warn!("Error when creating file: {}", e);
                    let e: Error = e.into();
                    reply.error((&e).into());
                }
            }
        });
    }
    fn mkdir(
        &mut self,
        _req: &fuser::Request,
        parent: u64,
        name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let mut inner = self.inner.clone();
        let name = name.into();
        self.handle.spawn(async move {
            match inner.mkdir(parent, name).await {
                Ok(attr) => {
                    reply.entry(&TTL, &attr, 0);
                }
                Err(e) => {
                    warn!("Error when creating node: {}", e);
                    let e: Error = e.into();
                    reply.error((&e).into());
                }
            }
        });
    }
}
