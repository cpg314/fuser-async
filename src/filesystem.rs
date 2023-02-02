use std::collections::{BTreeSet, VecDeque};
use std::ffi::{OsStr, OsString};
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::DirEntry;

/// Base trait providing asynchronous functions for the system calls.
///
/// WARNING: The [`crate::FilesystemFUSE`] struct encapsulates implementors in a [`RwLock`].
///          Therefore, care should be taken to not introduce deadlocks.
///          An example would be if `open` waits on a `release` (e.g. to limit the number of open
///          files), but the `open` acquires a write lock before any `release`.
#[async_trait::async_trait]
pub trait Filesystem {
    type Error: Into<crate::error::Error>;
    /// Close filesystem. See also [`fuser::Filesystem::destroy`].
    async fn destroy(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    /// Lookup an entry by name in a directory. See also [`fuser::Filesystem::lookup`].
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<fuser::FileAttr, Self::Error>;
    /// Open a file and get a handle. See also [`fuser::Filesystem::open`].
    async fn open(&self, _ino: u64, _flags: i32) -> Result<u64, Self::Error> {
        Ok(0)
    }
    /// Release a file handle. See also [`fuser::Filesystem::release`].
    async fn release(&self, _ino: u64, _fh: u64) -> Result<(), Self::Error> {
        Ok(())
    }
    /// Get attributes on an entry. See also [`fuser::Filesystem::getattr`].
    async fn getattr(&self, _ino: u64) -> Result<fuser::FileAttr, Self::Error>;
    /// Set attributes. Currently only supports setting the size
    async fn setattr(
        &mut self,
        ino: u64,
        size: Option<u64>,
    ) -> Result<fuser::FileAttr, Self::Error>;
    /// Read a directory.
    /// To be called repeatedly by specifying a gradually increasing offset,
    /// until the returned iterator is empty. A minimum of two calls is required
    /// to be certain that the end has been reached.
    /// `offset` represents the index of the starting element.
    /// See also [`fuser::Filesystem::readdir`].
    async fn readdir(
        &self,
        ino: u64,
        offset: u64,
    ) -> Result<Box<dyn Iterator<Item = DirEntry> + Send + Sync + '_>, Self::Error>;
    /// Read from a file. See also [`fuser::Filesystem::read`].
    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> Result<bytes::Bytes, Self::Error>;
    /// Write to file. See also [`fuser::Filesystem::write`].
    async fn write(
        &self,
        ino: u64,
        fh: u64,
        data: bytes::Bytes,
        offset: i64,
    ) -> Result<u32, Self::Error>;
    /// Create and open a file. See also [`fuser::Filesystem::create`].
    async fn create(
        &mut self,
        parent: u64,
        name: OsString,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> Result<(fuser::FileAttr, u64), Self::Error>;
    /// Create a directory. See also [`fuser::Filesystem::mkdir`].
    async fn mkdir(&mut self, parent: u64, name: OsString) -> Result<fuser::FileAttr, Self::Error>;
    /// Get the set of inodes from the filesystem.
    ///
    /// Implementors can usually provide a more efficient implementation than the blanket one,
    /// which performs BFS from the root.
    async fn inodes(&self) -> Result<BTreeSet<u64>, Self::Error> {
        let mut inodes = BTreeSet::from([fuser::FUSE_ROOT_ID]);
        let mut queue = VecDeque::from([fuser::FUSE_ROOT_ID]);
        while let Some(ino) = queue.pop_front() {
            for entry in self.readdir(ino, 0).await? {
                inodes.insert(entry.inode);
                if entry.file_type == fuser::FileType::Directory {
                    queue.push_back(entry.inode);
                }
            }
        }
        Ok(inodes)
    }
}
#[async_trait::async_trait]
impl<T: Filesystem + Send + Sync + 'static> Filesystem for Arc<RwLock<T>> {
    type Error = T::Error;
    async fn destroy(&mut self) -> Result<(), Self::Error> {
        let mut x = self.as_ref().write().await;
        x.destroy().await
    }
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<fuser::FileAttr, Self::Error> {
        let x = self.as_ref().read().await;
        x.lookup(parent, name).await
    }
    async fn open(&self, ino: u64, flags: i32) -> Result<u64, Self::Error> {
        let x = self.as_ref().read().await;
        x.open(ino, flags).await
    }
    async fn release(&self, ino: u64, fh: u64) -> Result<(), Self::Error> {
        let x = self.as_ref().read().await;
        x.release(ino, fh).await
    }
    async fn getattr(&self, ino: u64) -> Result<fuser::FileAttr, Self::Error> {
        let x = self.as_ref().read().await;
        x.getattr(ino).await
    }
    async fn setattr(
        &mut self,
        ino: u64,
        size: Option<u64>,
    ) -> Result<fuser::FileAttr, Self::Error> {
        let mut x = self.as_ref().write().await;
        x.setattr(ino, size).await
    }
    async fn readdir(
        &self,
        ino: u64,
        offset: u64,
    ) -> Result<Box<dyn Iterator<Item = DirEntry> + Send + Sync + '_>, Self::Error> {
        let x = self.as_ref().read().await;
        let dir = x.readdir(ino, offset).await?;
        // TODO: Avoid the collect (that would likely mean returning a `Stream` instead of an `Iterator`)
        Ok(Box::new(dir.collect::<Vec<_>>().into_iter()))
    }
    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> Result<bytes::Bytes, Self::Error> {
        let x = self.as_ref().read().await;
        x.read(ino, fh, offset, size).await
    }
    async fn write(
        &self,
        ino: u64,
        fh: u64,
        data: bytes::Bytes,
        offset: i64,
    ) -> Result<u32, Self::Error> {
        let x = self.as_ref().read().await;
        x.write(ino, fh, data, offset).await
    }
    async fn create(
        &mut self,
        parent: u64,
        name: OsString,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> Result<(fuser::FileAttr, u64), Self::Error> {
        let mut x = self.as_ref().write().await;
        x.create(parent, name, mode, umask, flags).await
    }
    async fn mkdir(&mut self, parent: u64, name: OsString) -> Result<fuser::FileAttr, Self::Error> {
        let mut x = self.as_ref().write().await;
        x.mkdir(parent, name).await
    }
    async fn inodes(&self) -> Result<BTreeSet<u64>, Self::Error> {
        let x = self.as_ref().read().await;
        x.inodes().await
    }
}
