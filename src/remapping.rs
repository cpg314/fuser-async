//! Utility to remap inodes of a [`Filesystem`]
use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::ops::Range;

use bimap::BiBTreeMap;
use tracing::*;

use crate::{DirEntry, Filesystem};

#[async_trait::async_trait]
pub trait InodeAllocator {
    type Error;
    /// Reserve a continuous number of inodes, returning the start.
    /// The range is disjoint from the previously allocated ones.
    async fn allocate(&mut self, n: usize) -> Result<u64, Self::Error>;
}
#[async_trait::async_trait]
impl<F: Filesystem + Send + Sync> InodeAllocator for F {
    type Error = F::Error;
    async fn allocate(&mut self, _n: usize) -> Result<u64, Self::Error> {
        Ok(self
            .inodes()
            .await?
            .iter()
            .last()
            .copied()
            .unwrap_or(fuser::FUSE_ROOT_ID)
            + 1)
    }
}

#[async_trait::async_trait]
impl InodeAllocator for () {
    type Error = crate::Error;
    async fn allocate(&mut self, _n: usize) -> Result<u64, Self::Error> {
        Err(crate::Error::NoInodes)
    }
}
#[async_trait::async_trait]
// The start of the range denotes the next available inode
impl InodeAllocator for Range<u64> {
    type Error = crate::Error;
    async fn allocate(&mut self, n: usize) -> Result<u64, Self::Error> {
        let Range { start, end } = *self;
        let remaining = end - start;
        if remaining < n as u64 {
            Err(crate::Error::NoInodes)
        } else {
            *self = (start + n as u64)..end;
            Ok(start)
        }
    }
}
#[async_trait::async_trait]
impl<A: InodeAllocator + Send + Sync> InodeAllocator for std::sync::Arc<tokio::sync::Mutex<A>> {
    type Error = A::Error;
    async fn allocate(&mut self, n: usize) -> Result<u64, Self::Error> {
        let mut x = self.lock().await;
        x.allocate(n).await
    }
}

/// Wraps a [`Filesystem`] and remaps its inodes.
pub struct RemappedFilesystem<F, A> {
    pub inner: F,
    pub mapping: BiBTreeMap<u64 /* external */, u64 /* internal */>,
    pub allocator: A,
}
impl<F, A: InodeAllocator> RemappedFilesystem<F, A> {
    pub fn new(inner: F, mapping: BiBTreeMap<u64, u64>, allocator: A) -> Self {
        // TODO: Check if the mapping is valid
        Self {
            inner,
            mapping,
            allocator,
        }
    }
    pub fn ext_to_int<E>(&self, ext: u64) -> Result<u64, Error<E>> {
        self.mapping
            .get_by_left(&ext)
            .ok_or(Error::NotFound(ext, "ext"))
            .copied()
    }
    pub fn int_to_ext<E>(&self, int: u64) -> Result<u64, Error<E>> {
        self.mapping
            .get_by_right(&int)
            .ok_or(Error::NotFound(int, "int"))
            .copied()
    }
    pub async fn next_ext_ino<E>(&mut self) -> Result<u64, Error<E>> {
        self.allocator
            .allocate(1)
            .await
            .map_err(|_| Error::NoExtInodes)
    }
}
#[derive(thiserror::Error, Debug)]
pub enum RefreshError<R, E> {
    #[error("{0}")]
    Remapping(#[from] Error<E>),
    #[error("{0}")]
    Refresh(R),
}
#[async_trait::async_trait]
impl<A: InodeAllocator + Send + Sync, F: Filesystem + crate::Refresh + Send + Sync> crate::Refresh
    for RemappedFilesystem<F, A>
{
    type Error = RefreshError<<F as crate::Refresh>::Error, <F as Filesystem>::Error>;

    async fn cleanup(&self) -> Result<(), Self::Error> {
        self.inner.cleanup().await.map_err(RefreshError::Refresh)
    }
    async fn refresh(&mut self) -> Result<(), Self::Error> {
        self.inner.refresh().await.map_err(RefreshError::Refresh)?;
        let current_int_inodes: BTreeSet<u64> = self.mapping.right_values().copied().collect();
        let all_int_inodes = self
            .inner
            .inodes()
            .await
            .map_err(|e| RefreshError::Remapping(Error::Base(e)))?;

        let new = all_int_inodes.len() - current_int_inodes.len();

        debug!(
            "{} base inodes tracked, {} base inodes in total, {} new",
            current_int_inodes.len(),
            all_int_inodes.len(),
            new
        );
        if new > 0 {
            let next_ext_ino = self
                .allocator
                .allocate(new)
                .await
                .map_err(|_| Error::NoExtInodes)?;
            self.mapping.extend(
                all_int_inodes
                    .difference(&current_int_inodes)
                    .enumerate()
                    .map(|(i, ino)| (next_ext_ino + i as u64, *ino)),
            );
        }

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error<E> {
    #[error("{0}")]
    Base(#[from] E),
    #[error("Inode {0} not found in direction {1}")]
    NotFound(u64, &'static str),
    #[error("No more external inodes available")]
    NoExtInodes,
}
impl<E: Into<crate::Error>> From<Error<E>> for crate::Error {
    fn from(source: Error<E>) -> Self {
        match source {
            Error::NotFound(_, _) => crate::Error::NoFileDir,
            Error::NoExtInodes => crate::Error::NoInodes,
            Error::Base(b) => b.into(),
        }
    }
}

#[async_trait::async_trait]
impl<A: InodeAllocator + Send + Sync, F: Filesystem + Send + Sync> Filesystem
    for RemappedFilesystem<F, A>
{
    type Error = Error<F::Error>;
    async fn inodes(&self) -> Result<BTreeSet<u64>, Self::Error> {
        Ok(self.mapping.left_values().copied().collect())
    }
    async fn destroy(&mut self) -> Result<(), Self::Error> {
        self.inner.destroy().await.map_err(Error::Base)
    }
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<fuser::FileAttr, Self::Error> {
        let parent = self.ext_to_int(parent)?;
        let mut attr = self.inner.lookup(parent, name).await?;
        attr.ino = self.int_to_ext(attr.ino)?;
        Ok(attr)
    }
    async fn open(&self, ino: u64, flags: i32) -> Result<u64, Self::Error> {
        let ino = self.ext_to_int(ino)?;
        Ok(self.inner.open(ino, flags).await?)
    }
    async fn release(&self, ino: u64, fh: u64) -> Result<(), Self::Error> {
        let ino = self.ext_to_int(ino)?;
        Ok(self.inner.release(ino, fh).await?)
    }
    async fn getattr(&self, ino_ext: u64) -> Result<fuser::FileAttr, Self::Error> {
        let ino_int = self.ext_to_int(ino_ext)?;
        let mut attr = self.inner.getattr(ino_int).await?;
        attr.ino = ino_ext;
        Ok(attr)
    }
    async fn setattr(
        &mut self,
        ino_ext: u64,
        size: Option<u64>,
    ) -> Result<fuser::FileAttr, Self::Error> {
        let ino_int = self.ext_to_int(ino_ext)?;
        let mut attr = self.inner.setattr(ino_int, size).await?;
        attr.ino = ino_ext;
        Ok(attr)
    }
    async fn readdir(
        &self,
        ino: u64,
        offset: u64,
    ) -> Result<Box<dyn Iterator<Item = DirEntry> + Send + Sync + '_>, Self::Error> {
        let ino = self.ext_to_int(ino)?;
        #[allow(clippy::needless_collect)]
        let dir: Result<Vec<DirEntry>, _> = self
            .inner
            .readdir(ino, offset)
            .await?
            .map(move |mut e| {
                e.inode = self.int_to_ext(e.inode)?;
                Ok::<_, Self::Error>(e)
            })
            .collect();
        Ok(Box::new(dir?.into_iter()))
    }
    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> Result<bytes::Bytes, Self::Error> {
        let ino = self.ext_to_int(ino)?;
        Ok(self.inner.read(ino, fh, offset, size).await?)
    }
    async fn write(
        &self,
        ino: u64,
        fh: u64,
        data: bytes::Bytes,
        offset: i64,
    ) -> Result<u32, Self::Error> {
        let ino = self.ext_to_int(ino)?;
        Ok(self.inner.write(ino, fh, data, offset).await?)
    }
    async fn create(
        &mut self,
        parent: u64,
        name: std::ffi::OsString,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> Result<(fuser::FileAttr, u64), Self::Error> {
        let parent = self.ext_to_int(parent)?;
        let (mut attr, fh) = self.inner.create(parent, name, mode, umask, flags).await?;
        let ext_ino = self.next_ext_ino().await?;
        self.mapping.insert(ext_ino, attr.ino);
        attr.ino = ext_ino;
        Ok((attr, fh))
    }
    async fn mkdir(&mut self, parent: u64, name: OsString) -> Result<fuser::FileAttr, Self::Error> {
        let parent = self.ext_to_int(parent)?;
        let mut attr = self.inner.mkdir(parent, name).await?;
        let ext_ino = self.next_ext_ino().await?;
        self.mapping.insert(ext_ino, attr.ino);
        attr.ino = ext_ino;
        Ok(attr)
    }
}
