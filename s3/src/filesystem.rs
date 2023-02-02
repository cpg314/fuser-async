use std::collections::BTreeSet;

use tracing::*;

use fuser_async::{utils, DirEntry, Filesystem};

use super::{inode::Inode, DataBlockCache, Error, S3Filesystem};

#[async_trait::async_trait]
impl<C: DataBlockCache> Filesystem for S3Filesystem<C> {
    type Error = Error;
    async fn inodes(&self) -> Result<BTreeSet<u64>, Self::Error> {
        let inodes = self.inodes.read().await;
        Ok(inodes.0.keys().copied().collect())
    }
    async fn lookup(
        &self,
        parent: u64,
        name: &std::ffi::OsStr,
    ) -> Result<fuser::FileAttr, Self::Error> {
        let inodes = self.inodes.read().await;
        let (ino, d) = self
            .get_directory_from_path(inodes.get_directory_path(parent)?)?
            .iter()
            .filter_map(|ino| inodes.0.get(ino).map(|d| (ino, d)))
            .find(|(_, d)| d.name() == name)
            .ok_or(fuser_async::Error::NoFileDir)?;
        d.attrs(*ino, self).await
    }
    async fn getattr(&self, ino_fuse: u64) -> Result<fuser::FileAttr, Self::Error> {
        let inodes = self.inodes.read().await;
        let inode = inodes.get_inode(ino_fuse)?;
        inode.attrs(ino_fuse, self).await
    }
    async fn setattr(
        &mut self,
        _ino: u64,
        _size: Option<u64>,
    ) -> Result<fuser::FileAttr, Self::Error> {
        Err(fuser_async::Error::Unimplemented.into())
    }
    async fn readdir(
        &self,
        ino_fuse: u64,
        offset: u64,
    ) -> Result<Box<dyn Iterator<Item = DirEntry> + Send + Sync + '_>, Self::Error> {
        let inodes = self.inodes.read().await;
        let it: Vec<DirEntry> = self
            .get_directory_from_path(inodes.get_directory_path(ino_fuse)?)?
            .iter()
            .filter_map(|ino| inodes.0.get(ino).map(move |d| (ino, d)))
            .skip(offset as usize)
            .map(|(ino, d)| d.dir_entry(*ino))
            .collect();
        Ok(Box::new(it.into_iter()))
    }
    async fn open(&self, ino_fuse: u64, flags: i32) -> Result<u64, Self::Error> {
        let inodes = self.inodes.read().await;
        debug!(inode = ino_fuse, "Opening file handle");
        let inode = inodes.get_file_inode(ino_fuse)?;
        inode.new_handle(&self.s3, flags).await
    }
    async fn release(&self, ino: u64, fh: u64) -> Result<(), Self::Error> {
        debug!(handle = fh, inode = ino, "Releasing file handle");
        let inodes = self.inodes.read().await;
        match inodes.0.get(&ino) {
            Some(Inode::File(inode)) => return inode.release_handle(fh).await,
            Some(Inode::FileWrite(inode)) => {
                let size = inode.finish().await?;
                let path = inode.path.clone();
                drop(inodes);
                let mut inodes = self.inodes.write().await;
                inodes
                    .0
                    .insert(ino, Inode::File(super::inode::FileInode::new(&path, size)));
            }
            Some(Inode::Directory(_)) => return Err(fuser_async::Error::InvalidArgument.into()),
            None => return Err(fuser_async::Error::NoFileDir.into()),
        }
        Ok(())
    }
    async fn read(
        &self,
        ino_fuse: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> Result<bytes::Bytes, Self::Error> {
        debug!(
            handle = fh,
            inode = ino_fuse,
            "Reading {} bytes from {}",
            size,
            offset
        );

        let inodes = self.inodes.read().await;
        let inode = inodes.get_file_inode(ino_fuse)?;
        if offset > inode.size as i64 {
            return Ok(Default::default());
        }
        let size = size.min(u32::try_from(inode.size - offset as u64).unwrap_or(u32::MAX));
        if size == 0 {
            return Ok(Default::default());
        }
        let handles = inode.handles.read().await;
        let handle = handles
            .get(&fh)
            .ok_or(fuser_async::Error::BadFileDescriptor)?;
        handle.get(offset as u64, size).await
    }
    async fn write(
        &self,
        ino: u64,
        _fh: u64,
        data: bytes::Bytes,
        offset: i64,
    ) -> Result<u32, Self::Error> {
        if offset < 0 {
            error!("Negative offsets not supported");
            return Err(fuser_async::Error::Unimplemented.into());
        }
        let inodes = self.inodes.read().await;
        match inodes.0.get(&ino) {
            Some(super::inode::Inode::FileWrite(fw)) => {
                let len = data.len() as u32;
                fw.add_data(offset as u64, data).await?;
                Ok(len)
            }
            _ => Err(fuser_async::Error::NoFileDir.into()),
        }
    }
    async fn create(
        &mut self,
        parent: u64,
        name: std::ffi::OsString,
        _mode: u32,
        _umask: u32,
        _flags: i32,
    ) -> Result<(fuser::FileAttr, u64), Self::Error> {
        let mut inodes = self.inodes.write().await;
        let parent_path = inodes.get_directory_path(parent)?.to_owned();
        let write =
            super::write::FileWriteInode::new::<C>(&parent_path.join(name), self.s3.clone())
                .await?;
        let ino = inodes.next_inode();
        inodes.0.insert(ino, super::inode::Inode::FileWrite(write));
        self.directory_table
            .entry(parent_path)
            .or_default()
            .push(ino);

        Ok((utils::file_attr(ino, 0, std::time::UNIX_EPOCH), 0))
    }
    async fn mkdir(
        &mut self,
        parent: u64,
        name: std::ffi::OsString,
    ) -> Result<fuser::FileAttr, Self::Error> {
        let mut inodes = self.inodes.write().await;
        let parent_path = inodes.get_directory_path(parent)?.to_owned();
        let path = parent_path.join(name);
        let ino = inodes.next_inode();
        inodes
            .0
            .insert(ino, super::inode::Inode::Directory(path.clone()));
        self.directory_table.insert(path, vec![]);
        self.directory_table
            .entry(parent_path)
            .or_default()
            .push(ino);
        Ok(utils::dir_attr(ino, 0, std::time::UNIX_EPOCH))
    }
}
