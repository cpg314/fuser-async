//! Sample [`Filesystem`] implementation that passes the calls to the local filesystem via [`tokio::fs`].
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::os::unix::fs::{DirEntryExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};
use tracing::*;

use fuser_async::{utils, DirEntry, Error, Filesystem, Refresh};

pub fn glob(path: impl AsRef<Path>, dirs_only: bool) -> impl Iterator<Item = PathBuf> {
    let path = path.as_ref();
    glob::glob(path.to_str().unwrap())
        .unwrap()
        .filter(move |d| !dirs_only || d.as_ref().unwrap().is_dir())
        .map(|d| d.unwrap())
}

pub struct LocalInode {
    /// Path relative to the root
    path: PathBuf,
    handles:
        RwLock<BTreeMap<u64, Mutex<tokio::io::BufWriter<tokio::io::BufReader<tokio::fs::File>>>>>,
    #[cfg(feature = "memmap")]
    mmap: tokio::sync::OnceCell<memmap2::Mmap>,
}
impl LocalInode {
    async fn open(&self, root: &Path, create_new: bool) -> Result<u64, Error> {
        let mut handles = self.handles.write().await;
        let fh = handles.keys().last().map(|x| x + 1).unwrap_or_default();
        let path = root.join(&self.path);
        let f = tokio::fs::OpenOptions::new()
            .create_new(create_new)
            .read(true)
            .write(create_new)
            .open(&path)
            .await
            .map_err(|_| Error::NoFileDir)?;
        let f = tokio::io::BufReader::new(f);
        let f = tokio::io::BufWriter::new(f);
        handles.insert(fh, Mutex::new(f));
        #[cfg(feature = "memmap")]
        self.mmap
            .get_or_try_init(|| async {
                let file = std::fs::File::open(&path).map_err(|_| Error::NoFileDir)?;
                unsafe { memmap2::Mmap::map(&file).map_err(|_| Error::NoFileDir) }
            })
            .await?;
        Ok(fh)
    }
    async fn set_len(&self, root: &Path, size: u64) -> Result<(), Error> {
        let f = tokio::fs::OpenOptions::new()
            .write(true)
            .open(root.join(&self.path))
            .await
            .map_err(|_| Error::NoFileDir)?;
        f.set_len(size).await.unwrap();
        Ok(())
    }
    async fn release(&self, fh: u64) -> Result<(), Error> {
        let mut handles = self.handles.write().await;
        let fh = handles.remove(&fh).ok_or(Error::BadFileDescriptor)?;
        let mut fh = fh.lock().await;
        fh.flush()
            .await
            .map_err(|_| Error::io("Failed to flush file"))?;
        Ok(())
    }
}
impl From<PathBuf> for LocalInode {
    fn from(path: PathBuf) -> Self {
        Self {
            path,
            handles: Default::default(),
            #[cfg(feature = "memmap")]
            mmap: Default::default(),
        }
    }
}

fn get_file_attr(metadata: &std::fs::Metadata, new_ino: u64) -> Result<fuser::FileAttr, Error> {
    Ok(fuser::FileAttr {
        ino: new_ino,
        size: metadata.size(),
        blocks: metadata.blocks(),
        // TODO: Fill these
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: utils::file_type(metadata)?,
        perm: metadata.mode() as u16,
        nlink: metadata.nlink().try_into().unwrap_or_default(),
        uid: metadata.uid(),
        gid: metadata.gid(),
        rdev: 0,
        flags: 0,
        blksize: metadata.blksize().try_into().unwrap_or_default(),
    })
}
/// Filesystem backed by a directory on the local filesystem.
pub struct LocalFilesystem {
    pub root: PathBuf,
    /// Complete mapping of external inodes to data about them.
    pub inodes: BTreeMap<u64 /* external inode */, LocalInode>,
    // TODO: Use inode remapping
    pub inodes_rev: HashMap<u64 /* local inode */, u64 /* external inode */>,
}
#[async_trait::async_trait]
impl Refresh for LocalFilesystem {
    type Error = std::io::Error;
    /// Maps all the local inodes to external inodes exposed to clients.
    async fn refresh(&mut self) -> std::io::Result<()> {
        let mut next_ino = self.next_ino();
        for d in glob(self.root.join("**/*"), false) {
            let metadata = std::fs::metadata(&d)?;
            if let std::collections::hash_map::Entry::Vacant(e) =
                self.inodes_rev.entry(metadata.ino())
            {
                let d = d.strip_prefix(&self.root).unwrap().to_owned();
                self.inodes.insert(next_ino, d.into());
                e.insert(next_ino);
                next_ino += 1;
            }
        }
        Ok(())
    }
    async fn cleanup(&self) -> std::io::Result<()> {
        Ok(())
    }
}
impl LocalFilesystem {
    pub async fn new(root: &Path) -> std::io::Result<Self> {
        let mut out = Self {
            root: root.into(),
            inodes: Default::default(),
            inodes_rev: Default::default(),
        };
        out.inodes
            .insert(fuser::FUSE_ROOT_ID, PathBuf::default().into());
        out.refresh().await?;
        Ok(out)
    }
    fn get_inode(&self, ino: u64) -> Result<&LocalInode, Error> {
        self.inodes.get(&ino).ok_or(Error::NoFileDir)
    }
    fn next_ino(&self) -> u64 {
        self.inodes
            .keys()
            .last()
            .copied()
            .unwrap_or(fuser::FUSE_ROOT_ID)
            + 1
    }
    fn insert_inode(&mut self, inode: LocalInode) -> Result<fuser::FileAttr, Error> {
        let metadata =
            std::fs::metadata(self.root.join(&inode.path)).map_err(|_| Error::NoFileDir)?;
        let ino_local = metadata.ino();
        let attr = get_file_attr(&metadata, self.next_ino())?;
        self.inodes_rev.insert(ino_local, attr.ino);
        self.inodes.insert(attr.ino, inode);
        debug!("Inserted inode (local {}, fuse {})", ino_local, attr.ino);
        Ok(attr)
    }
}

#[async_trait::async_trait]
impl Filesystem for LocalFilesystem {
    type Error = Error;
    async fn inodes(&self) -> Result<BTreeSet<u64>, Error> {
        Ok(self.inodes.keys().copied().collect())
    }
    async fn lookup(&self, parent: u64, name: &std::ffi::OsStr) -> Result<fuser::FileAttr, Error> {
        let p = self.get_inode(parent).map_err(|_| Error::NoFileDir)?;
        let f = std::fs::read_dir(self.root.join(&p.path))
            .map_err(|_| Error::NoFileDir)?
            .filter_map(Result::ok)
            .find(|e| e.file_name() == name)
            .ok_or(Error::NoFileDir)?;
        let ino = *self.inodes_rev.get(&f.ino()).ok_or_else(|| {
            Error::Other(format!("Failed to find inode mapping for {:?}", f.ino()))
        })?;
        let attr = get_file_attr(&f.metadata().map_err(|_| Error::NoFileDir)?, ino)?;
        Ok(attr)
    }
    async fn open(&self, ino: u64, _flags: i32) -> Result<u64, Error> {
        let p = self.get_inode(ino)?;
        p.open(&self.root, false).await
    }
    async fn release(&self, ino: u64, fh: u64) -> Result<(), Error> {
        let p = self.get_inode(ino)?;
        p.release(fh).await
    }
    async fn getattr(&self, ino: u64) -> Result<fuser::FileAttr, Error> {
        let p = self.get_inode(ino)?;
        let attr = get_file_attr(
            &std::fs::metadata(self.root.join(&p.path)).map_err(|_| Error::NoFileDir)?,
            ino,
        )?;
        Ok(attr)
    }

    async fn setattr(
        &mut self,
        ino: u64,
        size: Option<u64>,
    ) -> Result<fuser::FileAttr, Self::Error> {
        let p = self.get_inode(ino)?;
        if let Some(size) = size {
            p.set_len(&self.root, size).await?;
        }
        self.getattr(ino).await
    }
    async fn readdir(
        &self,
        ino: u64,
        offset: u64,
    ) -> Result<Box<dyn Iterator<Item = DirEntry> + Send + Sync + '_>, Error> {
        let p = self.get_inode(ino)?;
        let path = self.root.join(&p.path);
        if !path.is_dir() {
            return Err(Error::NotDirectory);
        }
        Ok(Box::new(
            std::fs::read_dir(&path)
                .map_err(|_| Error::io("Failed to read directory"))?
                .filter_map(Result::ok)
                .filter_map(|e| DirEntry::try_from(&e).ok())
                .filter_map(|mut d| {
                    let ino = d.inode;
                    self.inodes_rev
                        .get(&d.inode)
                        .map(|ino| {
                            d.inode = *ino;
                            d
                        })
                        .or_else(|| {
                            warn!("Failed to map local inode {}", ino);
                            None
                        })
                })
                .skip(offset as usize),
        ))
    }
    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> Result<bytes::Bytes, Self::Error> {
        if offset < 0 {
            error!("Negative offsets not supported");
            return Err(Error::Unimplemented);
        }
        let p = self.get_inode(ino)?;

        cfg_if::cfg_if! {
            if  #[cfg(feature = "memmap")] {
                let _ = fh;
                let f = p.mmap.get().ok_or(Error::BadFileDescriptor)?;
                let mut f = std::io::Cursor::new(f);
            } else {
                let handles = p.handles.read().await;
                let f = handles.get(&fh).ok_or(Error::BadFileDescriptor)?;
                let mut f = f.lock().await;
            }
        }
        f.seek(std::io::SeekFrom::Start(offset as u64))
            .await
            .map_err(|_| Error::io("Failed to seek"))?;
        #[cfg(feature = "memmap")]
        let mut f = (&mut f).take(size as u64);
        #[cfg(not(feature = "memmap"))]
        let mut f = (&mut *f).take(size as u64);
        let mut data = Vec::with_capacity(size as usize);
        tokio::io::copy(&mut f, &mut data)
            .await
            .map_err(|_| Error::io("Failed to read data"))?;
        Ok(data.into())
    }
    async fn write(
        &self,
        ino: u64,
        fh: u64,
        data: bytes::Bytes,
        offset: i64,
    ) -> Result<u32, Self::Error> {
        if offset < 0 {
            error!("Negative offsets not supported");
            return Err(Error::Unimplemented);
        }
        let p = self.get_inode(ino)?;
        let handles = p.handles.read().await;
        let f = handles.get(&fh).ok_or(Error::BadFileDescriptor)?;
        let mut f = f.lock().await;
        f.seek(std::io::SeekFrom::Start(offset as u64))
            .await
            .map_err(|_| Error::io("Failed seeking"))?;
        Ok(f.write(&data)
            .await
            .map_err(|_| Error::io("Failed to write data to file"))? as u32)
    }
    async fn create(
        &mut self,
        parent: u64,
        name: std::ffi::OsString,
        _mode: u32,
        _umask: u32,
        _flags: i32,
    ) -> Result<(fuser::FileAttr, u64), Self::Error> {
        let parent = self.get_inode(parent)?;
        let inode: LocalInode = parent.path.join(name).into();

        let fh = inode.open(&self.root, true).await?;
        let attr = self.insert_inode(inode)?;

        Ok((attr, fh))
    }
    async fn mkdir(
        &mut self,
        parent: u64,
        name: std::ffi::OsString,
    ) -> Result<fuser::FileAttr, Self::Error> {
        let parent = self.get_inode(parent)?;
        let path = parent.path.join(name);

        std::fs::create_dir(self.root.join(&path))
            .map_err(|_| Error::io("Failed to create directory"))?;

        self.insert_inode(path.into())
    }
}
