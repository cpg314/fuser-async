//! Utilities
pub use outof::OutOf;

use std::os::unix::fs::MetadataExt;

use crate::error::Error;

pub const BLOCK_SIZE: u32 = 512;
impl TryFrom<&std::fs::DirEntry> for crate::DirEntry {
    type Error = Error;
    fn try_from(e: &std::fs::DirEntry) -> Result<Self, Error> {
        let metadata = e.metadata().map_err(|_| Error::NoFileDir)?;
        let name = e.file_name();
        let name = name.to_str().ok_or(Error::Encoding)?;
        Ok(Self {
            inode: metadata.ino(),
            name: name.into(),
            file_type: file_type(&metadata)?,
        })
    }
}

/// Try getting the [`fuser::FileType`] from a [`std::fs::Metadata`].
///
/// For now, this only supports directories, files, and symlinks.
pub fn file_type(metadata: &std::fs::Metadata) -> Result<fuser::FileType, Error> {
    let ft = metadata.file_type();
    if ft.is_dir() {
        Ok(fuser::FileType::Directory)
    } else if ft.is_file() {
        Ok(fuser::FileType::RegularFile)
    } else if ft.is_symlink() {
        Ok(fuser::FileType::Symlink)
    } else {
        Err(Error::Unimplemented)
    }
}

/// Crate basic [`fuser::FileAttr`] from inode, size and time.
pub fn file_attr(ino: u64, size: u64, time: std::time::SystemTime) -> fuser::FileAttr {
    fuser::FileAttr {
        ino,
        size,
        blocks: (size as f64 / BLOCK_SIZE as f64).ceil() as u64,
        atime: time,
        mtime: time,
        ctime: time,
        crtime: time,
        kind: fuser::FileType::RegularFile,
        perm: 0o644,
        nlink: 1,
        uid: 501,
        gid: 20,
        rdev: 0,
        flags: 0,
        blksize: BLOCK_SIZE,
    }
}
pub fn dir_attr(ino: u64, children: u32, time: std::time::SystemTime) -> fuser::FileAttr {
    fuser::FileAttr {
        ino,
        size: 0,
        blocks: 0,
        atime: time,
        mtime: time,
        ctime: time,
        crtime: time,
        kind: fuser::FileType::Directory,
        perm: 0o644,
        nlink: children + 2,
        uid: 501,
        gid: 20,
        rdev: 0,
        flags: 0,
        blksize: BLOCK_SIZE,
    }
}
pub fn unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

mod outof {
    use num_traits::cast::AsPrimitive;
    use serde::{Deserialize, Serialize};

    static PERCENT_DEFAULT_DISPLAY_PRECISION: usize = 0;
    /// Compute and display fractions.
    ///
    /// ```
    /// use fuser_async::utils::OutOf;
    /// let r = OutOf::new(1, 3);
    /// assert_eq!(format!("{:.1}", r), "33.3%");
    /// assert_eq!(format!("{:.0}", r), "33%");
    /// ```
    #[derive(Copy, Serialize, Deserialize, Clone)]
    pub struct OutOf {
        pub part: f64,
        pub total: f64,
    }
    impl Default for OutOf {
        fn default() -> Self {
            Self {
                part: 0.0,
                total: 0.0,
            }
        }
    }
    impl AsPrimitive<f64> for OutOf {
        fn as_(self) -> f64 {
            self.part / self.total
        }
    }
    impl OutOf {
        pub fn new<A: AsPrimitive<f64>, B: AsPrimitive<f64>>(part: A, total: B) -> Self {
            Self {
                part: part.as_(),
                total: total.as_(),
            }
        }
        pub fn perc(&self) -> f64 {
            self.part / self.total * 100.0
        }
        pub fn display_full(&self) -> String {
            if self.total == 0.0 {
                format!("{}/{}", self.part, self.total)
            } else {
                format!("{}/{} ({})", self.part, self.total, self)
            }
        }
    }
    impl std::fmt::Display for OutOf {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            let prec = f.precision().unwrap_or(PERCENT_DEFAULT_DISPLAY_PRECISION);
            let ratio = self.as_();
            if ratio.is_finite() {
                write!(f, "{:.*}%", prec, 100.0 * ratio)
            } else {
                Ok(())
            }
        }
    }
}
