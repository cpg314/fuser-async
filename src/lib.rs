#![doc = include_str!("../README.md")]
#![doc(html_root_url = "https://docs.rs/fuser-async/0.1.1")]
// Implementations
// pub mod implementations;
// pub use implementations::local;
// #[cfg(feature = "s3")]
// pub use implementations::s3;
// Internals
mod async_readseek;
mod error;
mod filesystem;
mod fuse;
// Interface
pub use async_readseek::FileHandle;
pub use error::Error;
pub use filesystem::Filesystem;
pub use fuse::FilesystemFUSE;
// Utils
pub mod cache;
pub mod remapping;
pub mod rwlockoption;
pub mod utils;

use std::path::Path;

trait_set::trait_set! {
    /// Trait alias for a  [`Filesystem`] that is [`Send`], [`Sync`], [`std::marker::Unpin`] and `'static`.
    pub trait FilesystemSSUS = Filesystem + Send + Sync  + std::marker::Unpin + 'static;
}

/// Directory entry (returned by [`Filesystem::readdir`]).
#[derive(Debug)]
pub struct DirEntry {
    pub inode: u64,
    pub file_type: fuser::FileType,
    pub name: String, // TODO: Use OsStr and/or Cow
}
impl DirEntry {
    pub fn path(&self) -> &Path {
        Path::new(&self.name)
    }
}

/// Trait for filesystems that can be refreshed.
#[async_trait::async_trait]
pub trait Refresh {
    type Error;
    /// Refresh filesystem
    async fn refresh(&mut self) -> Result<(), Self::Error>;
    /// Cleanup caches
    async fn cleanup(&self) -> Result<(), Self::Error>;
}
