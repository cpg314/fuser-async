//! Remote filesystem using the [S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html),
//! backed by [`rusty_s3`].
mod filesystem;
mod handle;
mod inode;
mod refresh;
pub mod write;
pub use inode::CACHE_DIRECT_SIZE_MB;

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use tokio::sync::RwLock;
use tracing::*;

use fuser_async::cache;
use fuser_async::Refresh;

trait_set::trait_set! {
    /// Trait alias for [`fuser_async::cache::DataBlockCache`] with [`anyhow`] errors.
    pub trait DataBlockCache = fuser_async::cache::DataBlockCache<anyhow::Error>;
}

// File handles become invalid after that many seconds.
const HANDLE_EXPIRATION_S: u64 = 72 * 3600;
const CACHE_CLEANUP_S: u64 = 5;

/// S3 filesystem error
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("HTTP error: {0}")]
    HTTP(#[from] reqwest::Error),
    #[error("{0}")]
    Base(#[from] fuser_async::Error),
    #[error("Read {read} bytes, expected {expected}")]
    ShortRead { read: usize, expected: usize },
    #[error("Cache not enabled")]
    NoCache,
    #[error("S3 error: {0}")]
    S3Error(String),
    #[error("Failed to find available part for upload")]
    FindPart,
    #[error("Writing twice on the same range")]
    WriteTwice,
    #[error("File already closed")]
    AlreadyClosed,
    #[error("Cache error: {0}")]
    Cache(#[from] cache::CacheError<anyhow::Error>),
    #[error("{0}")]
    Miscellaneous(String),
}
impl From<Error> for fuser_async::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::HTTP(e) => Self::IO(format!("HTTP error: {}", e)),
            Error::Base(b) => b,
            _ => fuser_async::Error::IO(source.to_string()),
        }
    }
}

async fn retry<F, E, T>(f: impl Fn() -> F, action: &str) -> Result<T, E>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut retries = 10;
    let mut duration_s = 1.0;
    loop {
        match f().await {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                warn!(
                    "Received error while {} ({}), {} retries left, sleep {} s",
                    action, e, retries, duration_s
                );
                if retries > 0 {
                    retries -= 1;
                    // Sleep 1s, 1.5s, ..., 57 seconds
                    tokio::time::sleep(std::time::Duration::from_secs_f32(duration_s)).await;
                    duration_s *= 1.5;
                } else {
                    return Err(e);
                }
            }
        }
    }
}

/// Configuration for fetching blocks via HTTP
#[derive(Parser, Clone)]
pub struct FetchConfig {
    /// Block size for fetching and caching, in MB.
    ///
    /// Larger blocks usually give faster download speed, at the cost of latency and worse
    /// performance on small files.
    #[clap(long, default_value_t = 10)]
    pub fetch_size_mb: u16,
    /// Cache size per open file, in MB.
    #[clap(long, default_value_t = 300)]
    pub file_cache_mb: u64,
    /// Number of folders to retrieve in parallel.
    #[clap(long, default_value_t = 4)]
    pub listing_parallelism: usize,
    /// Maximal folder depth at which to parallelize listing.
    /// A value of 0 will parallelize over top-level folders.
    #[clap(long)]
    pub listing_parallelism_depth: Option<usize>,
    #[clap(long, default_value_t = 60)]
    pub timeout_s: u64,
}

impl FetchConfig {
    fn fetch_size_bytes(&self) -> u64 {
        self.fetch_size_mb as u64 * 1e6 as u64
    }
}
/// S3 configuration
#[derive(Parser, serde::Deserialize, serde::Serialize, Clone)]
pub struct Config {
    #[clap(long)]
    /// Bucket URL
    pub url: String,
    #[clap(long, default_value_t)]
    /// Bucket name
    pub bucket: String,
    #[clap(skip)]
    /// Root relative to the bucket
    pub root: PathBuf,
    #[clap(long, default_value_t)]
    pub username: String,
    #[clap(long, default_value_t)]
    pub password: String,
    #[clap(long, default_value_t)]
    pub region: String,
    #[clap(long, value_enum, default_value_t=UrlStyle::Path)]
    pub url_style: UrlStyle,
}
impl Config {
    pub fn from_yaml(path: &Path) -> anyhow::Result<Self> {
        Ok(serde_yaml::from_str(&std::fs::read_to_string(path)?)?)
    }
    pub fn to_yaml(&self) -> anyhow::Result<String> {
        Ok(serde_yaml::to_string(&self)?)
    }
}
pub struct S3Data {
    client: reqwest::Client,
    credentials: rusty_s3::Credentials,
    bucket: rusty_s3::Bucket,
    root: String,
    fetch_config: FetchConfig,
}
/// Remote filesystem using the [S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html),
/// backed by [`rusty_s3`].
pub struct S3Filesystem<C: DataBlockCache> {
    s3: Arc<S3Data>,
    directory_table: HashMap<PathBuf, Vec<u64>>,
    inodes: RwLock<Inodes<C>>,
}
pub struct Inodes<C: DataBlockCache>(BTreeMap<u64, inode::Inode<C>>);
impl<C: DataBlockCache> Default for Inodes<C> {
    fn default() -> Self {
        Self(Default::default())
    }
}
impl<C: DataBlockCache> Inodes<C> {
    fn next_inode(&self) -> u64 {
        self.0.keys().last().copied().unwrap_or(fuser::FUSE_ROOT_ID) + 1
    }
    fn get_directory_path(&self, ino: u64) -> Result<&Path, Error> {
        if let Some(inode::Inode::Directory(path)) = self.0.get(&ino) {
            Ok(path)
        } else {
            Err(fuser_async::Error::NoFileDir.into())
        }
    }
    fn get_inode(&self, ino: u64) -> Result<&inode::Inode<C>, Error> {
        self.0
            .get(&ino)
            .ok_or_else(|| fuser_async::Error::NotDirectory.into())
    }
    fn get_file_inode(&self, ino: u64) -> Result<&inode::FileInode<C>, Error> {
        match self.get_inode(ino)? {
            inode::Inode::File(f) => Ok(f),
            inode::Inode::FileWrite(_) => {
                warn!("Cannot open write-mode files in read mode");
                Err(fuser_async::Error::NoFileDir.into())
            }
            _ => Err(fuser_async::Error::NoFileDir.into()),
        }
    }
}

#[derive(clap::ArgEnum, Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum UrlStyle {
    Path,
    VirtualHost,
}
impl From<UrlStyle> for rusty_s3::UrlStyle {
    fn from(source: UrlStyle) -> Self {
        match source {
            UrlStyle::Path => rusty_s3::UrlStyle::Path,
            UrlStyle::VirtualHost => rusty_s3::UrlStyle::VirtualHost,
        }
    }
}
impl<C: DataBlockCache> S3Filesystem<C> {
    pub async fn new(config: Config, fetch_config: FetchConfig) -> anyhow::Result<Self> {
        let start = std::time::Instant::now();
        info!("Initializing S3 backend");
        let client = reqwest::Client::builder()
            .tcp_keepalive(std::time::Duration::from_secs(60))
            .timeout(std::time::Duration::from_secs(fetch_config.timeout_s))
            .build()?;
        let mut out = Self {
            s3: Arc::new(S3Data {
                client,
                credentials: rusty_s3::Credentials::new(config.username, config.password),
                bucket: rusty_s3::Bucket::new(
                    reqwest::Url::parse(&config.url)?,
                    config.url_style.into(),
                    config.bucket,
                    config.region,
                )?,
                // Ensure a final slash
                root: config.root.join("").to_str().unwrap().into(),
                fetch_config,
            }),
            directory_table: Default::default(),
            inodes: Default::default(),
        };
        out.refresh().await?;
        info!("Done initializing S3 backend in {:?}", start.elapsed());
        Ok(out)
    }
    fn get_directory_from_path(&self, path: &Path) -> Result<&Vec<u64>, Error> {
        self.directory_table
            .get(path)
            .ok_or_else(|| fuser_async::Error::NotDirectory.into())
    }
}
