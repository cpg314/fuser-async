//! Caching utilities

use std::hash::Hasher;
use std::sync::atomic::AtomicU64;

use rustc_hash::FxHasher;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use tracing::*;

#[derive(Debug, thiserror::Error)]
pub enum CacheError<E> {
    #[error("Cache with too few blocks ({0})")]
    TooFewBlocks(usize),
    #[error("Position out of bounds")]
    OutOfBounds,
    #[error("Unexpected empty block")]
    UnexpectedEmpty,
    #[error("Block insertion error: {0}")]
    Insert(E),
}

/// Cached block
pub struct Block {
    pub data: bytes::BytesMut,
    pub pos: Option<u64>,
}
/// Block cache trait
#[async_trait::async_trait]
pub trait DataBlockCache<InsertError>: Send + Sync + std::fmt::Display + Sized + 'static {
    /// Create a new block cache with given maximal capacity, block size, and cumulative size of all
    /// blocks.
    fn new(
        capacity_mb: u64,
        block_size: u64,
        total_size: u64,
    ) -> Result<Self, CacheError<InsertError>>;
    /// Get the block at a position.
    async fn get(&self, pos: u64) -> Option<RwLockReadGuard<Block>>;
    /// Insert a block at a position, locking until the output is ready.
    async fn insert_lock<A: AsRef<[u8]> + Send>(
        &self,
        pos: u64,
        f: impl std::future::Future<Output = Result<A, InsertError>> + Send,
    ) -> Result<RwLockReadGuard<Block>, CacheError<InsertError>>;
    /// Caching statistics.
    fn stats(&self) -> &CacheStats;
}

/// Cache statistics
#[derive(Default)]
pub struct CacheStats {
    pub capacity_blocks: usize,
    pub capacity_mb: u64,
    pub block_size: u64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}
impl std::fmt::Display for CacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let hits = self.hits.load(std::sync::atomic::Ordering::SeqCst);
        let misses = self.misses.load(std::sync::atomic::Ordering::SeqCst);
        let misses_excluding = misses
            .checked_sub(self.capacity_blocks as u64)
            .unwrap_or_default();
        write!(
            f,
            "Cache of capacity {} blocks of {:.2} MB (total {} MB). {} hits, {} hits excluding capacity",
            self.capacity_blocks,
            self.block_size as f64 / 1e6,
            self.capacity_mb,
            crate::utils::OutOf::new(hits, hits + misses).display_full(),
            crate::utils::OutOf::new(hits, hits + misses_excluding).display_full(),
        )
    }
}

impl CacheStats {
    pub fn add_hit(&self) {
        self.hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    pub fn add_miss(&self) {
        self.misses
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    pub fn remove_miss(&self) {
        self.misses
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Least Recently Used (LRU) cache, with one slot for every possible block, each protected by a
/// [`RwLock`].
pub struct LRUCache {
    lru: Mutex<lru::LruCache<u64, ()>>,
    data: Vec<RwLock<Option<Block>>>,
    stats: CacheStats,
}
impl std::fmt::Display for LRUCache {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.stats)
    }
}

#[async_trait::async_trait]
impl<E> DataBlockCache<E> for LRUCache {
    fn new(capacity_mb: u64, block_size: u64, total_size: u64) -> Result<Self, CacheError<E>> {
        let capacity_blocks = (capacity_mb as f64 / block_size as f64 * 1e6).ceil() as usize;
        let total_blocks = (total_size as f64 / block_size as f64).ceil() as usize;
        debug!(
            capacity_blocks,
            capacity_mb, block_size, "Allocating LRU cache"
        );
        if capacity_blocks < 4 {
            return Err(CacheError::TooFewBlocks(capacity_blocks));
        }

        Ok(Self {
            stats: CacheStats {
                capacity_blocks,
                capacity_mb,
                block_size,
                ..Default::default()
            },
            data: (0..total_blocks).map(|_| None).map(RwLock::new).collect(),
            lru: Mutex::new(lru::LruCache::<u64, ()>::unbounded()),
        })
    }
    async fn get(&self, pos: u64) -> Option<RwLockReadGuard<Block>> {
        assert!((pos as usize) < self.data.len());
        {
            let mut lru = self.lru.lock().await;
            lru.get(&pos);
        }
        let data = self.data[pos as usize].read().await;
        if data.is_some() {
            self.stats.add_hit();
            return Some(RwLockReadGuard::map(data, |x| x.as_ref().unwrap()));
        }
        self.stats.add_miss();
        None
    }
    async fn insert_lock<A: AsRef<[u8]> + Send>(
        &self,
        pos: u64,
        f: impl std::future::Future<Output = Result<A, E>> + Send,
    ) -> Result<RwLockReadGuard<Block>, CacheError<E>> {
        if (pos as usize) >= self.data.len() {
            return Err(CacheError::OutOfBounds);
        }
        let mut block = self.data[pos as usize].write().await;
        if block.is_some() {
            self.stats.remove_miss();
            return Ok(RwLockReadGuard::map(block.downgrade(), |f| {
                f.as_ref().unwrap()
            }));
        }

        let buf = f.await.map_err(|e| CacheError::Insert(e))?;
        let buf = buf.as_ref();

        let mut lru = self.lru.lock().await;
        if lru.len() < self.stats.capacity_blocks {
            lru.put(pos, ());
            let mut data = bytes::BytesMut::with_capacity(self.stats.block_size as usize);
            data.resize(buf.len(), 0);
            data.copy_from_slice(buf);
            *block = Some(Block { data, pos: None });
        } else {
            let (old, _) = lru.pop_lru().unwrap();
            let mut block_old = self.data[old as usize].write().await;
            lru.put(pos, ());
            let mut block_old = block_old.take().ok_or(CacheError::UnexpectedEmpty)?;
            block_old.data.clear();
            block_old.data.resize(buf.len(), 0);
            block_old.data.copy_from_slice(buf);

            *block = Some(block_old);
        }
        return Ok(RwLockReadGuard::map(block.downgrade(), |f| {
            f.as_ref().unwrap()
        }));
    }
    fn stats(&self) -> &CacheStats {
        &self.stats
    }
}

/// Simple cache implemented with a [`Vec<RwLock<Block>>`], with the key determined by the position
/// modulo the size.
///
/// Prone to collisions.
pub struct IndexCache {
    data: Vec<RwLock<Block>>,
    collisions: AtomicU64,
    pub stats: CacheStats,
    use_hash: bool,
}

impl IndexCache {
    fn key(&self, pos: u64) -> usize {
        if self.use_hash {
            pos as usize % self.stats.capacity_blocks
        } else {
            let mut hasher = FxHasher::default();
            hasher.write_u64(pos);
            hasher.finish() as usize % self.stats.capacity_blocks
        }
    }
}
impl std::fmt::Display for IndexCache {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let collisions = self.collisions.load(std::sync::atomic::Ordering::SeqCst);
        write!(f, "{}, {} collisions", self.stats, collisions)
    }
}
impl std::fmt::Debug for IndexCache {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

#[async_trait::async_trait]
impl<E> DataBlockCache<E> for IndexCache {
    fn new(capacity_mb: u64, block_size: u64, _total_size: u64) -> Result<Self, CacheError<E>> {
        let capacity_blocks = (capacity_mb as f64 / block_size as f64 * 1e6).ceil() as usize;
        debug!(capacity_blocks, capacity_mb, "Allocating index cache",);
        if capacity_blocks < 4 {
            return Err(CacheError::TooFewBlocks(capacity_blocks));
        }

        Ok(Self {
            stats: CacheStats {
                capacity_mb,
                capacity_blocks,
                block_size,
                ..Default::default()
            },
            data: (0..capacity_blocks)
                .map(|_| bytes::BytesMut::zeroed(block_size as usize))
                .map(|data| Block { pos: None, data })
                .map(RwLock::new)
                .collect(),
            collisions: Default::default(),
            use_hash: false,
        })
    }
    fn stats(&self) -> &CacheStats {
        &self.stats
    }
    async fn get(&self, pos: u64) -> Option<RwLockReadGuard<Block>> {
        let data = self.data[self.key(pos)].read().await;
        if data.pos == Some(pos) {
            self.stats.add_hit();
            return Some(data);
        }
        self.stats.add_miss();
        None
    }
    async fn insert_lock<A: AsRef<[u8]> + Send>(
        &self,
        pos: u64,
        f: impl std::future::Future<Output = Result<A, E>> + Send,
    ) -> Result<RwLockReadGuard<Block>, CacheError<E>> {
        let key = self.key(pos);
        let mut block = self.data[key].write().await;
        match block.pos {
            Some(pos2) if pos2 == pos => {
                self.stats.remove_miss();
                return Ok(block.downgrade());
            }
            Some(_) => {
                self.collisions
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            None => {}
        }

        block.pos = Some(pos);
        let buf = f.await.map_err(|e| CacheError::Insert(e))?;
        let buf = buf.as_ref();
        block.data.resize(buf.len(), 0);
        block.data.copy_from_slice(buf);
        return Ok(block.downgrade());
    }
}
