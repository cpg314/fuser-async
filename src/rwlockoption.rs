//! Implementation of [`RwLockOption<T>`], with a similar interface as [`tokio::sync::OnceCell`].

use std::sync::Arc;

#[derive(Clone)]
pub struct RwLockOption<T>(Arc<tokio::sync::RwLock<Option<T>>>);
pub type RwLockOptionGuard<T> = tokio::sync::OwnedRwLockReadGuard<Option<T>, T>;
pub type RwLockOptionWriteGuard<T> = tokio::sync::OwnedRwLockMappedWriteGuard<Option<T>, T>;
impl<T> RwLockOption<T> {
    pub async fn get_or_try_init<E, F: std::future::Future<Output = Result<T, E>>>(
        &self,
        f: impl FnOnce() -> F,
    ) -> Result<RwLockOptionGuard<T>, E> {
        let s = self.0.clone().read_owned().await;
        let s = if s.is_none() {
            drop(s);
            let mut s = self.0.clone().write_owned().await;
            if s.is_none() {
                *s = Some(f().await?);
            }
            // Don't use `downgrade`, because that would create a deadlock with other potential
            // accesses to the function above, trying to acquire a write lock.
            // TODO: Add a test
            // See
            // https://docs.rs/tokio/latest/tokio/sync/struct.RwLock.html#method.read
            // https://docs.rs/tokio/latest/tokio/sync/struct.RwLockWriteGuard.html#method.downgrade
            drop(s);
            self.0.clone().read_owned().await
        } else {
            s
        };
        Ok(tokio::sync::OwnedRwLockReadGuard::map(s, |x| {
            x.as_ref().unwrap()
        }))
    }
    pub async fn get_mut_or_try_init<E, F: std::future::Future<Output = Result<T, E>>>(
        &self,
        f: impl FnOnce() -> F,
    ) -> Result<RwLockOptionWriteGuard<T>, E> {
        let mut s = self.0.clone().write_owned().await;
        if s.is_none() {
            *s = Some(f().await?);
        }
        Ok(tokio::sync::OwnedRwLockWriteGuard::map(s, |s| match s {
            Some(s) => s,
            None => unreachable!(),
        }))
    }
    pub fn is_some(&self) -> Option<bool> {
        self.0.try_read().ok().map(|s| s.is_some())
    }
    pub fn try_read(&self) -> Option<RwLockOptionGuard<T>> {
        let data = self.0.clone().try_read_owned().ok()?;
        if data.is_none() {
            return None;
        }
        Some(tokio::sync::OwnedRwLockReadGuard::map(data, |x| {
            x.as_ref().unwrap()
        }))
    }
    pub async fn clear(&self) -> anyhow::Result<()> {
        let mut s = self.0.try_write()?;
        s.take();
        Ok(())
    }
    pub fn new() -> Self {
        Self(Arc::new(tokio::sync::RwLock::new(None)))
    }
}
impl<T> Default for RwLockOption<T> {
    fn default() -> Self {
        Self::new()
    }
}
