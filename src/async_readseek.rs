use std::future::Future;

use crate::FilesystemSSUS;

// Inspired by the [`tokio::io`] implementation of [`tokio::io::AsyncRead`] on [`tokio::io::File`].
#[derive(Debug)]
enum State<E> {
    Idle,
    Busy(tokio::task::JoinHandle<Operation<E>>),
}

#[derive(Debug)]
enum Operation<E> {
    Read(Result<bytes::Bytes, E>),
}
struct FileHandleInner<E> {
    state: State<E>,
    pos: u64,
}
impl<E> Default for FileHandleInner<E> {
    fn default() -> Self {
        Self {
            state: State::Idle,
            pos: 0,
        }
    }
}
macro_rules! ready {
    ($e:expr) => {
        match $e {
            std::task::Poll::Ready(t) => t,
            std::task::Poll::Pending => {
                return std::task::Poll::Pending;
            }
        }
    };
}
/// Owned handle on a FUSE filesystem file, implementing [`tokio::io::AsyncRead`].
///
/// The handle is released when the object is dropped
pub struct FileHandle<F: FilesystemSSUS + Clone>
where
    F::Error: Send,
{
    fs: F,
    fh: u64,
    inode: u64,
    inner: tokio::sync::Mutex<FileHandleInner<F::Error>>,
}

impl<F: FilesystemSSUS + Clone> FileHandle<F>
where
    F::Error: Send,
{
    /// Create a new owned handle (calling `open` with the provided flags) on an inode.
    pub async fn new(fs: F, inode: u64, flags: i32) -> Result<FileHandle<F>, F::Error> {
        let fh = fs.open(inode, flags).await?;
        Ok(FileHandle {
            fs,
            fh,
            inode,
            inner: Default::default(),
        })
    }
}

impl<F: FilesystemSSUS + Clone> Drop for FileHandle<F>
where
    F::Error: Send,
{
    fn drop(&mut self) {
        let fs = self.fs.clone();
        let fh = self.fh;
        let inode = self.inode;
        tokio::spawn(async move { fs.release(inode, fh).await });
    }
}

impl<F: FilesystemSSUS + Clone> tokio::io::AsyncRead for FileHandle<F>
where
    F::Error: Send + std::fmt::Display,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        dst: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<tokio::io::Result<()>> {
        let me = self.get_mut();
        let inner = me.inner.get_mut();
        let capacity = dst.remaining();

        loop {
            match inner.state {
                State::Idle => {
                    let (inode, fh, fs) = (me.inode, me.fh, me.fs.clone());
                    let offset = inner.pos;
                    inner.state = State::Busy(tokio::task::spawn(async move {
                        Operation::Read(fs.read(inode, fh, offset as i64, capacity as u32).await)
                    }))
                }
                State::Busy(ref mut rx) => {
                    let op = ready!(std::pin::Pin::new(rx).poll(cx))?;
                    match op {
                        Operation::Read(Ok(buf)) => {
                            dst.put_slice(&buf);
                            inner.pos += buf.len() as u64;
                            inner.state = State::Idle;
                            return std::task::Poll::Ready(Ok(()));
                        }
                        Operation::Read(Err(e)) => {
                            inner.state = State::Idle;

                            return std::task::Poll::Ready(Err(tokio::io::Error::new(
                                tokio::io::ErrorKind::Other,
                                e.to_string(),
                            )));
                        }
                    }
                }
            }
        }
    }
}

impl<F: FilesystemSSUS + Clone> tokio::io::AsyncSeek for FileHandle<F>
where
    F::Error: Send,
{
    fn start_seek(
        self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> tokio::io::Result<()> {
        let me = self.get_mut();
        let inner = me.inner.get_mut();
        match inner.state {
            State::Idle => {
                inner.pos = match position {
                    std::io::SeekFrom::Start(s) => s,
                    std::io::SeekFrom::Current(s) => {
                        if s >= 0 {
                            inner.pos.checked_add(s as u64).unwrap_or_default()
                        } else {
                            inner.pos.checked_sub(s.unsigned_abs()).unwrap_or_default()
                        }
                    }
                    _ => {
                        return Err(tokio::io::Error::new(
                            tokio::io::ErrorKind::Other,
                            "Unsupported seek mode",
                        ))
                    }
                };

                Ok(())
            }
            State::Busy(_) => Err(tokio::io::Error::new(
                tokio::io::ErrorKind::Other,
                "other file operation is pending, call poll_complete before start_seek",
            )),
        }
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        let me = self.get_mut();
        let inner = me.inner.get_mut();
        std::task::Poll::Ready(Ok(inner.pos))
    }
}
