/// Error type, which maps to a [`libc`] error.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Not implemented")]
    Unimplemented,
    #[error("Not a directory")]
    NotDirectory,
    #[error("No such file or directory")]
    NoFileDir,
    #[error("Invalid Argument")]
    InvalidArgument,
    #[error("File already exists")]
    Exists,
    #[error("Bad file descriptor")]
    BadFileDescriptor,
    #[error("Too many open files")]
    TooManyOpenFiles,
    #[error("Read-only filesystem")]
    ReadOnly,
    #[error("Unsupported encoding")]
    Encoding,
    #[error("I/O error: {0}")]
    IO(String),
    #[error("No inodes left")]
    NoInodes,
    #[error("{0}")]
    Other(String),
}
impl Error {
    pub fn io(message: &str) -> Self {
        Self::IO(message.into())
    }
}
impl From<&Error> for libc::c_int {
    fn from(source: &Error) -> Self {
        match source {
            Error::Unimplemented | Error::Encoding => libc::ENOSYS,
            Error::NotDirectory => libc::ENOTDIR,
            Error::NoFileDir => libc::ENOENT,
            Error::NoInodes => libc::ENOSPC,
            Error::Exists => libc::EEXIST,
            Error::ReadOnly => libc::EROFS,
            Error::TooManyOpenFiles => libc::EMFILE,
            Error::InvalidArgument => libc::EINVAL,
            Error::BadFileDescriptor => libc::EBADF,
            Error::IO(_) => libc::EIO,
            Error::Other(_) => libc::EIO,
        }
    }
}
