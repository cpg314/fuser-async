This crate allows building FUSE filesystems where the system calls are directed to `async` functions.

This can be particularly useful when the syscalls benefit from (or require) async IO.
When using a multi-threaded [`tokio`] runtime, this can also be useful for CPU-bound code
(although one should be careful with the [interactions between IO- and CPU-bound tasks](https://docs.rs/tokio/latest/tokio/#cpu-bound-tasks-and-blocking-code)).

More precisely, this crate provides:

- a [`Filesystem`] trait, analogous to the fuser [`fuser::Filesystem`] trait, which has all
  its methods returning futures.
- a [`FilesystemFUSE`] struct, which can be constructed from any implementor of the [`Filesystem`]
  trait, and which implements the [`fuser::Filesystem`] trait, allowing to mount the filesystem
  using the [fuser crate](https://crates.io/crates/fuser) crate.

<small>Behind the scenes, [`FilesystemFUSE`] wraps the inner filesystem into an [`std::sync::Arc<tokio::sync::RwLock>`] and stores a handle on the [`tokio`] runtime. </small>

## Usage

1. Implement the [`Filesystem`] trait.
2. Instantiate a [`FilesystemFUSE`] object from a instance implementing [`Filesystem`],
   with [`FilesystemFUSE::new`].
3. Mount the filesystem using [`fuser`], or use file handles programmatically with [`FileHandle`] (which implements [`tokio::io::AsyncRead`])

```rust,ignore
let fs = TestFs::new();
let fuse = FilesystemFUSE::new(fs);
let _mount = fuser::spawn_mount2(
    fuse,
    mountpoint,
    &[fuser::MountOption::RO, fuser::MountOption::Async],
  )?;
tokio::signal::ctrl_c().await?;

```

## Implementations

Two example implementations are provided:

- [Local filesystem passthrough](fuser-async-local).
- [S3 object storage](fuser-async-s3), using the [`rusty_s3`](https://docs.rs/rusty-s3/latest/rusty_s3/) crate.

See the `demo` binary crate for a binary running these.

```console
$ cargo run -r --features s3,demo --bin demo -- --help
USAGE:
    demo [OPTIONS] <MOUNTPOINT> <SUBCOMMAND>

ARGS:
    <MOUNTPOINT>    Mountpoint

OPTIONS:
    -d, --debug
    -h, --help     Print help information

SUBCOMMANDS:
    local
    s3
```

## Difference with other solutions

## `fuse_mt`

See <https://github.com/wfraser/fuse-mt/issues/3>. The implementation of this crate is fairly similar to the prototype linked in this issue,
except that we use a recent [`tokio`] version and support different syscalls.

## Limitations/TODO

- Not all methods from [`fuser::Filesystem`] are supported (they are however easy to add). In particular, the current focus of is on read operations.
- No tests yet beyond the demo and the dependent crates.
