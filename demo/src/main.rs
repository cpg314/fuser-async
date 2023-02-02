use std::path::{Path, PathBuf};
use std::process;

use clap::{Parser, Subcommand};
use fuser_async_local as local;
use fuser_async_s3 as s3;
use tracing::*;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::prelude::*;

use fuser_async::cache::LRUCache;
use fuser_async::{FilesystemFUSE, FilesystemSSUS};

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Input {
    Local {
        root: PathBuf,
    },
    S3 {
        #[clap(flatten)]
        config: s3::Config,
        #[clap(flatten)]
        fetch_config: s3::FetchConfig,
    },
}

#[derive(Parser)]
struct Flags {
    #[clap(subcommand)]
    input: Input,
    /// Mountpoint
    mountpoint: PathBuf,
    #[clap(long, short)]
    debug: bool,
}

async fn mount<F: FilesystemSSUS>(fs: F, mountpoint: &Path) -> anyhow::Result<()>
where
    F::Error: std::fmt::Display,
{
    let fuse = FilesystemFUSE::new(fs);

    let _mount = fuser::spawn_mount2(fuse, mountpoint, &[fuser::MountOption::Async])?;
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn main_impl(args: Flags) -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(if args.debug {
            LevelFilter::DEBUG
        } else {
            LevelFilter::INFO
        })
        .with(Some(tracing_subscriber::fmt::layer()).with_filter(
            tracing_subscriber::filter::filter_fn(|metadata| {
                !(metadata.target().starts_with("hyper") && metadata.level() <= &Level::DEBUG)
            }),
        ))
        .init();

    match args.input {
        Input::Local { root } => {
            let fs = local::LocalFilesystem::new(&root).await?;
            mount(fs, &args.mountpoint).await?;
        }
        Input::S3 {
            config,
            fetch_config,
        } => {
            let fs = s3::S3Filesystem::<LRUCache>::new(config, fetch_config).await?;
            mount(fs, &args.mountpoint).await?;
        }
    };

    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Flags::parse();
    if let Err(e) = main_impl(args).await {
        error!("{:?}", e);
        process::exit(1)
    }
}
