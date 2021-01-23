use snafu::{Backtrace, Snafu};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum OpSource {
    CreateObject,
    CopyObject,
    ReadObject,
    DownloadUrl,
    ListPrefix,
    Pre(Box<Self>),
}

impl OpSource {
    pub fn pre(op: OpSource) -> Self {
        Self::Pre(Box::new(op))
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    CloudStorage {
        #[snafu(source(from(cloud_storage::Error, Box::new)))]
        source: Box<cloud_storage::Error>,
        object: String,
        op: OpSource,
    },
    #[snafu(display("IOError occured, path: {}: {}", "path", "source"))]
    Io {
        path: PathBuf,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Tokio IOError occured, path: {}: {}", "path", "source"))]
    TokioIo {
        path: PathBuf,
        source: tokio::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(context(false))]
    Reqwest {
        source: reqwest::Error,
    },
    Other {
        message: &'static str,
    },
    WrongPath {
        path: PathBuf,
    },
}
