use snafu::{Backtrace, Snafu};
use std::path::PathBuf;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(context(false))]
    CloudStorage {
        #[snafu(source(from(cloud_storage::Error, Box::new)))]
        source: Box<cloud_storage::Error>,
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
