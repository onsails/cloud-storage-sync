use bytes::Bytes;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::ready;
use futures::Stream;
use std::path::Path;
use tokio::fs::*;
use tokio::io::AsyncRead;

pub(crate) struct FileUtil;

impl FileUtil {
    pub(crate) async fn exists(path: impl AsRef<Path>) -> bool {
        metadata(path).await.is_ok()
    }

    pub(crate) async fn is_dir(path: impl AsRef<Path>) -> bool {
        metadata(path).await.map(|m| m.is_dir()).unwrap_or(false)
    }
}

// https://stackoverflow.com/questions/62290132/how-do-i-convert-a-futures-ioasyncread-to-rusotobytestream

const KB: usize = 1024;

pub(crate) struct ByteStream<R: AsyncRead + Unpin>(pub(crate) R);

impl<R: AsyncRead + Unpin> Stream for ByteStream<R> {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut buf = vec![0_u8; 8 * KB];

        match ready!(Pin::new(&mut self.0).poll_read(cx, &mut buf[..])) {
            Ok(n) if n != 0 => Some(Ok(Bytes::from(buf))).into(),
            Ok(_) => None.into(),
            Err(e) => Some(Err(e)).into(),
        }
    }
}
