use cloud_storage::Object;
use std::path::Path;
use tokio::{fs::*, io::AsyncReadExt};

pub(crate) struct FileUtil;

impl FileUtil {
    pub(crate) async fn exists(path: impl AsRef<Path>) -> bool {
        metadata(path).await.is_ok()
    }

    pub(crate) async fn is_dir(path: impl AsRef<Path>) -> bool {
        metadata(path).await.map(|m| m.is_dir()).unwrap_or(false)
    }
}

pub(crate) trait CrcDecode {
    fn crc32c_decode(&self) -> u32;
}

impl CrcDecode for Object {
    fn crc32c_decode(&self) -> u32 {
        let crc32c_vec = base64::decode(&self.crc32c).unwrap();
        u32::from_be_bytes(*array_ref!(crc32c_vec, 0, 4))
    }
}

pub(crate) async fn file_crc32c(file: impl AsRef<Path>) -> Result<u32, std::io::Error> {
    let mut file = File::open(file).await.unwrap();

    let mut crc = 0u32;
    loop {
        let len = {
            let mut buffer = bytes::BytesMut::with_capacity(1024 * 8);
            file.read_buf(&mut buffer).await?;
            crc = crc32c::crc32c_append(crc, &buffer);
            buffer.len()
        };
        if len == 0 {
            break;
        }
    }
    Ok(crc)
}

// https://stackoverflow.com/questions/62290132/how-do-i-convert-a-futures-ioasyncread-to-rusotobytestream

// const KB: usize = 1024;

// pub(crate) struct ByteStream<R: AsyncRead + Unpin>(pub(crate) R);

// impl<R: AsyncRead + Unpin> Stream for ByteStream<R> {
//     type Item = Result<Bytes, std::io::Error>;

//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
//         let mut buf = vec![0_u8; 8 * KB];

//         match ready!(Pin::new(&mut self.0).poll_read(cx, &mut buf[..])) {
//             Ok(n) if n != 0 => Some(Ok(Bytes::from(buf))).into(),
//             Ok(_) => None.into(),
//             Err(e) => Some(Err(e)).into(),
//         }
//     }
// }
