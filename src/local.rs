use crate::error::*;
use crate::util::*;
use crate::Result;
use cloud_storage::{object::Object, Client};
use futures::future::{BoxFuture, FutureExt};
use futures::stream::TryStreamExt;
use snafu::{futures::TryStreamExt as SnafuTryStreamExt, ResultExt};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};

#[derive(Debug)]
pub struct LocalSource {
    pub(crate) force_overwrite: bool,
    pub(crate) concurrency: usize,
    pub(crate) client: Client,
}

impl LocalSource {
    pub fn new(force_overwrite: bool, concurrency: usize) -> Self {
        let client = Client::default();
        Self {
            force_overwrite,
            concurrency,
            client,
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Syncs local file or directory to Gcs bucket
    /// if path_src is a file then the resulting object will be [bucket_dst]/[path_dst]/[filename]
    /// where [filename] is a string after the last "/" of the path_src
    pub async fn to_gcs(
        &self,
        path_src: impl AsRef<Path>,
        bucket_dst: &str,
        path_dst: &str,
    ) -> Result<usize, Error> {
        let path_buf = PathBuf::from(path_src.as_ref());
        if path_buf.is_dir() {
            self.sync_local_dir_to_gcs(
                path_src.to_str_wrap()?.to_owned(),
                bucket_dst.to_owned(),
                path_dst.to_owned(),
            )
            .await
        } else {
            let filename = path_buf.file_name().ok_or(Error::Other {
                message: "path_src is not a file, should never happen, please report an issue",
            })?;
            let path_dst = PathBuf::from(path_dst).join(filename);
            let gcs_path_dst = path_dst.to_str_wrap()?;
            self.sync_local_file_to_gcs(path_src, bucket_dst, gcs_path_dst)
                .await
        }
    }

    /// Syncs local directory to gcs bucket
    /// the resulting filenames will be [path_dst]/[filename]
    /// where [filename] is path relative to the path_src
    fn sync_local_dir_to_gcs(
        &self,
        // path_src: impl AsRef<Path>,
        path_src: String,
        bucket: String,
        path_dst: String,
    ) -> BoxFuture<Result<usize>> {
        async move {
            // get dir entries
            let entries = fs::read_dir(&path_src).await.context(TokioIo {
                path: path_src.clone(),
            })?;
            // convert to stream
            let entries = tokio_stream::wrappers::ReadDirStream::new(entries);

            let (entry_count, op_count) = entries
                .context(Io { path: path_src })
                .map_ok(|entry| (entry, bucket.clone(), path_dst.clone()))
                .and_then(|(entry, bucket, path_dst)| async move {
                    let entry_path = entry.path();
                    let path_dst = PathBuf::from(&path_dst).join(entry.file_name());
                    let path_dst = path_dst.to_str_wrap()?.to_owned();
                    if entry_path.is_dir() {
                        self.sync_local_dir_to_gcs(
                            entry_path.to_str_wrap()?.to_owned(),
                            bucket.clone(),
                            path_dst.clone(),
                        )
                        .await
                    } else {
                        self.sync_local_file_to_gcs(&entry_path, &bucket, &path_dst)
                            .await
                    }
                })
                .try_fold(
                    (0usize, 0usize),
                    |(entry_count, op_count), entry_op_count| async move {
                        Ok((entry_count + 1, op_count + entry_op_count))
                    },
                )
                .await?;

            if entry_count == 0 {
                // empty directory, create an object/
                let dir_object = format!("{}/", path_dst);
                match Object::read(&bucket, &dir_object).await {
                    Ok(_) => Ok(0),
                    Err(cloud_storage::Error::Google(response))
                        if response.errors_has_reason(&cloud_storage::Reason::NotFound) =>
                    {
                        log::trace!("Creating gs://{}{}", bucket, dir_object);
                        Object::create(&bucket, vec![], &dir_object, "")
                            .await
                            .context(CloudStorage {
                                object: dir_object,
                                op: OpSource::CreateObject,
                            })?;
                        Ok(1)
                    }
                    Err(e) => Err(e).context(CloudStorage {
                        object: dir_object,
                        op: OpSource::ReadObject,
                    }),
                }
            } else {
                Ok(op_count)
            }
            // .map(|(count, ..)| count);
        }
        .boxed()
    }

    /// Syncs local file and remote object
    async fn sync_local_file_to_gcs(
        &self,
        path_src: impl AsRef<Path>,
        bucket: &str,
        filename: &str,
    ) -> Result<usize> {
        if !self
            .should_upload_local(path_src.as_ref(), bucket, filename)
            .await?
        {
            log::trace!("Skip {:?}", path_src.as_ref());
            Ok(0)
        } else {
            log::trace!(
                "Copy {:?} to gs://{}/{}",
                path_src.as_ref(),
                bucket,
                filename,
            );
            let file_src = File::open(path_src.as_ref()).await.context(Io {
                path: path_src.as_ref(),
            })?;
            let metadata = file_src.metadata().await.context(Io {
                path: path_src.as_ref(),
            })?;
            let length = metadata.len();
            // let stream = ByteStream(Pin::new(Box::new(file_src)));
            let stream = tokio_util::io::ReaderStream::new(file_src);
            // let reader = BufReader::new(file_src);
            let mime_type =
                mime_guess::from_path(path_src).first_or(mime::APPLICATION_OCTET_STREAM);
            let mime_type_str = mime_type.essence_str();
            Object::create_streamed(bucket, stream, length, filename, mime_type_str)
                .await
                .context(CloudStorage {
                    object: filename.to_owned(),
                    op: OpSource::CreateObject,
                })?;
            Ok(1)
        }
    }

    async fn should_upload_local(
        &self,
        path_src: impl AsRef<Path>,
        bucket: &str,
        filename: &str,
    ) -> Result<bool> {
        if self.force_overwrite {
            return Ok(true);
        }

        let src_len = path_src
            .as_ref()
            .metadata()
            .context(Io {
                path: path_src.as_ref(),
            })?
            .len();
        if let Ok(object) = self.client.object().read(bucket, filename).await {
            if object.size != src_len {
                log::trace!("Size mismatch, src: {}, dst: {}", src_len, object.size);
                Ok(true)
            } else if file_crc32c(path_src.as_ref()).await.context(Io {
                path: path_src.as_ref(),
            })? != object.crc32c_decode()
            {
                log::trace!("Crc32c mismatch");
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            // cloud-sync-rs don't provide semantic errors, so on any error we assume here that file does not exists in a bucket
            Ok(true)
        }
    }
}

pub(crate) trait ToStrWrap {
    fn to_str_wrap(&self) -> Result<&str>;
}

impl<P: AsRef<Path>> ToStrWrap for P {
    fn to_str_wrap(&self) -> Result<&str> {
        self.as_ref().to_str().ok_or(Error::Other {
            message: "Can't convert Path to &str, should never happen, please report an issue",
        })
    }
}
