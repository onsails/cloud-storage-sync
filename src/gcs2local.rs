use crate::error::*;
use crate::util::*;
use crate::Result;
use cloud_storage::object::Object;
use futures::stream::TryStreamExt;
use snafu::{futures::TryStreamExt as SnafuTryStreamExt, ResultExt};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
pub struct GCS2Local {
    pub(crate) force_overwrite: bool,
}

impl GCS2Local {
    /// Syncs remote GCS bucket path to a local path
    ///
    /// Returns actual downloads count
    pub async fn sync_gcs_to_local(
        &self,
        bucket_src: &str,
        path_src: &str,
        path_dst: impl AsRef<Path>,
    ) -> Result<usize> {
        log::trace!(
            "Syncing bucket: {}, path: {} to local path: {:?}",
            bucket_src,
            path_src,
            path_dst.as_ref()
        );
        let objects_src =
            Object::list_prefix(bucket_src, path_src)
                .await
                .context(CloudStorage {
                    object: path_src.to_owned(),
                    op: OpSource::pre(OpSource::ListPrefix),
                })?;
        objects_src
            .context(CloudStorage {
                object: path_src.to_owned(),
                op: OpSource::ListPrefix,
            })
            // .map_err(Error::from)
            .try_fold(
                (0usize, path_dst),
                |(mut count, path_dst), object_srcs| async move {
                    for object_src in object_srcs {
                        count += self
                            .download_object(bucket_src, path_src, path_dst.as_ref(), object_src)
                            .await?;
                    }
                    Ok((count, path_dst))
                },
            )
            .await
            .map(|(count, _)| count)
    }

    async fn download_object(
        &self,
        bucket_src: &str,
        path_src: &str,
        path_dst: impl AsRef<Path>,
        object_src: Object,
    ) -> Result<usize> {
        let mut count = 0;
        let path = object_src.name.strip_prefix(path_src).ok_or({
            Error::Other {
                message: "Failed to strip path prefix, should never happen, please report an issue",
            }
        })?;
        let path = PathBuf::from(path);
        let path = path.strip_prefix("/").unwrap_or_else(|_| path.as_path());
        let path_dst = &path_dst.as_ref().join(path);

        if let Some(dir_dst) = path_dst.parent() {
            if FileUtil::exists(dir_dst).await {
                if !FileUtil::is_dir(dir_dst).await {
                    fs::remove_file(dir_dst)
                        .await
                        .context(Io { path: dir_dst })?;
                }
            } else {
                log::trace!("Creating directory {:?}", &dir_dst);
                fs::create_dir_all(dir_dst)
                    .await
                    .context(Io { path: dir_dst })?;
            }
        }

        if object_src.name.ends_with('/') {
            match path_dst.metadata() {
                Ok(md) if md.is_dir() => {}
                Ok(_) => {
                    std::fs::remove_file(path_dst).context(Io { path: path_dst })?;
                    std::fs::create_dir(path_dst).context(Io { path: path_dst })?;
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::create_dir(path_dst).context(Io { path: path_dst })?;
                }
                Err(err) => {
                    Err(err).context(Io { path: path_dst })?;
                }
            };
        } else if !self.should_download(&object_src, path_dst).await? {
            log::trace!("Skip {:?}", object_src.name);
        } else {
            log::trace!(
                "Copy gs://{}/{} to {:?}",
                bucket_src,
                object_src.name,
                &path_dst,
            );
            let file_dst = File::create(path_dst)
                .await
                .context(Io { path: path_dst })?;

            let url_src = object_src.download_url(60).context(CloudStorage {
                object: object_src.name.to_owned(),
                op: OpSource::DownloadUrl,
            })?;
            let response_src = reqwest::get(&url_src).await?;

            let (file_dst, copied) = response_src
                .bytes_stream()
                .map_err(Error::from)
                .try_fold((file_dst, 0), |(mut file_dst, copied), chunk| async move {
                    let copied = copied + chunk.len();
                    file_dst
                        .write_all(&chunk)
                        .await
                        .context(Io { path: path_dst })?;
                    Ok((file_dst, copied))
                })
                .await?;

            file_dst.sync_all().await.context(Io { path: path_dst })?;
            count += 1;

            log::trace!("Copied {} bytes", copied);
        }
        Ok(count)
    }

    async fn should_download(&self, object: &Object, path_dst: impl AsRef<Path>) -> Result<bool> {
        if self.force_overwrite {
            return Ok(true);
        }

        if !path_dst.as_ref().exists() {
            return Ok(true);
        }

        let dst_len = path_dst
            .as_ref()
            .metadata()
            .context(Io {
                path: path_dst.as_ref(),
            })?
            .len();

        if dst_len != object.size {
            log::trace!("Size mismatch, src: {}, dst: {}", object.size, dst_len);
            Ok(true)
        } else if file_crc32c(path_dst.as_ref()).await.context(Io {
            path: path_dst.as_ref(),
        })? != object.crc32c_decode()
        {
            log::trace!("Crc32c mismatch");
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
