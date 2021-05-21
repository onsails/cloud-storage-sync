use crate::error::*;
use crate::util::*;
use crate::Result;
use cloud_storage::{object::Object, Client, ListRequest};
use futures::stream::FuturesUnordered;
use futures::stream::{StreamExt, TryStreamExt};
use snafu::{futures::TryStreamExt as SnafuTryStreamExt, ResultExt};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
pub struct GcsSource {
    pub(crate) force_overwrite: bool,
    pub(crate) concurrency: usize,
    pub(crate) client: Client,
}

impl GcsSource {
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

    /// Syncs remote Gcs bucket path to a local path
    ///
    /// Returns actual downloads count
    pub async fn to_local(
        &self,
        bucket_src: &str,
        path_src: &str,
        dst_dir: impl AsRef<Path>,
    ) -> Result<usize> {
        log::trace!(
            "Syncing bucket: {}, path: {} to local path: {:?}",
            bucket_src,
            path_src,
            dst_dir.as_ref()
        );
        let dst_dir = dst_dir.as_ref();
        log::trace!("Requesting objects");
        let objects_src = self
            .client
            .object()
            .list(
                bucket_src,
                ListRequest {
                    prefix: Some(path_src.to_owned()),
                    ..Default::default()
                },
            )
            .await
            .context(CloudStorage {
                object: path_src.to_owned(),
                op: OpSource::pre(OpSource::ListPrefix),
            })?;
        log::trace!("iterating objects");
        objects_src
            .context(CloudStorage {
                object: path_src.to_owned(),
                op: OpSource::ListPrefix,
            })
            // .map_err(Error::from)
            .try_fold(
                (0usize, dst_dir),
                |(mut count, dst_dir), object_srcs| async move {
                    log::trace!("objects: {:?}", object_srcs);
                    let mut jobs_pool = FuturesUnordered::new();

                    for object_src in object_srcs.items {
                        log::trace!("object: {:?}", object_src);

                        if jobs_pool.len() == self.concurrency {
                            // unwrap because it's not empty
                            count += jobs_pool.next().await.unwrap()?;
                        }

                        let strip_prefix = if path_src.ends_with('/') {
                            path_src.to_owned()
                        } else {
                            format!("{}/", path_src)
                        };
                        let stripped_object_name =
                            object_src.name.strip_prefix(&strip_prefix).ok_or({
                                Error::Other {
                message: "Failed to strip path prefix, should never happen, please report an issue",
            }
                            })?;
                        let path_dst = dst_dir.join(stripped_object_name);

                        Self::create_parent_dirs(self.force_overwrite, &path_dst).await?;

                        if object_src.name.ends_with('/') {
                            let created =
                                Self::maybe_create_dir(self.force_overwrite, &path_dst).await?;
                            if let Some(created) = created {
                                log::trace!("Created dir {:?}", created.as_os_str());
                            }
                            continue;
                        }

                        let path_dst = path_dst.to_str().expect("valid utf8 file name").to_owned();

                        log::trace!("downloading object {:?}", object_src);
                        let job = Self::download_object(
                            self.force_overwrite,
                            bucket_src,
                            path_dst,
                            object_src,
                        );

                        jobs_pool.push(job);
                    }

                    log::trace!("waiting for jobs completion");
                    while let Some(job) = jobs_pool.next().await {
                        count += job?;
                    }
                    log::trace!("all jobs completed");

                    Ok((count, dst_dir))
                },
            )
            .await
            .map(|(count, _)| count)
    }

    /// Copies remote Gcs bucket file or directory to another remote Gcs bucket file or directory
    pub async fn to_gcs(
        &self,
        bucket_src: &str,
        path_src: &str,
        bucket_dst: &str,
        path_dst: &str,
    ) -> Result<usize, Error> {
        let objects_src = self
            .client
            .object()
            .list(
                bucket_src,
                ListRequest {
                    prefix: Some(path_src.to_owned()),
                    ..Default::default()
                },
            )
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
                (0usize, bucket_dst, path_dst),
                |(mut count, bucket_dst, path_dst), object_srcs| async move {
                    for object_src in object_srcs.items {
                        object_src
                            .copy(bucket_dst, path_dst)
                            .await
                            .context(CloudStorage {
                                object: path_dst.to_owned(),
                                op: OpSource::CopyObject,
                            })?;
                        count += 1;
                    }

                    Ok((count, bucket_dst, path_dst))
                },
            )
            .await
            .map(|(count, ..)| count)
    }

    async fn create_parent_dirs(force_overwrite: bool, path_dst: impl AsRef<Path>) -> Result<()> {
        let path_dst = PathBuf::from(path_dst.as_ref());

        if let Some(dir_dst) = path_dst.parent() {
            if FileUtil::exists(dir_dst).await {
                if !FileUtil::is_dir(dir_dst).await {
                    if force_overwrite {
                        fs::remove_file(dir_dst)
                            .await
                            .context(Io { path: dir_dst })?;
                    } else {
                        return Err(Error::AlreadyExists { path: path_dst });
                    }
                }
            } else {
                log::trace!("Creating directory {:?}", &dir_dst);
                fs::create_dir_all(dir_dst)
                    .await
                    .context(Io { path: dir_dst })?;
            }
        }

        Ok(())
    }

    async fn maybe_create_dir(
        force_overwrite: bool,
        path_dst: impl AsRef<Path>,
    ) -> Result<Option<PathBuf>> {
        let path_dst = path_dst.as_ref();
        let path_dst = PathBuf::from(path_dst);
        let path_dst = path_dst.as_path();
        match path_dst.metadata() {
            Ok(md) if md.is_dir() => Ok(None),
            Ok(_) => {
                if force_overwrite {
                    std::fs::remove_file(path_dst).context(Io { path: path_dst })?;
                    std::fs::create_dir(path_dst).context(Io { path: path_dst })?;
                    Ok(Some(path_dst.to_owned()))
                } else {
                    Err(Error::AlreadyExists {
                        path: PathBuf::from(path_dst),
                    })
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(path_dst).context(Io { path: path_dst })?;
                Ok(Some(path_dst.to_owned()))
            }
            Err(err) => Err(err).context(Io { path: path_dst }),
        }
    }

    async fn download_object(
        force_overwrite: bool,
        bucket_src: &str,
        path_dst: impl AsRef<Path>,
        object_src: Object,
    ) -> Result<usize> {
        let mut count = 0;
        let path_dst = path_dst.as_ref();

        if !Self::should_download(force_overwrite, &object_src, path_dst).await? {
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

    async fn should_download(
        force_overwrite: bool,
        object: &Object,
        path_dst: impl AsRef<Path>,
    ) -> Result<bool> {
        if force_overwrite {
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
