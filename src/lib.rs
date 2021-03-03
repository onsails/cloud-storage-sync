#[macro_use]
extern crate arrayref;

pub mod error;
pub mod gcs2local;

pub use gcs2local::*;

mod util;

use crate::error::*;
use crate::util::*;
use cloud_storage::object::Object;
use futures::future::{BoxFuture, FutureExt};
use futures::stream::TryStreamExt;
use snafu::{futures::TryStreamExt as SnafuTryStreamExt, ResultExt};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Source {
    GCS { bucket: String, path: String },
    Local(PathBuf),
}

#[derive(Debug)]
pub enum Destination {
    GCS { bucket: String, path: String },
    Local(PathBuf),
}

impl Source {
    /// Syncs to a destination
    ///
    /// Returns actual downloads/uploads count
    pub async fn sync_to(&self, dst: Destination, sync: &Sync) -> Result<usize, Error> {
        match dst {
            Destination::GCS { bucket, path } => self.sync_to_gcs(&bucket, &path, sync).await,
            Destination::Local(path) => self.sync_to_local(&path, sync).await,
        }
    }

    /// Syncs to a local path
    ///
    /// Returns actual downloads count
    pub async fn sync_to_local(
        &self,
        path_dst: impl AsRef<Path>,
        sync: &Sync,
    ) -> Result<usize, Error> {
        match self {
            Source::GCS { bucket, path } => sync.sync_gcs_to_local(bucket, path, path_dst).await,
            Source::Local(path) => Sync::copy_local_to_local(path, path_dst).await,
        }
    }

    /// Syncs to GCS bucket
    ///
    /// Returns actual uploads count
    pub async fn sync_to_gcs(
        &self,
        bucket_dst: &str,
        path_dst: &str,
        sync: &Sync,
    ) -> Result<usize, Error> {
        match self {
            Source::GCS { bucket, path } => {
                Sync::copy_gcs_to_gcs(bucket, path, bucket_dst, path_dst).await
            }
            Source::Local(path) => sync.sync_local_to_gcs(path, bucket_dst, path_dst).await,
        }
    }
}

#[derive(Debug)]
pub struct Sync {
    force_overwrite: bool,
    gcs2local: GCS2Local,
}

impl Sync {
    /// Creates new Sync instance
    ///
    /// Arguments:
    ///
    /// * `force_overwrite`: Don't do size and checksum comparison, just overwrite everthing
    pub fn new(force_overwrite: bool, concurrency: usize) -> Self {
        let gcs2local = GCS2Local {
            force_overwrite,
            concurrency,
        };
        Self {
            force_overwrite,
            gcs2local,
        }
    }

    #[doc(hidden)]
    #[allow(unused_variables)]
    pub async fn copy_local_to_local(
        path_src: impl AsRef<Path>,
        path_dst: impl AsRef<Path>,
    ) -> Result<usize, Error> {
        unimplemented!()
    }

    /// Syncs remote GCS bucket path to a local path
    ///
    /// Returns actual downloads count
    pub async fn sync_gcs_to_local(
        &self,
        bucket_src: &str,
        path_src: &str,
        path_dst: impl AsRef<Path>,
    ) -> Result<usize> {
        self.gcs2local
            .sync_gcs_to_local(bucket_src, path_src, path_dst)
            .await
    }

    /// Syncs local file or directory to GCS bucket
    /// if path_src is a file then the resulting object will be [bucket_dst]/[path_dst]/[filename]
    /// where [filename] is a string after the last "/" of the path_src
    pub async fn sync_local_to_gcs(
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

    /// Copies remote GCS bucket file or directory to another remote GCS bucket file or directory
    pub async fn copy_gcs_to_gcs(
        bucket_src: &str,
        path_src: &str,
        bucket_dst: &str,
        path_dst: &str,
    ) -> Result<usize, Error> {
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
                (0usize, bucket_dst, path_dst),
                |(mut count, bucket_dst, path_dst), object_srcs| async move {
                    for object_src in object_srcs {
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

    /// Syncs local directory to gcs bucket
    /// the resulting filenames will be [path_dst]/[filename]
    /// where [filename] is path relative to the path_src
    pub fn sync_local_dir_to_gcs(
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
    pub async fn sync_local_file_to_gcs(
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
        if let Ok(object) = Object::read(bucket, filename).await {
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

trait ToStrWrap {
    fn to_str_wrap(&self) -> Result<&str>;
}

impl<P: AsRef<Path>> ToStrWrap for P {
    fn to_str_wrap(&self) -> Result<&str> {
        self.as_ref().to_str().ok_or(Error::Other {
            message: "Can't convert Path to &str, should never happen, please report an issue",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloud_storage::object::Object;
    use std::fs::{create_dir, remove_dir_all, File};
    use std::io::Read;
    use std::io::Write;
    use std::sync::Mutex;
    use tempdir::TempDir;

    lazy_static::lazy_static! {
        // prevent error
        // "dispatch dropped without returning error"
        // caused by parallel tests
        // https://github.com/ThouCheese/cloud-storage-rs/blob/master/src/lib.rs#L118
        static ref RUNTIME: Mutex<tokio::runtime::Runtime> = Mutex::new(tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap());
    }

    #[test]
    fn test_local_file_upload() {
        RUNTIME.lock().unwrap().block_on(async {
            let prefix = "local_file_upload";
            init(prefix).await;
            let populated = PopulatedDir::new().unwrap();
            let sync = Sync::new(false, 2);
            for i in 0..2 {
                let op_count = sync
                    .sync_local_file_to_gcs(
                        &populated.somefile,
                        &env_bucket(),
                        &format!("{}/somefile-renamed", prefix),
                    )
                    .await
                    .unwrap();
                if i == 0 {
                    assert_eq!(op_count, 1);
                } else {
                    assert_eq!(op_count, 0);
                }
            }

            let object = Object::read(&env_bucket(), &format!("{}/somefile-renamed", prefix))
                .await
                .unwrap();
            assert_eq!(
                file_crc32c(&populated.somefile).await.unwrap(),
                object.crc32c_decode()
            );
            populated.remove().unwrap();
            clear_bucket(prefix).await.unwrap();
        });
    }

    #[test]
    fn test_dir_sync() {
        RUNTIME.lock().unwrap().block_on(async {
            let prefix = "local_dir_upload";
            init(prefix).await;
            let populated = PopulatedDir::new().unwrap();
            let sync = Sync::new(false, 2);

            for i in 0..2 {
                log::info!("upload iter {}", i);
                let op_count = sync
                    .sync_local_dir_to_gcs(
                        populated.tempdir.to_str_wrap().unwrap().to_owned(),
                        env_bucket(),
                        prefix.to_owned(),
                    )
                    .await
                    .unwrap();

                if i == 0 {
                    assert_eq!(op_count, 3);
                } else {
                    assert_eq!(op_count, 0);
                }
            }

            let dir = TempDir::new("cloud-storage-sync").unwrap();
            for i in 0..2 {
                let op_count = sync
                    .sync_gcs_to_local(&env_bucket(), prefix, dir.as_ref())
                    .await
                    .unwrap();
                populated.assert_match(&dir.as_ref()).unwrap();

                if i == 0 {
                    // 2 op_count because we don't need to download an empty_dir/ object
                    assert_eq!(op_count, 2);
                } else {
                    assert_eq!(op_count, 0);
                }
            }

            populated.remove().unwrap();
            clear_bucket(prefix).await.unwrap();
        });
    }

    async fn init(prefix: &str) {
        let _ = env_logger::try_init();
        clear_bucket(prefix).await.unwrap();
    }

    async fn clear_bucket(prefix: &str) -> Result<(), cloud_storage::Error> {
        let bucket = env_bucket();
        let objects = Object::list_prefix(&bucket, prefix).await?;
        objects
            .try_for_each(|objects| async {
                for object in objects {
                    log::trace!("deleting gs://{}{}", &object.bucket, &object.name);
                    Object::delete(&object.bucket, &object.name).await?;
                }
                Ok(())
            })
            .await?;
        Ok(())
    }

    fn env_bucket() -> String {
        dotenv::var("BUCKET").unwrap()
    }

    struct PopulatedDir {
        pub tempdir: TempDir,
        pub somefile: PathBuf,
        pub dirpath: PathBuf,
        pub dirfile: PathBuf,
        pub empty: PathBuf,
        pub dirfilecontents: String,
    }

    impl PopulatedDir {
        fn new() -> Result<PopulatedDir, std::io::Error> {
            let tempdir = TempDir::new("cloud-storage-sync")?;
            let filepath = tempdir.as_ref().join("somefile");
            let mut file = File::create(&filepath)?;
            write!(&mut file, "somefilecontents")?;

            let dirpath = tempdir.as_ref().join("somedir");
            create_dir(&dirpath)?;
            let dirfilepath = dirpath.join("dirfile");
            let mut dirfile = File::create(&dirfilepath)?;
            let mut dirfilecontents = String::new();
            for _ in 0..1_000_000 {
                write!(&mut dirfile, "10_bytes_")?;
                dirfilecontents.push_str("10_bytes_");
            }

            let empty = tempdir.as_ref().join("empty_dir");
            create_dir(&empty)?;
            Ok(PopulatedDir {
                tempdir,
                somefile: filepath,
                dirpath,
                dirfile: dirfilepath,
                empty,
                dirfilecontents,
            })
        }

        fn remove(self) -> Result<(), std::io::Error> {
            remove_dir_all(self.tempdir)?;
            Ok(())
        }

        #[allow(clippy::expect_fun_call)]
        fn assert_match(&self, path: impl AsRef<Path>) -> Result<()> {
            self.assert_file_match(&path, "somefile", "somefilecontents")?;
            self.assert_file_match(&path, "somedir/dirfile", &self.dirfilecontents)?;
            let empty_dir = format!("{}/empty_dir", path.to_str_wrap().unwrap());
            assert!(
                std::fs::metadata(empty_dir.clone())
                    .expect(&format!("{} should exist", empty_dir))
                    .is_dir(),
                "empty_dir should be a dir"
            );
            Ok(())
        }

        fn assert_file_match(
            &self,
            in_dir: impl AsRef<Path>,
            path: impl AsRef<Path>,
            content: &str,
        ) -> Result<()> {
            dotenv::dotenv().ok();
            let path = in_dir.as_ref().join(path.as_ref());
            let mut file = File::open(&path).context(Io { path: &path })?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)
                .context(Io { path: &path })?;
            assert_eq!(contents, content);
            Ok(())
        }
    }
}
