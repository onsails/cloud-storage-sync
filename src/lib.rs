#[macro_use]
extern crate arrayref;

pub mod error;
pub mod gcs;
pub mod local;

pub use gcs::*;
pub use local::*;

mod util;

use crate::error::*;

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use crate::util::*;

    use super::*;
    use cloud_storage::{Client, ListRequest};
    use futures::TryStreamExt;
    use snafu::ResultExt;
    use std::io::Read;
    use std::io::Write;
    use std::sync::Mutex;
    use std::{
        fs::{create_dir, remove_dir_all, File},
        path::{Path, PathBuf},
    };
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

            let client = Client::default(); // FIXME: get from local2gcs

            let populated = PopulatedDir::new().unwrap();
            let local = LocalSource::new(false, 2);

            for i in 0..2 {
                let op_count = local
                    .to_gcs(&populated.somefile, &env_bucket(), &prefix)
                    .await
                    .unwrap();
                if i == 0 {
                    assert_eq!(op_count, 1);
                } else {
                    assert_eq!(op_count, 0);
                }
            }

            let object = client
                .object()
                .read(&env_bucket(), &format!("{}/somefile", prefix))
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

            let gcs = GcsSource::new(false, 2);
            let local = LocalSource::new(false, 2);

            for i in 0..2 {
                log::info!("upload iter {}", i);
                let op_count = local
                    .to_gcs(
                        populated.tempdir.to_str_wrap().unwrap().to_owned(),
                        &env_bucket(),
                        prefix,
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
                let op_count = gcs
                    .to_local(&env_bucket(), prefix, dir.as_ref())
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
        let client = Client::default();
        let objects = client
            .object()
            .list(
                &bucket,
                ListRequest {
                    prefix: Some(prefix.to_owned()),
                    ..Default::default()
                },
            )
            .await?;
        objects
            .try_for_each(|objects| async {
                for object in objects.items {
                    log::trace!("deleting gs://{}{}", &object.bucket, &object.name);
                    client.object().delete(&object.bucket, &object.name).await?;
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
            let empty_dir = format!("{}/empty_dir", path.as_ref().to_str().unwrap());
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
