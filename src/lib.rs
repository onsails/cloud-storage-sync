#![feature(str_strip)]
#[macro_use]
extern crate arrayref;

use cloud_storage::object::Object;
use cloud_storage::Error as CSError;
use snafu::{Backtrace, ResultExt, Snafu};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Error as IoError};
use std::path::{Path, PathBuf};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    CloudStorage {
        source: CSError,
    },
    #[snafu(display("IOError occured, path: {}: {}", "path", "source"))]
    Io {
        path: PathBuf,
        source: IoError,
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

type Result<T, E = Error> = std::result::Result<T, E>;

pub enum Source {
    GCS { bucket: String, path: String },
    Local(PathBuf),
}

pub enum Destination {
    GCS { bucket: String, path: String },
    Local(PathBuf),
}

impl Source {
    /// Syncs to a destination
    pub fn sync_to(&self, dst: Destination, sync: &Sync) -> Result<(), Error> {
        match dst {
            Destination::GCS { bucket, path } => self.sync_to_gcs(&bucket, &path, sync),
            Destination::Local(path) => self.sync_to_local(&path, sync),
        }
    }

    /// Syncs to a local path
    pub fn sync_to_local(&self, path_dst: impl AsRef<Path>, sync: &Sync) -> Result<(), Error> {
        match self {
            Source::GCS { bucket, path } => sync.sync_gcs_to_local(bucket, path, path_dst),
            Source::Local(path) => Sync::copy_local_to_local(path, path_dst),
        }
    }

    /// Syncs to GCS bucket
    pub fn sync_to_gcs(&self, bucket_dst: &str, path_dst: &str, sync: &Sync) -> Result<(), Error> {
        match self {
            Source::GCS { bucket, path } => {
                Sync::copy_gcs_to_gcs(bucket, path, bucket_dst, path_dst)
            }
            Source::Local(path) => sync.sync_local_to_gcs(path, bucket_dst, path_dst),
        }
    }
}

pub struct Sync {
    force_overwrite: bool,
}

impl Sync {
    /// Creates new Sync instance
    ///
    /// Arguments:
    ///
    /// * `force_overwrite`: Don't do size and checksum comparison, just overwrite everthing
    pub fn new(force_overwrite: bool) -> Self {
        Self { force_overwrite }
    }

    #[doc(hidden)]
    #[allow(unused_variables)]
    pub fn copy_local_to_local(
        path_src: impl AsRef<Path>,
        path_dst: impl AsRef<Path>,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    /// Syncs remote GCS bucket path to a local path
    pub fn sync_gcs_to_local(
        &self,
        bucket_src: &str,
        path_src: &str,
        path_dst: impl AsRef<Path>,
    ) -> Result<()> {
        let objects_src = Object::list_prefix(bucket_src, path_src)?;
        for object_src in objects_src {
            let path = object_src
                .name
                .strip_prefix(path_src)
                .ok_or_else(|| Error::Other {
                    message:
                        "Failed to strip path prefix, should never happen, please report an issue",
                })?;
            let path = PathBuf::from(path);
            let path = path.strip_prefix("/").unwrap_or_else(|_| path.as_path());
            let path_dst = &path_dst.as_ref().join(path);

            if let Some(dir_dst) = path_dst.parent() {
                if dir_dst.exists() {
                    if !dir_dst.is_dir() {
                        fs::remove_file(dir_dst).context(Io { path: dir_dst })?;
                    }
                } else {
                    log::trace!("Creating directory {:?}", &dir_dst);
                    fs::create_dir_all(dir_dst).context(Io { path: dir_dst })?;
                }
            }

            if !self.should_download_remote(&object_src, path_dst)? {
                log::trace!("Skip {:?}", object_src.name);
            } else {
                log::trace!(
                    "Copy gs://{}/{} to {:?}",
                    bucket_src,
                    object_src.name,
                    &path_dst,
                );
                let file_dst = File::create(path_dst).context(Io { path: path_dst })?;
                let mut writer = BufWriter::new(file_dst);
                let url_src = object_src.download_url(60)?;
                let mut response_src = reqwest::blocking::get(&url_src)?;
                response_src.copy_to(&mut writer)?;
            }
        }
        Ok(())
    }
    /// Syncs local file or directory to GCS bucket
    /// if path_src is a file then the resulting object will be [bucket_dst]/[path_dst]/[filename]
    /// where [filename] is a string after the last "/" of the path_src
    pub fn sync_local_to_gcs(
        &self,
        path_src: impl AsRef<Path>,
        bucket_dst: &str,
        path_dst: &str,
    ) -> Result<(), Error> {
        let path_buf = PathBuf::from(path_src.as_ref());
        if path_buf.is_dir() {
            self.sync_local_dir_to_gcs(path_src, bucket_dst, path_dst)?;
            Ok(())
        } else {
            let filename = path_buf.file_name().ok_or_else(|| Error::Other {
                message: "path_src is not a file, should never happen, please report an issue",
            })?;
            let path_dst = PathBuf::from(path_dst).join(filename);
            let gcs_path_dst = path_dst.to_str_wrap()?;
            self.sync_local_file_to_gcs(path_src, bucket_dst, gcs_path_dst)?;
            Ok(())
        }
    }

    /// Copies remote GCS bucket file or directory to another remote GCS bucket file or directory
    pub fn copy_gcs_to_gcs(
        bucket_src: &str,
        path_src: &str,
        bucket_dst: &str,
        path_dst: &str,
    ) -> Result<(), Error> {
        let objects_src = Object::list_prefix(bucket_src, path_src)?;
        for object_src in objects_src {
            object_src.copy(bucket_dst, path_dst)?;
        }
        Ok(())
    }

    /// Syncs local directory to gcs bucket
    /// the resulting filenames will be [path_dst]/[filename]
    /// where [filename] is path relative to the path_src
    pub fn sync_local_dir_to_gcs(
        &self,
        path_src: impl AsRef<Path>,
        bucket: &str,
        path_dst: &str,
    ) -> Result<()> {
        for entry in fs::read_dir(path_src.as_ref()).context(Io {
            path: path_src.as_ref(),
        })? {
            let entry = entry.context(Io {
                path: path_src.as_ref(),
            })?;
            let entry_path = entry.path();
            let path_dst = PathBuf::from(path_dst).join(entry.file_name());
            let path_dst = path_dst.to_str_wrap()?;
            if entry_path.is_dir() {
                self.sync_local_dir_to_gcs(&entry_path, bucket, path_dst)?;
            } else {
                self.sync_local_file_to_gcs(&entry_path, bucket, path_dst)?;
            }
        }
        Ok(())
    }

    /// Syncs local file and remote object
    pub fn sync_local_file_to_gcs(
        &self,
        path_src: impl AsRef<Path>,
        bucket: &str,
        filename: &str,
    ) -> Result<()> {
        if !self.should_upload_local(path_src.as_ref(), bucket, filename)? {
            log::trace!("Skip {:?}", path_src.as_ref());
            Ok(())
        } else {
            log::trace!(
                "Copy {:?} to gs://{}/{}",
                path_src.as_ref(),
                bucket,
                filename,
            );
            let file_src = File::open(path_src.as_ref()).context(Io {
                path: path_src.as_ref(),
            })?;
            let metadata = file_src.metadata().context(Io {
                path: path_src.as_ref(),
            })?;
            let length = metadata.len();
            let reader = BufReader::new(file_src);
            let mime_type =
                mime_guess::from_path(path_src).first_or(mime::APPLICATION_OCTET_STREAM);
            let mime_type_str = mime_type.essence_str();
            Object::create_streamed(bucket, reader, length, filename, mime_type_str)?;
            Ok(())
        }
    }

    fn should_download_remote(&self, object: &Object, path_dst: impl AsRef<Path>) -> Result<bool> {
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
            Ok(true)
        } else if file_crc32c(path_dst.as_ref()).context(Io {
            path: path_dst.as_ref(),
        })? != object.crc32c_decode()
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn should_upload_local(
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
        if let Ok(object) = Object::read(bucket, filename) {
            if object.size != src_len {
                Ok(true)
            } else if file_crc32c(path_src.as_ref()).context(Io {
                path: path_src.as_ref(),
            })? != object.crc32c_decode()
            {
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

pub(crate) trait CrcDecode {
    fn crc32c_decode(&self) -> u32;
}

impl CrcDecode for Object {
    fn crc32c_decode(&self) -> u32 {
        let crc32c_vec = base64::decode(&self.crc32c).unwrap();
        u32::from_be_bytes(*array_ref!(crc32c_vec, 0, 4))
    }
}

pub(crate) fn file_crc32c(file: impl AsRef<Path>) -> Result<u32, std::io::Error> {
    let file = File::open(file).unwrap();
    let mut reader = BufReader::new(file);
    let mut crc = 0u32;
    loop {
        let len = {
            let buffer = reader.fill_buf()?;
            crc = crc32c::crc32c_append(crc, buffer);
            buffer.len()
        };
        if len == 0 {
            break;
        }
        reader.consume(len);
    }
    Ok(crc)
}

trait ToStrWrap {
    fn to_str_wrap(&self) -> Result<&str>;
}

impl<P: AsRef<Path>> ToStrWrap for P {
    fn to_str_wrap(&self) -> Result<&str> {
        self.as_ref().to_str().ok_or_else(|| Error::Other {
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
    use tempdir::TempDir;

    #[test]
    fn test_local_file_upload() {
        let prefix = "local_file_upload";
        init(prefix);
        let populated = PopulatedDir::new().unwrap();
        let sync = Sync::new(false);
        sync.sync_local_file_to_gcs(
            &populated.somefile,
            &env_bucket(),
            &format!("{}/somefile-renamed", prefix),
        )
        .unwrap();
        let object = Object::read(&env_bucket(), &format!("{}/somefile-renamed", prefix)).unwrap();
        assert_eq!(
            file_crc32c(&populated.somefile).unwrap(),
            object.crc32c_decode()
        );
        populated.remove().unwrap();
        clear_bucket(prefix).unwrap();
    }

    #[test]
    fn test_dir_sync() {
        let prefix = "local_dir_upload";
        init(prefix);
        let populated = PopulatedDir::new().unwrap();
        let sync = Sync::new(false);

        sync.sync_local_dir_to_gcs(populated.tempdir.path(), &env_bucket(), prefix)
            .unwrap();
        sync.sync_local_dir_to_gcs(populated.tempdir.path(), &env_bucket(), prefix)
            .unwrap();

        sync.sync_gcs_to_local(&env_bucket(), prefix, &populated.empty)
            .unwrap();
        populated.assert_match(&populated.empty).unwrap();
        sync.sync_gcs_to_local(&env_bucket(), prefix, &populated.empty)
            .unwrap();
        populated.assert_match(&populated.empty).unwrap();

        populated.remove().unwrap();
        clear_bucket(prefix).unwrap();
    }

    fn init(prefix: &str) {
        let _ = env_logger::try_init();
        clear_bucket(prefix).unwrap();
    }

    fn clear_bucket(prefix: &str) -> Result<(), cloud_storage::Error> {
        let objects = Object::list_prefix(&env_bucket(), prefix)?;
        for object in objects {
            object.delete()?;
        }
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
            write!(&mut dirfile, "dirfilecontents")?;

            let empty = tempdir.as_ref().join("empty");
            create_dir(&empty)?;
            Ok(PopulatedDir {
                tempdir,
                somefile: filepath,
                dirpath,
                dirfile: dirfilepath,
                empty,
            })
        }

        fn remove(self) -> Result<(), std::io::Error> {
            remove_dir_all(self.tempdir)?;
            Ok(())
        }

        fn assert_match(&self, path: impl AsRef<Path>) -> Result<()> {
            self.assert_file_match(&path, "somefile", "somefilecontents")?;
            self.assert_file_match(&path, "somedir/dirfile", "dirfilecontents")?;
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
