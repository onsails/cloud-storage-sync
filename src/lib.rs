#![feature(str_strip)]
use cloud_storage::object::Object;
use cloud_storage::Error as CSError;
use snafu::Snafu;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Error as IoError};
use std::path::{Path, PathBuf};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    CloudStorage {
        source: CSError,
    },
    #[snafu(context(false))]
    Io {
        source: IoError,
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

pub enum Source {
    GCS {
        bucket: &'static str,
        path: &'static str,
    },
    Local(PathBuf),
}

pub enum Destination {
    GCS {
        bucket: &'static str,
        path: &'static str,
    },
    Local(PathBuf),
}

impl Source {
    fn copy_to(&self, dst: Destination) -> Result<(), Error> {
        match dst {
            Destination::GCS { bucket, path } => self.copy_to_gcs(bucket, path),
            Destination::Local(path) => self.copy_to_local(&path),
        }
    }

    fn copy_to_local(&self, path_dst: &PathBuf) -> Result<(), Error> {
        match self {
            Source::GCS { bucket, path } => Self::copy_gcs_to_local(bucket, path, path_dst),
            Source::Local(path) => Sync::copy_local_to_local(path, path_dst),
        }
    }

    fn copy_gcs_to_local(
        bucket_src: &str,
        path_src: &str,
        path_dst: &PathBuf,
    ) -> Result<(), Error> {
        let objects_src = Object::list_prefix(bucket_src, path_src)?;
        for object_src in objects_src {
            let path = object_src
                .name
                .strip_prefix(path_src)
                .ok_or_else(|| Error::Other {
                    message: "Failed to strip path prefix",
                })?;
            let path_dst = path_dst.join(path);
            let file_dst = File::open(path_dst)?;
            let mut writer = BufWriter::new(file_dst);
            let url_src = object_src.download_url(60)?;
            let mut response_src = reqwest::blocking::get(&url_src)?;
            response_src.copy_to(&mut writer)?;
        }
        Ok(())
    }

    fn copy_to_gcs(&self, bucket_dst: &str, path_dst: &str) -> Result<(), Error> {
        match self {
            Source::GCS { bucket, path } => {
                Sync::copy_gcs_to_gcs(bucket, path, bucket_dst, path_dst)
            }
            Source::Local(path) => {
                Sync::copy_local_to_gcs(path, bucket_dst, &PathBuf::from(path_dst))
            }
        }
    }
}

pub struct Sync;

impl Sync {
    #[doc(hidden)]
    pub fn copy_local_to_local(path_src: &PathBuf, path_dst: &PathBuf) -> Result<(), Error> {
        unimplemented!()
    }
    /**
     * Copies local file or directory to GCS bucket
     * if path_src is a file then the resulting object will be [bucket_dst]/[path_dst]/[filename]
     * where [filename] is a string after the last "/" of the path_src
     */
    pub fn copy_local_to_gcs(
        path_src: impl AsRef<Path>,
        bucket_dst: &str,
        path_dst: impl AsRef<Path>,
    ) -> Result<(), Error> {
        let path_buf = PathBuf::from(path_src.as_ref());
        if path_buf.is_dir() {
            Self::copy_local_dir_to_gcs(path_src, bucket_dst, path_dst)?;
            Ok(())
        } else {
            let filename = path_buf.file_name().ok_or_else(|| Error::Other {
                message: "path_src is not a file, should never happen, please report an issue",
            })?;
            let path_dst = path_dst.as_ref().join(filename);
            Self::copy_local_file_to_gcs(path_src, bucket_dst, path_dst)?;
            Ok(())
        }
    }

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

    /**
     * Copies local directory to gcs bucket
     * the resulting filenames will be [path_dst]/[filename]
     * where [filename] is path relative to the path_src
     */
    pub fn copy_local_dir_to_gcs(
        path_src: impl AsRef<Path>,
        bucket: &str,
        path_dst: impl AsRef<Path>,
    ) -> Result<(), Error> {
        for entry in fs::read_dir(path_src)? {
            let entry = entry?;
            let entry_path = entry.path();
            let path_dst = path_dst.as_ref().join(entry.file_name());
            if entry_path.is_dir() {
                Self::copy_local_dir_to_gcs(&entry_path, bucket, &path_dst)?;
            } else {
                Self::copy_local_file_to_gcs(&entry_path, bucket, &path_dst)?;
            }
        }
        Ok(())
    }

    /**
     * Copies local file to gcs bucket
     */
    pub fn copy_local_file_to_gcs(
        path_src: impl AsRef<Path>,
        bucket: &str,
        filename: impl AsRef<Path>,
    ) -> Result<(), Error> {
        log::trace!(
            "Copy {:?} to gs://{}/{}",
            path_src.as_ref(),
            bucket,
            filename.as_ref().display()
        );
        let file_src = File::open(path_src.as_ref())?;
        let metadata = file_src.metadata()?;
        let length = metadata.len();
        let reader = BufReader::new(file_src);
        let mime_type = mime_guess::from_path(path_src).first_or(mime::APPLICATION_OCTET_STREAM);
        let mime_type_str = mime_type.essence_str();
        let filename_path = filename.as_ref();
        let filename = filename_path.to_str().ok_or_else(|| Error::WrongPath {
            path: filename_path.to_path_buf(),
        })?;
        Object::create_streamed(bucket, reader, length, filename, mime_type_str)?;
        Ok(())
    }
}

#[cfg(test)]
#[macro_use]
extern crate arrayref;

#[cfg(test)]
mod tests {
    use super::*;
    use cloud_storage::object::Object;
    use std::fs::{create_dir, remove_dir_all, File};
    use std::io::Read;
    use std::io::Write;
    use tempdir::TempDir;

    const BUCKET: &str = "onsails-cloud-storage-sync";

    #[test]
    fn test_local_file_upload() {
        let prefix = "local_file_upload";
        init(prefix);
        let populated = PopulatedDir::new().unwrap();
        Sync::copy_local_file_to_gcs(
            &populated.somefile,
            BUCKET,
            &PathBuf::from(format!("{}/somefile-renamed", prefix)),
        )
        .unwrap();
        let object = Object::read(BUCKET, &format!("{}/somefile-renamed", prefix)).unwrap();
        assert_eq!(file_crc32c(&populated.somefile), object.crc32c_decode());
        populated.remove().unwrap();
        clear_bucket(prefix).unwrap();
    }

    #[test]
    fn test_local_dir_upload() {
        let prefix = "local_dir_upload";
        init(prefix);
        let populated = PopulatedDir::new().unwrap();
        Sync::copy_local_dir_to_gcs(populated.tempdir.path(), BUCKET, &PathBuf::from(prefix))
            .unwrap();
        populated.remove().unwrap();
        clear_bucket(prefix).unwrap();
    }

    fn init(prefix: &str) {
        let _ = env_logger::try_init();
        clear_bucket(prefix).unwrap();
    }

    fn clear_bucket(prefix: &str) -> Result<(), cloud_storage::Error> {
        let objects = Object::list_prefix(&BUCKET, prefix)?;
        for object in objects {
            object.delete()?;
        }
        Ok(())
    }

    fn file_crc32c(file: &PathBuf) -> u32 {
        let mut file = File::open(file).unwrap();
        let mut buf = vec![];
        file.read_to_end(&mut buf).unwrap();
        crc32c::crc32c(&buf)
    }

    struct PopulatedDir {
        pub tempdir: TempDir,
        pub somefile: PathBuf,
        pub dirpath: PathBuf,
        pub dirfile: PathBuf,
    }

    impl PopulatedDir {
        fn new() -> Result<PopulatedDir, std::io::Error> {
            let tempdir = TempDir::new("cloud-storage-sync")?;
            let filepath = tempdir.as_ref().join("somefile");
            let mut file = File::create(filepath.clone())?;
            write!(&mut file, "somefilecontents")?;
            let dirpath = tempdir.as_ref().join("somedir");
            create_dir(dirpath.clone())?;
            let dirfilepath = dirpath.join("dirfile");
            let mut dirfile = File::create(dirfilepath.clone())?;
            write!(&mut dirfile, "dirfilecontents")?;
            Ok(PopulatedDir {
                tempdir,
                somefile: filepath,
                dirpath,
                dirfile: dirfilepath,
            })
        }

        fn remove(self) -> Result<(), std::io::Error> {
            remove_dir_all(self.tempdir)?;
            Ok(())
        }
    }

    trait CrcDecode {
        fn crc32c_decode(&self) -> u32;
    }

    impl CrcDecode for Object {
        fn crc32c_decode(&self) -> u32 {
            let crc32c_vec = base64::decode(&self.crc32c).unwrap();
            u32::from_be_bytes(*array_ref!(crc32c_vec, 0, 4))
        }
    }
}
