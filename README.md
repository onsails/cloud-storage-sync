# cloud-storage-sync
Library to sync files to, from and between Google Cloud Storage buckets

This project depends on [cloud-storage-rs](https://github.com/ThouCheese/cloud-storage-rs.git) crate.
To access bucket you need to specify SERVICE_ACCOUNT environment variable which should contain path to the service account json key.

```rust
let force_overwrite = false;
let sync = Sync::new(force_overwrite);

sync.copy_local_file_to_gcs(
    "some_local_file",
    BUCKET,
    "prefix/somefile",
)?;

sync.copy_local_dir_to_gcs(
    "some_local_dir", 
    BUCKET,
    "some/directory",
)?;
```