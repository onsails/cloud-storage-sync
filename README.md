# cloud-storage-sync

![docs](https://docs.rs/cloud-storage-sync/badge.svg)

Library to sync files to, from and between Google Cloud Storage buckets

This project depends on [cloud-storage-rs](https://github.com/ThouCheese/cloud-storage-rs.git) crate.
To access bucket you need to specify SERVICE_ACCOUNT environment variable which should contain path to the service account json key.

```rust
let force_overwrite = false;
let sync = Sync::new(force_overwrite);

sync.sync_local_file_to_gcs(
    "some_local_file",
    BUCKET,
    "prefix/somefile",
)?;

sync.sync_local_to_gcs(
    "/some/local/file_or_dir", 
    BUCKET,
    "some/directory",
)?;

sync.sync_gcs_to_local(
    BUCKET,
    "myprefix",
    "../some/directory"
)?;
```
