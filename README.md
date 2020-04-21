# cloud-storage-sync
Library to sync files to and from Google Cloud Storage

```rust
Sync::copy_local_file_to_gcs(
    "some_local_file",
    BUCKET,
    "prefix/somefile",
);

Sync::copy_local_dir_to_gcs(
    "some_local_dir", 
    BUCKET,
    "some/directory",
)
```