use buzz_media::{BlobStorage, FilesystemBlobConfig, FilesystemBlobStorage, MediaError};

mod support;

async fn open(root: &std::path::Path, quota_bytes: Option<u64>) -> FilesystemBlobStorage {
    FilesystemBlobStorage::open(FilesystemBlobConfig {
        root: root.to_path_buf(),
        quota_bytes,
    })
    .await
    .expect("filesystem blob store")
}

#[tokio::test]
async fn filesystem_satisfies_shared_blob_contract() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let storage = open(&directory.path().join("objects"), None).await;
    support::run_blob_storage_contract(&storage).await;
    assert_eq!(storage.usage_bytes(), 0);
}

#[tokio::test]
async fn replacement_listing_quota_and_recovery_are_restart_safe() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path().join("objects");
    let storage = open(&root, Some(7)).await;
    storage
        .put("a.bin", b"123", "application/octet-stream")
        .await
        .expect("first object");
    storage
        .put("nested/b.bin", b"4567", "application/octet-stream")
        .await
        .expect("nested object");
    assert_eq!(storage.usage_bytes(), 7);

    storage
        .put("a.bin", b"12", "application/octet-stream")
        .await
        .expect("atomic replacement");
    assert_eq!(storage.usage_bytes(), 6);
    let first = storage.list_page(None, 1).await.expect("first list page");
    assert_eq!(first.objects, vec![("a.bin".to_string(), 2)]);
    assert!(first.is_truncated);
    let second = storage
        .list_page(first.next_continuation_token, 1)
        .await
        .expect("second list page");
    assert_eq!(second.objects, vec![("nested/b.bin".to_string(), 4)]);
    assert!(!second.is_truncated);

    let rejected = storage
        .put("a.bin", b"oversized", "application/octet-stream")
        .await;
    assert!(matches!(rejected, Err(MediaError::StorageError(_))));
    assert_eq!(storage.get("a.bin").await.expect("preserved object"), b"12");
    assert_eq!(storage.usage_bytes(), 6);

    let source = directory.path().join("quota-source.bin");
    tokio::fs::write(&source, b"abc")
        .await
        .expect("quota source");
    assert!(matches!(
        storage
            .put_file("quota.bin", &source, "application/octet-stream")
            .await,
        Err(MediaError::StorageError(_))
    ));
    assert!(!storage.head("quota.bin").await.expect("quota head"));

    tokio::fs::write(root.join("nested/.buzz-tmp-abandoned"), b"partial")
        .await
        .expect("abandoned temporary");
    drop(storage);
    let reopened = open(&root, Some(7)).await;
    assert_eq!(reopened.usage_bytes(), 6);
    assert!(!root.join("nested/.buzz-tmp-abandoned").exists());
    assert_eq!(
        reopened.get("nested/b.bin").await.expect("restart read"),
        b"4567"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        assert_eq!(
            std::fs::metadata(&root)
                .expect("root metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(root.join("a.bin"))
                .expect("object metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[tokio::test]
async fn invalid_and_symlinked_paths_fail_closed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let root = directory.path().join("objects");
    let storage = open(&root, None).await;
    for key in [
        "",
        "/absolute",
        "../escape",
        "nested/../../escape",
        "nested//object",
        r"nested\object",
        ".buzz-tmp-forged",
    ] {
        assert!(
            matches!(
                storage.put(key, b"x", "application/octet-stream").await,
                Err(MediaError::StorageError(_))
            ),
            "unsafe key should fail: {key:?}"
        );
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let outside = directory.path().join("outside");
        std::fs::create_dir(&outside).expect("outside directory");
        symlink(&outside, root.join("linked")).expect("directory symlink");
        assert!(matches!(
            storage
                .put("linked/escape", b"x", "application/octet-stream")
                .await,
            Err(MediaError::StorageError(_))
        ));
        assert!(!outside.join("escape").exists());
        assert!(matches!(
            storage.list_page(None, 100).await,
            Err(MediaError::StorageError(_))
        ));
    }
}

#[tokio::test]
async fn listing_is_globally_lexicographic_and_page_memory_is_bounded() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let storage = open(&directory.path().join("objects"), None).await;
    for key in ["a/item", "a.bin", "a0.bin", "z/item"] {
        storage
            .put(key, b"x", "application/octet-stream")
            .await
            .expect("listing fixture");
    }
    let mut token = None;
    let mut keys = Vec::new();
    loop {
        let page = storage.list_page(token, 1).await.expect("listing page");
        keys.extend(page.objects.into_iter().map(|(key, _)| key));
        if !page.is_truncated {
            break;
        }
        token = page.next_continuation_token;
    }
    assert_eq!(keys, ["a.bin", "a/item", "a0.bin", "z/item"]);
}

#[tokio::test]
async fn concurrent_replacements_publish_only_complete_objects() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let directory = tempfile::tempdir().expect("temporary directory");
    let storage = open(&directory.path().join("objects"), Some(64 * 1024)).await;
    let left = vec![0x41; 16 * 1024];
    let right = vec![0x42; 16 * 1024];
    let stop = Arc::new(AtomicBool::new(false));
    let reader_store = storage.clone();
    let reader_stop = Arc::clone(&stop);
    let reader_left = left.clone();
    let reader_right = right.clone();
    let reader = tokio::spawn(async move {
        while !reader_stop.load(Ordering::Acquire) {
            if let Ok(bytes) = reader_store.get("race.bin").await {
                assert!(bytes == reader_left || bytes == reader_right);
            }
            tokio::task::yield_now().await;
        }
    });
    let left_write = storage.put("race.bin", &left, "application/octet-stream");
    let right_write = storage.put("race.bin", &right, "application/octet-stream");
    let (left_result, right_result) = tokio::join!(left_write, right_write);
    left_result.expect("left replacement");
    right_result.expect("right replacement");
    stop.store(true, Ordering::Release);
    reader.await.expect("reader task");
    let final_bytes = storage.get("race.bin").await.expect("final object");
    assert!(final_bytes == left || final_bytes == right);
    assert_eq!(storage.usage_bytes(), final_bytes.len() as u64);
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_root_is_rejected() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("temporary directory");
    let real = directory.path().join("real");
    std::fs::create_dir(&real).expect("real directory");
    let linked = directory.path().join("linked");
    symlink(&real, &linked).expect("root symlink");
    assert!(matches!(
        FilesystemBlobStorage::open(FilesystemBlobConfig {
            root: linked,
            quota_bytes: None,
        })
        .await,
        Err(MediaError::StorageError(_))
    ));
}
