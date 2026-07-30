use buzz_core::{CommunityId, TenantContext};
use buzz_media::storage::{ctx_sidecar_key, BlobMeta, BlobStorage};
use futures_util::StreamExt as _;
use sha2::Digest as _;
use uuid::Uuid;

pub async fn run_blob_storage_contract(storage: &dyn BlobStorage) {
    let run = Uuid::new_v4();
    let key = format!("_test/blob-contract-{run}.bin");
    let file_key = format!("_test/blob-contract-file-{run}.bin");
    let body = b"blob-storage-contract";

    storage
        .put(&key, body, "application/octet-stream")
        .await
        .expect("put should succeed");
    assert!(storage.head(&key).await.expect("head should succeed"));
    let meta = storage
        .head_with_metadata(&key)
        .await
        .expect("head_with_metadata should succeed")
        .expect("object should exist");
    assert_eq!(meta.size, body.len() as u64);
    assert_eq!(storage.get(&key).await.expect("get should succeed"), body);
    assert_eq!(
        storage
            .get_range(&key, 2, 8)
            .await
            .expect("range get should succeed"),
        &body[2..=8]
    );
    let streamed = storage
        .get_stream(&key)
        .await
        .expect("stream get should succeed")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("stream chunks")
        .concat();
    assert_eq!(streamed, body);

    let directory = tempfile::tempdir().expect("temporary directory");
    let file_path = directory.path().join("upload.bin");
    tokio::fs::write(&file_path, b"streamed-file")
        .await
        .expect("write upload fixture");
    storage
        .put_file(&file_key, &file_path, "application/octet-stream")
        .await
        .expect("file put should succeed");
    assert_eq!(
        storage.get(&file_key).await.expect("file get"),
        b"streamed-file"
    );

    let tenant_a = TenantContext::resolved(
        CommunityId::from_uuid(Uuid::new_v4()),
        "media-a.example.test",
    );
    let tenant_b = TenantContext::resolved(
        CommunityId::from_uuid(Uuid::new_v4()),
        "media-b.example.test",
    );
    let sha = hex::encode(sha2::Sha256::digest(body));
    let sidecar = BlobMeta {
        ext: "bin".to_string(),
        mime_type: "application/octet-stream".to_string(),
        size: body.len() as u64,
        ..BlobMeta::default()
    };
    storage
        .put_sidecar(&tenant_a, &sha, &sidecar)
        .await
        .expect("tenant A sidecar put");
    assert_eq!(
        storage.read_sidecar_mime(&tenant_a, &sha).await.as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(storage.read_sidecar_mime(&tenant_b, &sha).await, None);

    storage.delete(&key).await.expect("delete should succeed");
    storage
        .delete(&file_key)
        .await
        .expect("file delete should succeed");
    storage
        .delete(&ctx_sidecar_key(&tenant_a, &sha))
        .await
        .expect("sidecar delete should succeed");
    assert!(!storage.head(&key).await.expect("head after delete"));
}
