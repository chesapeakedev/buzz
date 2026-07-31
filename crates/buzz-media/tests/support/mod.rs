use buzz_core::{CommunityId, TenantContext};
use buzz_media::storage::{ctx_sidecar_key, BlobMeta, BlobMetadata, BlobStorage};
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

/// Exercise metadata publication and tenant isolation against every adapter.
pub async fn run_blob_metadata_contract(metadata: &dyn BlobMetadata) {
    let tenant_a = TenantContext::resolved(
        CommunityId::from_uuid(Uuid::new_v4()),
        "metadata-a.example.test",
    );
    let tenant_b = TenantContext::resolved(
        CommunityId::from_uuid(Uuid::new_v4()),
        "metadata-b.example.test",
    );
    let sha = hex::encode(sha2::Sha256::digest(b"metadata-contract"));
    let meta = BlobMeta {
        ext: "bin".to_string(),
        mime_type: "application/octet-stream".to_string(),
        size: 18,
        uploaded_at: 1_700_000_000,
        ..BlobMeta::default()
    };

    assert!(metadata
        .get_metadata(&tenant_a, &sha)
        .await
        .expect("initial metadata")
        .is_none());
    metadata
        .put_metadata(&tenant_a, &sha, &meta)
        .await
        .expect("metadata put");
    let stored = metadata
        .get_metadata(&tenant_a, &sha)
        .await
        .expect("metadata get")
        .expect("published metadata");
    assert_eq!(stored.ext, meta.ext);
    assert_eq!(stored.mime_type, meta.mime_type);
    assert_eq!(stored.size, meta.size);
    assert_eq!(stored.uploaded_at, meta.uploaded_at);
    assert_eq!(
        metadata
            .read_mime(&tenant_a, &format!("{sha}.bin"))
            .await
            .as_deref(),
        Some("application/octet-stream")
    );
    assert!(metadata
        .get_metadata(&tenant_b, &sha)
        .await
        .expect("cross-tenant metadata")
        .is_none());

    metadata
        .delete_metadata(&tenant_a, &sha)
        .await
        .expect("metadata delete");
    assert!(metadata
        .get_metadata(&tenant_a, &sha)
        .await
        .expect("deleted metadata")
        .is_none());
}

#[test]
fn community_scoped_media_keys_are_wire_compatible() {
    let community = CommunityId::from_uuid(Uuid::from_u128(1));
    let sha = "a".repeat(64);
    assert_eq!(
        ctx_sidecar_key(
            &TenantContext::resolved(community, "media.example.test"),
            &sha,
        ),
        format!("_meta/{community}/{sha}.json")
    );
    assert_ne!(
        ctx_sidecar_key(
            &TenantContext::resolved(community, "media.example.test"),
            &sha,
        ),
        format!("_meta/{sha}.json")
    );
}
