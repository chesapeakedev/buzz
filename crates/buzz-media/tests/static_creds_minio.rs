//! Live round-trip test for the **static-credentials** S3 path against a local
//! MinIO, guarded by `#[ignore]`.
//!
//! This is the path local/dev and any static-key deployment uses
//! (`s3_access_key`/`s3_secret_key` both non-empty -> `Credentials::new`). It
//! exists to prove that adding the IRSA/credential-chain fallback did **not**
//! regress hardcoded credentials.
//!
//! Run it against the docker-compose MinIO (creds `buzz_dev`/`buzz_dev_secret`,
//! bucket `buzz-media`, endpoint `http://localhost:9000`):
//!
//! ```bash
//! docker compose up -d minio minio-init
//! cargo test -p buzz-media --test static_creds_minio -- --ignored
//! ```
//!
//! Overridable via `BUZZ_S3_ENDPOINT` / `BUZZ_S3_ACCESS_KEY` /
//! `BUZZ_S3_SECRET_KEY` / `BUZZ_S3_BUCKET`.

use buzz_media::config::MediaConfig;
use buzz_media::storage::MediaStorage;

mod support;

fn minio_config() -> MediaConfig {
    MediaConfig {
        s3_endpoint: std::env::var("BUZZ_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:9000".to_string()),
        s3_access_key: std::env::var("BUZZ_S3_ACCESS_KEY")
            .unwrap_or_else(|_| "buzz_dev".to_string()),
        s3_secret_key: std::env::var("BUZZ_S3_SECRET_KEY")
            .unwrap_or_else(|_| "buzz_dev_secret".to_string()),
        s3_bucket: std::env::var("BUZZ_S3_BUCKET").unwrap_or_else(|_| "buzz-media".to_string()),
        s3_region: "us-east-1".to_string(),
        max_image_bytes: 50 * 1024 * 1024,
        max_gif_bytes: 10 * 1024 * 1024,
        max_video_bytes: 524_288_000,
        max_file_bytes: 104_857_600,
        public_base_url: "http://localhost:3000/media".to_string(),
        upload_records_enabled: false,
        upload_ip_header: None,
        upload_port_header: None,
    }
}

#[tokio::test]
#[ignore = "requires a live MinIO (docker compose up -d minio minio-init)"]
async fn static_creds_round_trip_against_minio() {
    let storage =
        MediaStorage::new(&minio_config()).expect("static creds should build a storage client");
    support::run_blob_storage_contract(&storage).await;
    support::run_blob_metadata_contract(&storage).await;
}
