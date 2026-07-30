//! Durable filesystem media blob storage for single-node deployments.

use std::io::{ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use buzz_core::{CommunityId, TenantContext};
use futures_util::StreamExt as _;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio::sync::Mutex;
use tokio_util::io::ReaderStream;

use crate::bucket_index::Page;
use crate::error::MediaError;
use crate::storage::{
    ctx_sidecar_key, BlobHeadMeta, BlobMeta, BlobMetadata, BlobStorage, ByteStream,
};

const TEMP_PREFIX: &str = ".buzz-tmp-";
const MAX_LIST_PAGE: usize = 10_000;

/// Configuration for a filesystem media blob store.
#[derive(Debug, Clone)]
pub struct FilesystemBlobConfig {
    /// Root beneath which object keys are materialized.
    pub root: PathBuf,
    /// Optional maximum physical bytes across all stored objects.
    pub quota_bytes: Option<u64>,
}

/// Durable media blob storage rooted in one local directory.
///
/// All mutations share one writer gate. Embedded mode is single-process, so
/// this gate makes quota checks, atomic replacement, cleanup, and accounting
/// one coherent boundary.
#[derive(Clone)]
pub struct FilesystemBlobStorage {
    root: Arc<PathBuf>,
    writer: Arc<Mutex<()>>,
    usage_bytes: Arc<AtomicU64>,
    quota_bytes: Option<u64>,
    metadata: Option<Arc<dyn BlobMetadata>>,
}

impl FilesystemBlobStorage {
    /// Open or create a filesystem store and recover abandoned temporary files.
    pub async fn open(config: FilesystemBlobConfig) -> Result<Self, MediaError> {
        Self::open_with_metadata(config, None).await
    }

    /// Open a filesystem store using an external metadata publication gate.
    ///
    /// Embedded mode supplies the SQLite `media_objects` adapter here. The
    /// legacy constructor remains useful for storage-only tests and callers
    /// that intentionally use filesystem sidecars.
    pub async fn open_with_metadata(
        config: FilesystemBlobConfig,
        metadata: Option<Arc<dyn BlobMetadata>>,
    ) -> Result<Self, MediaError> {
        if matches!(config.quota_bytes, Some(0)) {
            return Err(storage_error("filesystem quota must be positive"));
        }
        reject_root_symlink(&config.root).await?;
        tokio::fs::create_dir_all(&config.root)
            .await
            .map_err(|_| storage_error("cannot create filesystem object root"))?;
        set_private_directory(&config.root).await?;
        reject_root_symlink(&config.root).await?;
        let root = tokio::fs::canonicalize(&config.root)
            .await
            .map_err(|_| storage_error("cannot resolve filesystem object root"))?;
        let (usage, removed) = scan_tree(root.clone(), true).await?;
        if let Some(quota) = config.quota_bytes {
            if usage > quota {
                return Err(storage_error("filesystem object usage exceeds quota"));
            }
        }
        if removed > 0 {
            tracing::info!(
                removed,
                "removed abandoned filesystem object temporary files"
            );
        }
        Ok(Self {
            root: Arc::new(root),
            writer: Arc::new(Mutex::new(())),
            usage_bytes: Arc::new(AtomicU64::new(usage)),
            quota_bytes: config.quota_bytes,
            metadata,
        })
    }

    /// Current physical object bytes tracked by this process.
    pub fn usage_bytes(&self) -> u64 {
        self.usage_bytes.load(Ordering::Acquire)
    }

    /// Remove abandoned temporary files and refresh physical usage.
    pub async fn recover(&self) -> Result<u64, MediaError> {
        let _writer = self.writer.lock().await;
        let (usage, removed) = scan_tree(self.root.as_ref().clone(), true).await?;
        if let Some(quota) = self.quota_bytes {
            if usage > quota {
                return Err(storage_error("filesystem object usage exceeds quota"));
            }
        }
        self.usage_bytes.store(usage, Ordering::Release);
        Ok(removed)
    }

    async fn write_bytes(&self, key: &str, bytes: &[u8]) -> Result<(), MediaError> {
        let components = validate_key(key)?;
        let _writer = self.writer.lock().await;
        let target = self.prepare_target(&components).await?;
        let existing = regular_file_size_if_present(&target).await?;
        let replacement = u64::try_from(bytes.len())
            .map_err(|_| storage_error("filesystem object is too large"))?;
        self.check_quota(existing, replacement)?;
        let temporary = temporary_path(
            target
                .parent()
                .ok_or_else(|| storage_error("object key has no parent"))?,
        );
        let result = async {
            let mut file = create_private_file(&temporary).await?;
            file.write_all(bytes)
                .await
                .map_err(|_| storage_error("cannot write temporary object"))?;
            file.sync_all()
                .await
                .map_err(|_| storage_error("cannot flush temporary object"))?;
            drop(file);
            tokio::fs::rename(&temporary, &target)
                .await
                .map_err(|_| storage_error("cannot publish filesystem object"))?;
            self.update_usage(existing, replacement);
            sync_parent(&target).await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
            return result;
        }
        Ok(())
    }

    async fn write_file(&self, key: &str, source: &Path) -> Result<(), MediaError> {
        let components = validate_key(key)?;
        let _writer = self.writer.lock().await;
        let target = self.prepare_target(&components).await?;
        let existing = regular_file_size_if_present(&target).await?;
        let temporary = temporary_path(
            target
                .parent()
                .ok_or_else(|| storage_error("object key has no parent"))?,
        );
        let result = async {
            let mut input = tokio::fs::File::open(source)
                .await
                .map_err(|_| storage_error("cannot open upload source"))?;
            let mut output = create_private_file(&temporary).await?;
            let copied = tokio::io::copy(&mut input, &mut output)
                .await
                .map_err(|_| storage_error("cannot stream temporary object"))?;
            self.check_quota(existing, copied)?;
            output
                .sync_all()
                .await
                .map_err(|_| storage_error("cannot flush temporary object"))?;
            drop(output);
            tokio::fs::rename(&temporary, &target)
                .await
                .map_err(|_| storage_error("cannot publish filesystem object"))?;
            self.update_usage(existing, copied);
            sync_parent(&target).await?;
            Ok(copied)
        }
        .await;
        match result {
            Ok(_) => {}
            Err(error) => {
                let _ = tokio::fs::remove_file(&temporary).await;
                return Err(error);
            }
        }
        Ok(())
    }

    async fn prepare_target(&self, components: &[&str]) -> Result<PathBuf, MediaError> {
        let (file_name, directories) = components
            .split_last()
            .ok_or_else(|| storage_error("empty object key"))?;
        let mut parent = self.root.as_ref().clone();
        for component in directories {
            parent.push(component);
            match tokio::fs::symlink_metadata(&parent).await {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                    return Err(storage_error("object parent is not a safe directory"));
                }
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    tokio::fs::create_dir(&parent)
                        .await
                        .map_err(|_| storage_error("cannot create object directory"))?;
                    set_private_directory(&parent).await?;
                    let metadata = tokio::fs::symlink_metadata(&parent)
                        .await
                        .map_err(|_| storage_error("cannot verify object directory"))?;
                    if metadata.file_type().is_symlink() || !metadata.is_dir() {
                        return Err(storage_error("object parent is not a safe directory"));
                    }
                    sync_directory(
                        parent
                            .parent()
                            .ok_or_else(|| storage_error("object directory has no parent"))?
                            .to_path_buf(),
                    )
                    .await?;
                }
                Err(_) => return Err(storage_error("cannot inspect object directory")),
            }
        }
        Ok(parent.join(file_name))
    }

    async fn read_path(&self, key: &str) -> Result<PathBuf, MediaError> {
        let components = validate_key(key)?;
        let mut path = self.root.as_ref().clone();
        for (index, component) in components.iter().enumerate() {
            path.push(component);
            let metadata = tokio::fs::symlink_metadata(&path).await.map_err(|error| {
                if error.kind() == ErrorKind::NotFound {
                    MediaError::NotFound
                } else {
                    storage_error("cannot inspect filesystem object")
                }
            })?;
            if metadata.file_type().is_symlink() {
                return Err(storage_error("filesystem object path contains a symlink"));
            }
            let is_last = index + 1 == components.len();
            if (!is_last && !metadata.is_dir()) || (is_last && !metadata.is_file()) {
                return Err(MediaError::NotFound);
            }
        }
        Ok(path)
    }

    fn check_quota(&self, existing: u64, replacement: u64) -> Result<(), MediaError> {
        let current = self.usage_bytes();
        let next = current
            .checked_sub(existing)
            .and_then(|bytes| bytes.checked_add(replacement))
            .ok_or_else(|| storage_error("filesystem object usage overflow"))?;
        if self.quota_bytes.is_some_and(|quota| next > quota) {
            return Err(storage_error("filesystem object quota exceeded"));
        }
        Ok(())
    }

    fn update_usage(&self, existing: u64, replacement: u64) {
        let current = self.usage_bytes();
        let next = current.saturating_sub(existing).saturating_add(replacement);
        self.usage_bytes.store(next, Ordering::Release);
    }
}

#[async_trait::async_trait]
impl BlobStorage for FilesystemBlobStorage {
    async fn put(&self, key: &str, bytes: &[u8], _content_type: &str) -> Result<(), MediaError> {
        self.write_bytes(key, bytes).await
    }

    async fn put_file(
        &self,
        key: &str,
        path: &Path,
        _content_type: &str,
    ) -> Result<(), MediaError> {
        self.write_file(key, path).await
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, MediaError> {
        let path = self.read_path(key).await?;
        tokio::fs::read(path)
            .await
            .map_err(|_| storage_error("cannot read filesystem object"))
    }

    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, MediaError> {
        if start > end {
            return Err(storage_error("invalid filesystem object range"));
        }
        let path = self.read_path(key).await?;
        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(|_| storage_error("cannot open filesystem object"))?;
        let size = file
            .metadata()
            .await
            .map_err(|_| storage_error("cannot stat filesystem object"))?
            .len();
        if end >= size {
            return Err(storage_error("filesystem object range exceeds size"));
        }
        let length = end
            .checked_sub(start)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| storage_error("filesystem object range overflow"))?;
        file.seek(SeekFrom::Start(start))
            .await
            .map_err(|_| storage_error("cannot seek filesystem object"))?;
        let capacity = usize::try_from(length)
            .map_err(|_| storage_error("filesystem object range is too large"))?;
        let mut bytes = Vec::with_capacity(capacity);
        file.take(length)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| storage_error("cannot read filesystem object range"))?;
        Ok(bytes)
    }

    async fn get_stream(&self, key: &str) -> Result<ByteStream, MediaError> {
        let path = self.read_path(key).await?;
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|_| storage_error("cannot open filesystem object"))?;
        let stream = ReaderStream::new(file)
            .map(|chunk| chunk.map_err(|_| storage_error("cannot stream filesystem object")));
        Ok(Box::pin(stream))
    }

    async fn head(&self, key: &str) -> Result<bool, MediaError> {
        if let Some((ctx, sha256)) = metadata_key(key) {
            let Some(metadata) = &self.metadata else {
                return Ok(false);
            };
            return Ok(metadata.get_metadata(&ctx, &sha256).await?.is_some());
        }
        match self.read_path(key).await {
            Ok(_) => Ok(true),
            Err(MediaError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), MediaError> {
        validate_key(key)?;
        let _writer = self.writer.lock().await;
        let path = match self.read_path(key).await {
            Ok(path) => path,
            Err(MediaError::NotFound) => return Ok(()),
            Err(error) => return Err(error),
        };
        let size = regular_file_size_if_present(&path).await?;
        tokio::fs::remove_file(&path)
            .await
            .map_err(|_| storage_error("cannot delete filesystem object"))?;
        let current = self.usage_bytes();
        self.usage_bytes
            .store(current.saturating_sub(size), Ordering::Release);
        sync_parent(&path).await?;
        Ok(())
    }

    async fn head_with_metadata(&self, key: &str) -> Result<Option<BlobHeadMeta>, MediaError> {
        let path = match self.read_path(key).await {
            Ok(path) => path,
            Err(MediaError::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        let size = tokio::fs::metadata(path)
            .await
            .map_err(|_| storage_error("cannot stat filesystem object"))?
            .len();
        Ok(Some(BlobHeadMeta { size }))
    }

    async fn get_sidecar(&self, ctx: &TenantContext, sha256: &str) -> Result<BlobMeta, MediaError> {
        if let Some(metadata) = &self.metadata {
            return metadata
                .get_metadata(ctx, sha256)
                .await?
                .ok_or(MediaError::NotFound);
        }
        let key = ctx_sidecar_key(ctx, sha256);
        serde_json::from_slice(&self.get(&key).await?)
            .map_err(|_| storage_error("invalid filesystem sidecar"))
    }

    async fn put_sidecar(
        &self,
        ctx: &TenantContext,
        sha256: &str,
        meta: &BlobMeta,
    ) -> Result<(), MediaError> {
        if let Some(metadata) = &self.metadata {
            return metadata.put_metadata(ctx, sha256, meta).await;
        }
        let key = ctx_sidecar_key(ctx, sha256);
        let bytes = serde_json::to_vec(meta).map_err(|_| storage_error("cannot encode sidecar"))?;
        self.put(&key, &bytes, "application/json").await
    }

    async fn read_sidecar_mime(&self, ctx: &TenantContext, sha256_ext: &str) -> Option<String> {
        if let Some(metadata) = &self.metadata {
            return metadata.read_mime(ctx, sha256_ext).await;
        }
        let sha256 = sha256_ext.split('.').next().unwrap_or(sha256_ext);
        self.get_sidecar(ctx, sha256)
            .await
            .ok()
            .map(|meta| meta.mime_type)
    }

    async fn list_page(
        &self,
        continuation_token: Option<String>,
        max_keys: usize,
    ) -> Result<Page, MediaError> {
        if !(1..=MAX_LIST_PAGE).contains(&max_keys) {
            return Err(storage_error(
                "filesystem listing page must be positive and bounded",
            ));
        }
        scan_objects_page(self.root.as_ref().clone(), continuation_token, max_keys).await
    }
}

fn metadata_key(key: &str) -> Option<(TenantContext, String)> {
    let mut components = key.split('/');
    if components.next()? != "_meta" {
        return None;
    }
    let community = uuid::Uuid::parse_str(components.next()?).ok()?;
    let digest = components.next()?.strip_suffix(".json")?;
    if components.next().is_some() || digest.is_empty() {
        return None;
    }
    Some((
        TenantContext::resolved(CommunityId::from_uuid(community), "filesystem"),
        digest.to_string(),
    ))
}

/// Embedded filesystem adapter for the backend-neutral metadata interface.
///
/// Blob bytes remain on the filesystem; metadata operations are delegated to
/// the configured SQLite publication gate.
#[async_trait::async_trait]
impl BlobMetadata for FilesystemBlobStorage {
    async fn get_metadata(
        &self,
        ctx: &TenantContext,
        sha256: &str,
    ) -> Result<Option<BlobMeta>, MediaError> {
        match &self.metadata {
            Some(metadata) => metadata.get_metadata(ctx, sha256).await,
            None => match self.get_sidecar(ctx, sha256).await {
                Ok(meta) => Ok(Some(meta)),
                Err(MediaError::NotFound) => Ok(None),
                Err(error) => Err(error),
            },
        }
    }

    async fn put_metadata(
        &self,
        ctx: &TenantContext,
        sha256: &str,
        meta: &BlobMeta,
    ) -> Result<(), MediaError> {
        self.put_sidecar(ctx, sha256, meta).await
    }

    async fn read_mime(&self, ctx: &TenantContext, sha256_ext: &str) -> Option<String> {
        self.read_sidecar_mime(ctx, sha256_ext).await
    }

    async fn delete_metadata(&self, ctx: &TenantContext, sha256: &str) -> Result<(), MediaError> {
        if let Some(metadata) = &self.metadata {
            return metadata.delete_metadata(ctx, sha256).await;
        }
        self.delete(&ctx_sidecar_key(ctx, sha256)).await
    }
}

fn validate_key(key: &str) -> Result<Vec<&str>, MediaError> {
    if key.is_empty() || key.starts_with('/') || key.contains('\\') || key.contains('\0') {
        return Err(storage_error("invalid filesystem object key"));
    }
    let components: Vec<_> = key.split('/').collect();
    if components.iter().any(|component| {
        component.is_empty()
            || *component == "."
            || *component == ".."
            || component.starts_with(TEMP_PREFIX)
    }) {
        return Err(storage_error("invalid filesystem object key"));
    }
    Ok(components)
}

fn temporary_path(parent: &Path) -> PathBuf {
    parent.join(format!("{TEMP_PREFIX}{}", uuid::Uuid::new_v4()))
}

fn storage_error(message: &'static str) -> MediaError {
    MediaError::StorageError(message.to_string())
}

async fn reject_root_symlink(root: &Path) -> Result<(), MediaError> {
    match tokio::fs::symlink_metadata(root).await {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(
            storage_error("filesystem object root is not a safe directory"),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(_) => Err(storage_error("cannot inspect filesystem object root")),
    }
}

async fn create_private_file(path: &Path) -> Result<tokio::fs::File, MediaError> {
    let mut options = tokio::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    options
        .open(path)
        .await
        .map_err(|_| storage_error("cannot create temporary object"))
}

#[cfg(unix)]
async fn set_private_directory(path: &Path) -> Result<(), MediaError> {
    use std::os::unix::fs::PermissionsExt as _;

    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .await
        .map_err(|_| storage_error("cannot protect filesystem object directory"))
}

#[cfg(not(unix))]
async fn set_private_directory(_path: &Path) -> Result<(), MediaError> {
    Ok(())
}

async fn regular_file_size_if_present(path: &Path) -> Result<u64, MediaError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => Err(
            storage_error("filesystem object target is not a regular file"),
        ),
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
        Err(_) => Err(storage_error("cannot inspect filesystem object target")),
    }
}

async fn sync_parent(path: &Path) -> Result<(), MediaError> {
    let parent = path
        .parent()
        .ok_or_else(|| storage_error("filesystem object has no parent"))?
        .to_path_buf();
    sync_directory(parent).await
}

#[cfg(unix)]
async fn sync_directory(path: PathBuf) -> Result<(), MediaError> {
    tokio::task::spawn_blocking(move || {
        std::fs::File::open(path)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| storage_error("cannot flush filesystem object directory"))
    })
    .await
    .map_err(|_| storage_error("filesystem directory flush task failed"))?
}

#[cfg(not(unix))]
async fn sync_directory(_path: PathBuf) -> Result<(), MediaError> {
    Ok(())
}

async fn scan_tree(root: PathBuf, remove_temporaries: bool) -> Result<(u64, u64), MediaError> {
    tokio::task::spawn_blocking(move || scan_tree_sync(&root, remove_temporaries))
        .await
        .map_err(|_| storage_error("filesystem object scan task failed"))?
}

async fn scan_objects_page(
    root: PathBuf,
    continuation_token: Option<String>,
    max_keys: usize,
) -> Result<Page, MediaError> {
    tokio::task::spawn_blocking(move || {
        scan_objects_page_sync(&root, continuation_token.as_deref(), max_keys)
    })
    .await
    .map_err(|_| storage_error("filesystem object scan task failed"))?
}

fn scan_tree_sync(root: &Path, remove_temporaries: bool) -> Result<(u64, u64), MediaError> {
    let mut stack = vec![root.to_path_buf()];
    let mut recovered_directories = std::collections::BTreeSet::new();
    let mut usage = 0u64;
    let mut removed = 0u64;
    while let Some(directory) = stack.pop() {
        for entry in std::fs::read_dir(directory)
            .map_err(|_| storage_error("cannot scan filesystem object directory"))?
        {
            let entry = entry.map_err(|_| storage_error("cannot scan filesystem object entry"))?;
            let file_type = entry
                .file_type()
                .map_err(|_| storage_error("cannot inspect filesystem object entry"))?;
            if file_type.is_symlink() {
                return Err(storage_error("filesystem object tree contains a symlink"));
            }
            if file_type.is_dir() {
                stack.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                return Err(storage_error(
                    "filesystem object tree contains a special file",
                ));
            }
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| storage_error("filesystem object name is not UTF-8"))?;
            if name.starts_with(TEMP_PREFIX) {
                if remove_temporaries {
                    std::fs::remove_file(entry.path())
                        .map_err(|_| storage_error("cannot remove abandoned temporary object"))?;
                    recovered_directories.insert(
                        entry
                            .path()
                            .parent()
                            .ok_or_else(|| storage_error("temporary object has no parent"))?
                            .to_path_buf(),
                    );
                    removed = removed.saturating_add(1);
                }
                continue;
            }
            usage = usage
                .checked_add(
                    entry
                        .metadata()
                        .map_err(|_| storage_error("cannot stat filesystem object entry"))?
                        .len(),
                )
                .ok_or_else(|| storage_error("filesystem object usage overflow"))?;
        }
    }
    for directory in recovered_directories {
        std::fs::File::open(directory)
            .and_then(|handle| handle.sync_all())
            .map_err(|_| storage_error("cannot flush filesystem object root"))?;
    }
    Ok((usage, removed))
}

fn scan_objects_page_sync(
    root: &Path,
    continuation_token: Option<&str>,
    max_keys: usize,
) -> Result<Page, MediaError> {
    let mut stack = vec![root.to_path_buf()];
    let capacity = max_keys
        .checked_add(1)
        .ok_or_else(|| storage_error("filesystem listing page is too large"))?;
    let mut objects = std::collections::BinaryHeap::with_capacity(capacity);
    while let Some(directory) = stack.pop() {
        for entry in std::fs::read_dir(directory)
            .map_err(|_| storage_error("cannot scan filesystem object directory"))?
        {
            let entry = entry.map_err(|_| storage_error("cannot scan filesystem object entry"))?;
            let file_type = entry
                .file_type()
                .map_err(|_| storage_error("cannot inspect filesystem object entry"))?;
            if file_type.is_symlink() {
                return Err(storage_error("filesystem object tree contains a symlink"));
            }
            if file_type.is_dir() {
                stack.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                return Err(storage_error(
                    "filesystem object tree contains a special file",
                ));
            }
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| storage_error("filesystem object name is not UTF-8"))?;
            if name.starts_with(TEMP_PREFIX) {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(|_| storage_error("filesystem object escaped its root"))?
                .components()
                .map(|component| {
                    component
                        .as_os_str()
                        .to_str()
                        .ok_or_else(|| storage_error("filesystem object name is not UTF-8"))
                })
                .collect::<Result<Vec<_>, _>>()?
                .join("/");
            let size = entry
                .metadata()
                .map_err(|_| storage_error("cannot stat filesystem object entry"))?
                .len();
            if continuation_token.is_some_and(|token| relative.as_str() <= token) {
                continue;
            }
            if objects.len() < capacity {
                objects.push((relative, size));
            } else if objects
                .peek()
                .is_some_and(|(largest, _)| relative < *largest)
            {
                objects.pop();
                objects.push((relative, size));
            }
        }
    }
    let mut objects = objects.into_sorted_vec();
    let is_truncated = objects.len() > max_keys;
    objects.truncate(max_keys);
    let next_continuation_token = if is_truncated {
        objects.last().map(|(key, _)| key.clone())
    } else {
        None
    };
    Ok(Page {
        objects,
        next_continuation_token,
        is_truncated,
    })
}
