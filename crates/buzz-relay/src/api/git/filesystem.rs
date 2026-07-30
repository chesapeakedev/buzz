//! Durable filesystem Git object and pointer storage.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::Mutex;

use super::store::{
    CasOutcome, ETag, GitStorage, Precond, ProbeConfig, ProbeFailure, ProbeReport, StoreError,
};

const TEMP_PREFIX: &str = ".buzz-tmp-";
const POINTER_MAGIC: &[u8; 8] = b"BUZZPTR1";
const POINTER_HEADER_BYTES: usize = POINTER_MAGIC.len() + 16 + 8;
const MAX_POINTER_BYTES: usize = 1024 * 1024;
const POINTER_LOCK_SHARDS: usize = 64;

/// Configuration for filesystem Git storage.
#[derive(Debug, Clone)]
pub struct FilesystemGitConfig {
    /// Root beneath which Git object keys are materialized.
    pub root: PathBuf,
    /// Optional maximum physical bytes across immutable objects and pointers.
    pub quota_bytes: Option<u64>,
}

/// Single-node Git storage backed by durable local files.
///
/// Immutable objects use create-only hard-link publication. Pointers store an
/// internal version token and body in one atomically replaced envelope so
/// `get_pointer` returns a consistent snapshot and stale tokens cannot win
/// after an ABA body transition.
#[derive(Debug, Clone)]
pub struct FilesystemGitStorage {
    root: Arc<PathBuf>,
    usage_bytes: Arc<AtomicU64>,
    quota_bytes: Option<u64>,
    pointer_locks: Arc<Vec<Mutex<()>>>,
}

impl FilesystemGitStorage {
    /// Open or create a filesystem Git store and recover abandoned writes.
    pub async fn open(config: FilesystemGitConfig) -> Result<Self, StoreError> {
        if matches!(config.quota_bytes, Some(0)) {
            return Err(storage_error("filesystem Git quota must be positive"));
        }
        reject_root_symlink(&config.root).await?;
        tokio::fs::create_dir_all(&config.root)
            .await
            .map_err(|_| storage_error("cannot create filesystem Git root"))?;
        set_private_directory(&config.root).await?;
        reject_root_symlink(&config.root).await?;
        let root = tokio::fs::canonicalize(&config.root)
            .await
            .map_err(|_| storage_error("cannot resolve filesystem Git root"))?;
        for directory in ["packs", "manifests", "idx", "pointers"] {
            let path = root.join(directory);
            tokio::fs::create_dir_all(&path)
                .await
                .map_err(|_| storage_error("cannot create filesystem Git directory"))?;
            set_private_directory(&path).await?;
        }
        let (usage, removed) = scan_tree(root.clone(), true).await?;
        if config.quota_bytes.is_some_and(|quota| usage > quota) {
            return Err(storage_error("filesystem Git usage exceeds quota"));
        }
        if removed > 0 {
            tracing::info!(removed, "removed abandoned filesystem Git temporary files");
        }
        Ok(Self {
            root: Arc::new(root),
            usage_bytes: Arc::new(AtomicU64::new(usage)),
            quota_bytes: config.quota_bytes,
            pointer_locks: Arc::new((0..POINTER_LOCK_SHARDS).map(|_| Mutex::new(())).collect()),
        })
    }

    /// Current committed and reserved physical bytes.
    pub fn usage_bytes(&self) -> u64 {
        self.usage_bytes.load(Ordering::Acquire)
    }

    /// Remove abandoned temporary files and refresh physical usage.
    ///
    /// Call only during startup before the store is shared with request tasks.
    pub async fn recover(&self) -> Result<u64, StoreError> {
        let (usage, removed) = scan_tree(self.root.as_ref().clone(), true).await?;
        if self.quota_bytes.is_some_and(|quota| usage > quota) {
            return Err(storage_error("filesystem Git usage exceeds quota"));
        }
        self.usage_bytes.store(usage, Ordering::Release);
        Ok(removed)
    }

    async fn put_content_addressed(
        &self,
        prefix: &str,
        bytes: &[u8],
    ) -> Result<(String, bool), StoreError> {
        let digest = digest_hex(bytes);
        let key = format!("{prefix}/{digest}");
        let created = self.put_create_only(&key, bytes).await?;
        Ok((key, created))
    }

    async fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<bool, StoreError> {
        let components = validate_key(key)?;
        let target = self.prepare_target(&components).await?;
        let temporary = write_temporary(
            target
                .parent()
                .ok_or_else(|| storage_error("filesystem Git key has no parent"))?,
            bytes,
        )
        .await?;
        let size = u64::try_from(bytes.len())
            .map_err(|_| storage_error("filesystem Git object is too large"))?;
        if let Err(error) = self.reserve(size) {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error);
        }
        let publication = tokio::fs::hard_link(&temporary, &target).await;
        let created = match publication {
            Ok(()) => true,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                self.release(size);
                false
            }
            Err(_) => {
                self.release(size);
                let _ = tokio::fs::remove_file(&temporary).await;
                return Err(storage_error("cannot publish immutable Git object"));
            }
        };
        tokio::fs::remove_file(&temporary)
            .await
            .map_err(|_| storage_error("cannot remove temporary Git object"))?;
        sync_parent(&target).await?;
        Ok(created)
    }

    async fn prepare_target(&self, components: &[&str]) -> Result<PathBuf, StoreError> {
        let (file_name, directories) = components
            .split_last()
            .ok_or_else(|| storage_error("empty filesystem Git key"))?;
        let mut parent = self.root.as_ref().clone();
        for component in directories {
            parent.push(component);
            match tokio::fs::symlink_metadata(&parent).await {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                    return Err(storage_error("filesystem Git parent is unsafe"));
                }
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    tokio::fs::create_dir(&parent)
                        .await
                        .map_err(|_| storage_error("cannot create filesystem Git directory"))?;
                    set_private_directory(&parent).await?;
                    let metadata = tokio::fs::symlink_metadata(&parent)
                        .await
                        .map_err(|_| storage_error("cannot verify filesystem Git directory"))?;
                    if metadata.file_type().is_symlink() || !metadata.is_dir() {
                        return Err(storage_error("filesystem Git parent is unsafe"));
                    }
                    sync_directory(
                        parent
                            .parent()
                            .ok_or_else(|| storage_error("filesystem Git directory has no parent"))?
                            .to_path_buf(),
                    )
                    .await?;
                }
                Err(_) => return Err(storage_error("cannot inspect filesystem Git directory")),
            }
        }
        Ok(parent.join(file_name))
    }

    async fn read_path(&self, key: &str) -> Result<PathBuf, StoreError> {
        let components = validate_key(key)?;
        let mut path = self.root.as_ref().clone();
        for (index, component) in components.iter().enumerate() {
            path.push(component);
            let metadata = tokio::fs::symlink_metadata(&path).await.map_err(|error| {
                if error.kind() == ErrorKind::NotFound {
                    StoreError::NotFound(key.to_string())
                } else {
                    storage_error("cannot inspect filesystem Git object")
                }
            })?;
            if metadata.file_type().is_symlink() {
                return Err(storage_error("filesystem Git path contains a symlink"));
            }
            let is_last = index + 1 == components.len();
            if (!is_last && !metadata.is_dir()) || (is_last && !metadata.is_file()) {
                return Err(StoreError::NotFound(key.to_string()));
            }
        }
        Ok(path)
    }

    async fn read_limited(&self, key: &str, max_bytes: u64) -> Result<Bytes, StoreError> {
        let path = self.read_path(key).await?;
        let size = tokio::fs::metadata(&path)
            .await
            .map_err(|_| storage_error("cannot stat filesystem Git object"))?
            .len();
        if size > max_bytes {
            return Err(StoreError::ObjectTooLarge {
                key: key.to_string(),
                size,
                max: max_bytes,
            });
        }
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|_| storage_error("cannot read filesystem Git object"))?;
        let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if actual > max_bytes {
            return Err(StoreError::ObjectTooLarge {
                key: key.to_string(),
                size: actual,
                max: max_bytes,
            });
        }
        Ok(Bytes::from(bytes))
    }

    async fn read_verified(
        &self,
        key: &str,
        expected_digest: &str,
        max_bytes: u64,
    ) -> Result<Bytes, StoreError> {
        validate_digest(expected_digest)?;
        validate_immutable_key(key)?;
        let bytes = self.read_limited(key, max_bytes).await?;
        let actual = digest_hex(&bytes);
        if actual != expected_digest {
            return Err(StoreError::DigestMismatch {
                key: key.to_string(),
                expected: expected_digest.to_string(),
                actual,
            });
        }
        Ok(bytes)
    }

    fn pointer_lock(&self, key: &str) -> &Mutex<()> {
        let digest = Sha256::digest(key.as_bytes());
        let mut prefix = [0u8; 8];
        prefix.copy_from_slice(&digest[..8]);
        let shard = u64::from_be_bytes(prefix) as usize % self.pointer_locks.len();
        &self.pointer_locks[shard]
    }

    async fn get_pointer_inner(&self, key: &str) -> Result<Option<PointerEnvelope>, StoreError> {
        validate_pointer_key(key)?;
        let path = match self.read_path(key).await {
            Ok(path) => path,
            Err(StoreError::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|_| storage_error("cannot read filesystem Git pointer"))?;
        decode_pointer(&bytes).map(Some)
    }

    async fn put_pointer_inner(
        &self,
        key: &str,
        body: &[u8],
        precondition: Precond,
    ) -> Result<CasOutcome, StoreError> {
        validate_pointer_key(key)?;
        if body.len() > MAX_POINTER_BYTES {
            return Err(storage_error("filesystem Git pointer is too large"));
        }
        let _pointer = self.pointer_lock(key).lock().await;
        let current = self.get_pointer_inner(key).await?;
        match (&precondition, &current) {
            (Precond::IfNoneMatchStar, Some(_)) => return Ok(CasOutcome::LostRace),
            (Precond::IfMatch(_), None) => return Ok(CasOutcome::LostRace),
            (Precond::IfMatch(expected), Some(envelope)) if expected != &envelope.etag => {
                return Ok(CasOutcome::LostRace);
            }
            _ => {}
        }

        let etag = ETag(uuid::Uuid::new_v4().to_string());
        let envelope = encode_pointer(&etag, body)?;
        let components = validate_key(key)?;
        let target = self.prepare_target(&components).await?;
        let temporary = write_temporary(
            target
                .parent()
                .ok_or_else(|| storage_error("filesystem Git pointer has no parent"))?,
            &envelope,
        )
        .await?;
        let new_size = u64::try_from(envelope.len())
            .map_err(|_| storage_error("filesystem Git pointer is too large"))?;
        let old_size = current
            .as_ref()
            .map(|value| value.physical_bytes)
            .unwrap_or_default();
        let reserved = new_size.saturating_sub(old_size);
        if let Err(error) = self.reserve(reserved) {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error);
        }
        if let Err(error) = tokio::fs::rename(&temporary, &target).await {
            self.release(reserved);
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(if error.kind() == ErrorKind::NotFound {
                storage_error("filesystem Git pointer directory vanished")
            } else {
                storage_error("cannot publish filesystem Git pointer")
            });
        }
        self.release(old_size.saturating_sub(new_size));
        sync_parent(&target).await?;
        Ok(CasOutcome::Won(etag))
    }

    fn reserve(&self, bytes: u64) -> Result<(), StoreError> {
        let mut current = self.usage_bytes.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(bytes)
                .ok_or_else(|| storage_error("filesystem Git usage overflow"))?;
            if self.quota_bytes.is_some_and(|quota| next > quota) {
                return Err(storage_error("filesystem Git quota exceeded"));
            }
            match self.usage_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    fn release(&self, bytes: u64) {
        let _ = self
            .usage_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
    }

    async fn remove_probe_key(&self, key: &str) {
        let path = match self.read_path(key).await {
            Ok(path) => path,
            Err(_) => return,
        };
        let size = tokio::fs::metadata(&path)
            .await
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        if tokio::fs::remove_file(&path).await.is_ok() {
            self.release(size);
            let _ = sync_parent(&path).await;
        }
    }

    async fn run_probe(&self, config: ProbeConfig) -> Result<ProbeReport, StoreError> {
        if config.race_width < 2 || config.race_rounds == 0 {
            return Err(ProbeFailure {
                phase: "config",
                round: 0,
                key: String::new(),
                reason: format!(
                    "race_width must be ≥ 2 and race_rounds ≥ 1, got {}/{}",
                    config.race_width, config.race_rounds
                ),
            }
            .into());
        }
        let nonce = uuid::Uuid::new_v4();
        let mut cleanup = Vec::new();
        for round in 0..config.race_rounds {
            let bytes = format!("filesystem-probe-{nonce}-{round}").into_bytes();
            let (key, _) = self.put_content_addressed("packs", &bytes).await?;
            let digest = digest_hex(&bytes);
            let read = self
                .read_verified(&key, &digest, u64::MAX)
                .await
                .map_err(|error| ProbeFailure {
                    phase: "sequential",
                    round,
                    key: key.clone(),
                    reason: error.to_string(),
                })?;
            if read.as_ref() != bytes {
                return Err(ProbeFailure {
                    phase: "sequential",
                    round,
                    key,
                    reason: "read-after-write bytes mismatch".to_string(),
                }
                .into());
            }
            cleanup.push(key);
        }

        for round in 0..config.race_rounds {
            let pointer_key = format!("pointers/probe/{nonce}-{round}");
            let initial = self
                .put_pointer_inner(&pointer_key, b"seed", Precond::IfNoneMatchStar)
                .await?;
            let etag = match initial {
                CasOutcome::Won(etag) => etag,
                CasOutcome::LostRace => {
                    return Err(ProbeFailure {
                        phase: "if_match_race",
                        round,
                        key: pointer_key,
                        reason: "fresh pointer seed lost".to_string(),
                    }
                    .into());
                }
            };
            let outcomes = futures_util::future::join_all((0..config.race_width).map(|racer| {
                let etag = etag.clone();
                let pointer_key = pointer_key.clone();
                async move {
                    self.put_pointer_inner(
                        &pointer_key,
                        format!("racer-{racer}").as_bytes(),
                        Precond::IfMatch(etag),
                    )
                    .await
                }
            }))
            .await;
            let winners = outcomes
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|outcome| matches!(outcome, CasOutcome::Won(_)))
                .count();
            if winners != 1 {
                return Err(ProbeFailure {
                    phase: "if_match_race",
                    round,
                    key: pointer_key,
                    reason: format!("expected exactly one winner, got {winners}"),
                }
                .into());
            }
            cleanup.push(pointer_key);

            let bytes = format!("filesystem-create-race-{nonce}-{round}").into_bytes();
            let outcomes = futures_util::future::join_all(
                (0..config.race_width).map(|_| self.put_content_addressed("packs", &bytes)),
            )
            .await;
            let mut created = 0usize;
            let mut object_key = None;
            for outcome in outcomes {
                let (key, won) = outcome?;
                object_key = Some(key);
                created += usize::from(won);
            }
            if created != 1 {
                return Err(ProbeFailure {
                    phase: "if_none_match_race",
                    round,
                    key: object_key.unwrap_or_default(),
                    reason: format!("expected exactly one creator, got {created}"),
                }
                .into());
            }
            if let Some(key) = object_key {
                cleanup.push(key);
            }
        }

        for key in &cleanup {
            self.remove_probe_key(key).await;
        }
        Ok(ProbeReport {
            race_width: config.race_width,
            race_rounds: config.race_rounds,
            transport_drops: 0,
        })
    }
}

#[async_trait::async_trait]
impl GitStorage for FilesystemGitStorage {
    async fn put_pack(&self, bytes: &[u8]) -> Result<String, StoreError> {
        self.put_content_addressed("packs", bytes)
            .await
            .map(|(key, _)| key)
    }

    async fn put_idx(&self, pack_digest: &str, idx_bytes: &[u8]) -> Result<String, StoreError> {
        validate_digest(pack_digest)?;
        let key = format!("idx/{pack_digest}");
        self.put_create_only(&key, idx_bytes).await?;
        Ok(key)
    }

    async fn get_idx(
        &self,
        pack_digest: &str,
        max_bytes: u64,
    ) -> Result<Option<Bytes>, StoreError> {
        validate_digest(pack_digest)?;
        let key = format!("idx/{pack_digest}");
        match self.read_limited(&key, max_bytes).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(StoreError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn put_manifest(&self, bytes: &[u8]) -> Result<String, StoreError> {
        self.put_content_addressed("manifests", bytes)
            .await
            .map(|(key, _)| key)
    }

    async fn get_verified(&self, key: &str, expected_digest: &str) -> Result<Bytes, StoreError> {
        self.read_verified(key, expected_digest, u64::MAX).await
    }

    async fn get_verified_limited(
        &self,
        key: &str,
        expected_digest: &str,
        max_bytes: u64,
    ) -> Result<Bytes, StoreError> {
        self.read_verified(key, expected_digest, max_bytes).await
    }

    async fn get_pointer(&self, key: &str) -> Result<Option<(ETag, Bytes)>, StoreError> {
        let _pointer = self.pointer_lock(key).lock().await;
        Ok(self
            .get_pointer_inner(key)
            .await?
            .map(|envelope| (envelope.etag, envelope.body)))
    }

    async fn put_pointer(
        &self,
        key: &str,
        body: &[u8],
        precond: Precond,
    ) -> Result<CasOutcome, StoreError> {
        self.put_pointer_inner(key, body, precond).await
    }

    async fn run_conformance_probe(&self, config: ProbeConfig) -> Result<ProbeReport, StoreError> {
        self.run_probe(config).await
    }
}

#[derive(Debug)]
struct PointerEnvelope {
    etag: ETag,
    body: Bytes,
    physical_bytes: u64,
}

fn encode_pointer(etag: &ETag, body: &[u8]) -> Result<Vec<u8>, StoreError> {
    let uuid = uuid::Uuid::parse_str(&etag.0)
        .map_err(|_| storage_error("invalid filesystem Git pointer token"))?;
    let length = u64::try_from(body.len())
        .map_err(|_| storage_error("filesystem Git pointer is too large"))?;
    let mut encoded = Vec::with_capacity(POINTER_HEADER_BYTES.saturating_add(body.len()));
    encoded.extend_from_slice(POINTER_MAGIC);
    encoded.extend_from_slice(uuid.as_bytes());
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(body);
    Ok(encoded)
}

fn decode_pointer(encoded: &[u8]) -> Result<PointerEnvelope, StoreError> {
    if encoded.len() < POINTER_HEADER_BYTES || &encoded[..POINTER_MAGIC.len()] != POINTER_MAGIC {
        return Err(storage_error("invalid filesystem Git pointer envelope"));
    }
    let uuid = uuid::Uuid::from_slice(&encoded[8..24])
        .map_err(|_| storage_error("invalid filesystem Git pointer token"))?;
    let mut length_bytes = [0u8; 8];
    length_bytes.copy_from_slice(&encoded[24..32]);
    let body_length = usize::try_from(u64::from_be_bytes(length_bytes))
        .map_err(|_| storage_error("filesystem Git pointer is too large"))?;
    let expected = POINTER_HEADER_BYTES
        .checked_add(body_length)
        .ok_or_else(|| storage_error("filesystem Git pointer length overflow"))?;
    if expected != encoded.len() || body_length > MAX_POINTER_BYTES {
        return Err(storage_error("invalid filesystem Git pointer length"));
    }
    let physical_bytes = u64::try_from(encoded.len())
        .map_err(|_| storage_error("filesystem Git pointer is too large"))?;
    Ok(PointerEnvelope {
        etag: ETag(uuid.to_string()),
        body: Bytes::copy_from_slice(&encoded[POINTER_HEADER_BYTES..]),
        physical_bytes,
    })
}

fn validate_key(key: &str) -> Result<Vec<&str>, StoreError> {
    if key.is_empty() || key.starts_with('/') || key.contains('\\') || key.contains('\0') {
        return Err(storage_error("invalid filesystem Git key"));
    }
    let components: Vec<_> = key.split('/').collect();
    if components.iter().any(|component| {
        component.is_empty()
            || *component == "."
            || *component == ".."
            || component.starts_with(TEMP_PREFIX)
    }) {
        return Err(storage_error("invalid filesystem Git key"));
    }
    Ok(components)
}

fn validate_digest(digest: &str) -> Result<(), StoreError> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(storage_error("invalid filesystem Git digest"));
    }
    Ok(())
}

fn validate_immutable_key(key: &str) -> Result<(), StoreError> {
    let components = validate_key(key)?;
    if components.len() != 2
        || !matches!(components[0], "packs" | "manifests")
        || validate_digest(components[1]).is_err()
    {
        return Err(storage_error("invalid immutable filesystem Git key"));
    }
    Ok(())
}

fn validate_pointer_key(key: &str) -> Result<(), StoreError> {
    let components = validate_key(key)?;
    if components.len() < 2 || components[0] != "pointers" {
        return Err(storage_error("invalid filesystem Git pointer key"));
    }
    Ok(())
}

fn digest_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn storage_error(message: &'static str) -> StoreError {
    StoreError::Storage(message.to_string())
}

async fn write_temporary(parent: &Path, bytes: &[u8]) -> Result<PathBuf, StoreError> {
    let path = parent.join(format!("{TEMP_PREFIX}{}", uuid::Uuid::new_v4()));
    let mut options = tokio::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .await
        .map_err(|_| storage_error("cannot create temporary Git object"))?;
    file.write_all(bytes)
        .await
        .map_err(|_| storage_error("cannot write temporary Git object"))?;
    file.sync_all()
        .await
        .map_err(|_| storage_error("cannot flush temporary Git object"))?;
    drop(file);
    Ok(path)
}

async fn reject_root_symlink(root: &Path) -> Result<(), StoreError> {
    match tokio::fs::symlink_metadata(root).await {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(storage_error("filesystem Git root is unsafe"))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(_) => Err(storage_error("cannot inspect filesystem Git root")),
    }
}

#[cfg(unix)]
async fn set_private_directory(path: &Path) -> Result<(), StoreError> {
    use std::os::unix::fs::PermissionsExt as _;

    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .await
        .map_err(|_| storage_error("cannot protect filesystem Git directory"))
}

#[cfg(not(unix))]
async fn set_private_directory(_path: &Path) -> Result<(), StoreError> {
    Ok(())
}

async fn sync_parent(path: &Path) -> Result<(), StoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| storage_error("filesystem Git object has no parent"))?
        .to_path_buf();
    sync_directory(parent).await
}

#[cfg(unix)]
async fn sync_directory(path: PathBuf) -> Result<(), StoreError> {
    tokio::task::spawn_blocking(move || {
        std::fs::File::open(path)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| storage_error("cannot flush filesystem Git directory"))
    })
    .await
    .map_err(|_| storage_error("filesystem Git directory flush task failed"))?
}

#[cfg(not(unix))]
async fn sync_directory(_path: PathBuf) -> Result<(), StoreError> {
    Ok(())
}

async fn scan_tree(root: PathBuf, remove_temporaries: bool) -> Result<(u64, u64), StoreError> {
    tokio::task::spawn_blocking(move || scan_tree_sync(&root, remove_temporaries))
        .await
        .map_err(|_| storage_error("filesystem Git scan task failed"))?
}

fn scan_tree_sync(root: &Path, remove_temporaries: bool) -> Result<(u64, u64), StoreError> {
    let mut stack = vec![root.to_path_buf()];
    let mut recovered_directories = std::collections::BTreeSet::new();
    let mut usage = 0u64;
    let mut removed = 0u64;
    while let Some(directory) = stack.pop() {
        for entry in std::fs::read_dir(directory)
            .map_err(|_| storage_error("cannot scan filesystem Git directory"))?
        {
            let entry = entry.map_err(|_| storage_error("cannot scan filesystem Git entry"))?;
            let file_type = entry
                .file_type()
                .map_err(|_| storage_error("cannot inspect filesystem Git entry"))?;
            if file_type.is_symlink() {
                return Err(storage_error("filesystem Git tree contains a symlink"));
            }
            if file_type.is_dir() {
                stack.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                return Err(storage_error("filesystem Git tree contains a special file"));
            }
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| storage_error("filesystem Git name is not UTF-8"))?;
            if name.starts_with(TEMP_PREFIX) {
                if remove_temporaries {
                    std::fs::remove_file(entry.path())
                        .map_err(|_| storage_error("cannot remove abandoned Git temporary"))?;
                    recovered_directories.insert(
                        entry
                            .path()
                            .parent()
                            .ok_or_else(|| storage_error("Git temporary has no parent"))?
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
                        .map_err(|_| storage_error("cannot stat filesystem Git entry"))?
                        .len(),
                )
                .ok_or_else(|| storage_error("filesystem Git usage overflow"))?;
        }
    }
    for directory in recovered_directories {
        std::fs::File::open(directory)
            .and_then(|handle| handle.sync_all())
            .map_err(|_| storage_error("cannot flush recovered Git directory"))?;
    }
    Ok((usage, removed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::git::store::probe::run_git_storage_contract;

    async fn store() -> (tempfile::TempDir, FilesystemGitStorage) {
        let directory = tempfile::tempdir().expect("temporary filesystem Git root");
        let storage = FilesystemGitStorage::open(FilesystemGitConfig {
            root: directory.path().join("git"),
            quota_bytes: None,
        })
        .await
        .expect("open filesystem Git storage");
        (directory, storage)
    }

    #[tokio::test]
    async fn satisfies_shared_git_storage_contract() {
        let (_directory, storage) = store().await;
        let keys = run_git_storage_contract(&storage).await;
        for key in keys {
            storage.remove_probe_key(&key).await;
        }
        assert_eq!(storage.usage_bytes(), 0);
    }

    #[tokio::test]
    async fn recovers_temporary_files_and_persists_objects_across_restart() {
        let (directory, storage) = store().await;
        let bytes = b"restart-safe-pack";
        let key = storage.put_pack(bytes).await.expect("put pack");
        let pointer_key = "pointers/restart.json";
        let etag = match storage
            .put_pointer(pointer_key, b"manifest", Precond::IfNoneMatchStar)
            .await
            .expect("put pointer")
        {
            CasOutcome::Won(etag) => etag,
            CasOutcome::LostRace => panic!("fresh pointer create must win"),
        };
        let abandoned = directory
            .path()
            .join("git/packs")
            .join(".buzz-tmp-abandoned");
        tokio::fs::write(&abandoned, b"uncommitted")
            .await
            .expect("write abandoned temporary");
        let expected_usage = storage.usage_bytes();
        drop(storage);

        let reopened = FilesystemGitStorage::open(FilesystemGitConfig {
            root: directory.path().join("git"),
            quota_bytes: None,
        })
        .await
        .expect("reopen filesystem Git storage");
        assert_eq!(reopened.usage_bytes(), expected_usage);
        assert!(!abandoned.exists());
        assert_eq!(
            reopened
                .get_verified(key.as_str(), &digest_hex(bytes))
                .await
                .expect("read pack"),
            bytes.as_slice()
        );
        assert_eq!(
            reopened
                .get_pointer(pointer_key)
                .await
                .expect("read pointer")
                .expect("pointer exists"),
            (etag, Bytes::from_static(b"manifest"))
        );
    }

    #[tokio::test]
    async fn same_size_pointer_replacement_can_use_a_full_quota() {
        let directory = tempfile::tempdir().expect("temporary filesystem Git root");
        let root = directory.path().join("git");
        let storage = FilesystemGitStorage::open(FilesystemGitConfig {
            root,
            quota_bytes: Some(POINTER_HEADER_BYTES as u64 + 4),
        })
        .await
        .expect("open quota-limited store");
        let key = "pointers/quota.json";
        let etag = match storage
            .put_pointer(key, b"same", Precond::IfNoneMatchStar)
            .await
            .expect("create pointer")
        {
            CasOutcome::Won(etag) => etag,
            CasOutcome::LostRace => panic!("fresh pointer create must win"),
        };
        assert_eq!(storage.usage_bytes(), POINTER_HEADER_BYTES as u64 + 4);
        assert!(matches!(
            storage
                .put_pointer(key, b"next", Precond::IfMatch(etag))
                .await,
            Ok(CasOutcome::Won(_))
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn creates_owner_only_directories_and_files() {
        use std::os::unix::fs::PermissionsExt;

        let (directory, storage) = store().await;
        let key = storage.put_pack(b"permissions").await.expect("put pack");
        let root_mode = tokio::fs::metadata(directory.path().join("git"))
            .await
            .expect("stat root")
            .permissions()
            .mode()
            & 0o777;
        let file_mode = tokio::fs::metadata(directory.path().join("git").join(key))
            .await
            .expect("stat object")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(root_mode, 0o700);
        assert_eq!(file_mode, 0o600);
    }
}
