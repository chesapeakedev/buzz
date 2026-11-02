//! Single-node deployment resources shared by embedded relay startup.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use fs2::FileExt as _;
use thiserror::Error;

/// Errors while preparing the embedded data directory.
#[derive(Debug, Error)]
pub enum DeploymentError {
    /// The data directory or lock file could not be opened.
    #[error("embedded data directory is unavailable")]
    Io(#[source] std::io::Error),
    /// Another relay process owns the data directory.
    #[error("another embedded relay already owns the data directory")]
    AlreadyLocked,
}

/// Canonical paths for durable and reconstructable embedded state.
#[derive(Debug, Clone)]
pub struct EmbeddedLayout {
    /// Root directory containing all embedded state.
    pub root: PathBuf,
    /// SQLite database file.
    pub database: PathBuf,
    /// Filesystem media object root.
    pub media_objects: PathBuf,
    /// Filesystem Git object root.
    pub git_objects: PathBuf,
    /// Process lock path.
    pub lock: PathBuf,
    /// Operator-editable configuration file.
    pub config: PathBuf,
    /// Durable relay signing key file.
    pub relay_key: PathBuf,
}

impl EmbeddedLayout {
    /// Create the documented `/data`-style directory layout.
    pub fn prepare(root: impl Into<PathBuf>) -> Result<Self, DeploymentError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(DeploymentError::Io)?;
        for child in [
            "db",
            "secrets",
            "objects/media",
            "objects/git",
            "work/git",
            "work/uploads",
        ] {
            std::fs::create_dir_all(root.join(child)).map_err(DeploymentError::Io)?;
        }
        Ok(Self {
            database: root.join("db/buzz.sqlite3"),
            media_objects: root.join("objects/media"),
            git_objects: root.join("objects/git"),
            lock: root.join("instance.lock"),
            config: root.join("buzz.toml"),
            relay_key: root.join("secrets/relay.key"),
            root,
        })
    }

    /// Acquire the exclusive process lock for this data directory.
    pub fn lock(&self) -> Result<EmbeddedInstanceLock, DeploymentError> {
        EmbeddedInstanceLock::acquire(&self.lock)
    }

    /// Validate that the root is a real writable directory before opening SQLite.
    pub fn validate_writable(&self) -> Result<(), DeploymentError> {
        let metadata = std::fs::symlink_metadata(&self.root).map_err(DeploymentError::Io)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(DeploymentError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "embedded data root is not a directory",
            )));
        }
        let probe = self
            .root
            .join(format!(".write-check-{}", std::process::id()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&probe)
            .map_err(DeploymentError::Io)?;
        use std::io::Write as _;
        file.write_all(b"ok").map_err(DeploymentError::Io)?;
        file.sync_all().map_err(DeploymentError::Io)?;
        std::fs::remove_file(probe).map_err(DeploymentError::Io)
    }

    /// Read a durable secret, creating it exactly once with owner-only mode.
    pub fn load_or_create_secret(
        &self,
        path: &Path,
        generate: impl FnOnce() -> String,
    ) -> Result<String, DeploymentError> {
        if let Ok(metadata) = std::fs::symlink_metadata(path) {
            if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
                return Err(DeploymentError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "durable secret is not a regular file",
                )));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.permissions().mode() & 0o077 != 0 {
                    return Err(DeploymentError::Io(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "durable secret is not owner-only",
                    )));
                }
            }
        }
        match std::fs::read_to_string(path) {
            Ok(value) => {
                let value = value.trim().to_string();
                if value.is_empty() {
                    return Err(DeploymentError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "durable secret is empty",
                    )));
                }
                Ok(value)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let value = generate();
                let mut options = OpenOptions::new();
                options.create_new(true).write(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                let mut file = options.open(path).map_err(DeploymentError::Io)?;
                use std::io::Write as _;
                file.write_all(value.as_bytes())
                    .map_err(DeploymentError::Io)?;
                file.write_all(b"\n").map_err(DeploymentError::Io)?;
                file.sync_all().map_err(DeploymentError::Io)?;
                Ok(value)
            }
            Err(error) => Err(DeploymentError::Io(error)),
        }
    }

    /// Create a commented starter configuration without overwriting operator edits.
    pub fn ensure_default_config(&self) -> Result<(), DeploymentError> {
        if self.config.exists() {
            return Ok(());
        }
        let contents = "# Buzz embedded relay configuration\n\n[server]\n# bind = \"127.0.0.1:3000\"\n# public_url = \"ws://localhost:3000\"\n\n[community]\n# access = \"open\"\n# owner_pubkey = \"<64-char-hex-pubkey>\"\n\n[git]\n# enabled = false\n";
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&self.config).map_err(DeploymentError::Io)?;
        use std::io::Write as _;
        file.write_all(contents.as_bytes())
            .map_err(DeploymentError::Io)?;
        file.sync_all().map_err(DeploymentError::Io)
    }
}

/// OS-held exclusive lock released when the relay process exits.
#[derive(Debug)]
pub struct EmbeddedInstanceLock {
    file: std::fs::File,
}

impl EmbeddedInstanceLock {
    fn acquire(path: &Path) -> Result<Self, DeploymentError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(DeploymentError::Io)?;
        file.try_lock_exclusive()
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::WouldBlock => DeploymentError::AlreadyLocked,
                _ => DeploymentError::Io(error),
            })?;
        Ok(Self { file })
    }
}

impl Drop for EmbeddedInstanceLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_is_created_and_lock_is_exclusive() {
        let directory = tempfile::tempdir().expect("temporary root");
        let layout = EmbeddedLayout::prepare(directory.path().join("data")).expect("layout");
        assert!(layout.database.ends_with("db/buzz.sqlite3"));
        assert!(layout.media_objects.is_dir());
        let lock = layout.lock().expect("first lock");
        assert!(matches!(layout.lock(), Err(DeploymentError::AlreadyLocked)));
        drop(lock);
        let _again = layout.lock().expect("lock after release");
    }

    #[test]
    fn durable_secret_is_created_once_and_reused() {
        let directory = tempfile::tempdir().expect("temporary root");
        let layout = EmbeddedLayout::prepare(directory.path().join("data")).expect("layout");
        let first = layout
            .load_or_create_secret(&layout.relay_key, || "a".repeat(64))
            .expect("create secret");
        let second = layout
            .load_or_create_secret(&layout.relay_key, || "b".repeat(64))
            .expect("read secret");
        assert_eq!(first, "a".repeat(64));
        assert_eq!(second, first);
    }
}
