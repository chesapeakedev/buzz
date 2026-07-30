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
            root,
        })
    }

    /// Acquire the exclusive process lock for this data directory.
    pub fn lock(&self) -> Result<EmbeddedInstanceLock, DeploymentError> {
        EmbeddedInstanceLock::acquire(&self.lock)
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
}
