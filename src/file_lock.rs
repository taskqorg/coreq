//! Owned advisory lock: release before closing so fork-inherited descriptors cannot extend its lifetime.
use std::{
    fs::{File, OpenOptions, TryLockError},
    path::Path,
};
pub struct Lock(File);
pub fn try_lock(path: &Path) -> Result<Lock, TryLockError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .map_err(TryLockError::Error)?;
    file.try_lock()?;
    Ok(Lock(file))
}
impl Drop for Lock {
    fn drop(&mut self) {
        if let Err(error) = self.0.unlock() {
            eprintln!("coreq: lock release failed: {error}");
        }
    }
}
