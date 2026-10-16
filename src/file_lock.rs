//! Owned advisory lock: release before closing so fork-inherited descriptors cannot extend its lifetime.
use std::{
    fs::{File, OpenOptions, TryLockError},
    path::Path,
};
pub struct Lock(File);
pub fn try_lock(path: &Path) -> Result<Lock, TryLockError> {
    try_lock_file(open(path).map_err(TryLockError::Error)?)
}
pub fn try_lock_file(file: File) -> Result<Lock, TryLockError> {
    file.try_lock()?;
    Ok(Lock(file))
}
pub fn try_lock_shared_file(file: File) -> Result<Lock, TryLockError> {
    file.try_lock_shared()?;
    Ok(Lock(file))
}
impl Lock {
    pub fn file(&self) -> &File { &self.0 }
}
pub fn lock(path: &Path) -> std::io::Result<Lock> {
    let file = open(path)?;
    file.lock()?;
    Ok(Lock(file))
}
fn open(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
}
impl Drop for Lock {
    fn drop(&mut self) {
        if let Err(error) = self.0.unlock() {
            eprintln!("coreq: lock release failed: {error}");
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    // Chain: wake writer/reader -> inherited descriptor -> next writer after the owner retires.
    #[test]
    fn inherited_descriptors_do_not_extend_owned_signal_locks() {
        let path = std::env::temp_dir().join(format!("coreq-signal-lock-{}", std::process::id()));
        for shared in [false, true] {
            let file = open(&path).unwrap();
            if shared { file.try_lock_shared().unwrap(); } else { file.try_lock().unwrap(); }
            let inherited = file.try_clone().unwrap();
            drop(file);
            assert!(try_lock(&path).is_err(), "baseline must reproduce close-only lock lifetime");
            drop(inherited);
            let guard = if shared { try_lock_shared_file(File::open(&path).unwrap()) } else { try_lock_file(open(&path).unwrap()) }.unwrap();
            let inherited = guard.file().try_clone().unwrap();
            drop(guard);
            let next = try_lock(&path).expect("retired owner left its signal locked by an inherited descriptor");
            drop(next); drop(inherited);
        }
        std::fs::remove_file(path).unwrap();
    }
}
