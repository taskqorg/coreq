//! `<root>/.<name>/status.json`: written each service pass, read by commands and trays (coreq.md § status.json).

use crate::{Tool, write_atomic};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};

pub fn path(t: &Tool, root: &Path) -> PathBuf {
    root.join(format!(".{}", t.name)).join("status.json")
}

/// The common keys; a consumer adds its own to the returned object.
pub fn common(t: &Tool, host: &str, project: &str, at: u64, error: Option<String>, update: Option<&str>) -> Value {
    json!({"host": host, "version": t.version, "pid": std::process::id(), "at": at, "project": project, "error": error, "update": update})
}

pub fn write(t: &Tool, root: &Path, status: &Value) -> Result<(), String> {
    let p = path(t, root);
    fs::create_dir_all(p.parent().unwrap()).map_err(|e| e.to_string())?;
    write_atomic(&p, &serde_json::to_string_pretty(status).unwrap_or_default())
}

/// Seconds since the file was written; unknown when unreadable.
pub fn age(p: &Path) -> Option<u64> {
    fs::metadata(p).and_then(|m| m.modified()).ok().and_then(|t| t.elapsed().ok()).map(|d| d.as_secs())
}

/// The status when readable and at most `max_age` seconds old; else unknown, never zero.
pub fn read(t: &Tool, root: &Path, max_age: u64) -> Option<Value> {
    let p = path(t, root);
    age(&p).filter(|a| *a <= max_age)?;
    serde_json::from_str(&fs::read_to_string(p).ok()?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn written_status_reads_back_while_fresh() {
        let root = crate::scratch("status");
        let t = crate::tool(Path::new("/i"));
        assert_eq!(read(&t, &root, 60), None, "missing is unknown");
        let mut s = common(&t, "mac", "maps", 1, None, Some("1.1.0"));
        s["tasks"] = json!([]);
        write(&t, &root, &s).unwrap();
        assert_eq!(path(&t, &root), root.join(".demoq/status.json"));
        let back = read(&t, &root, 60).unwrap();
        assert_eq!((back["version"].as_str(), back["update"].as_str(), back["error"].is_null()), (Some("1.0.0"), Some("1.1.0"), true));
        assert_eq!(back["tasks"], json!([]));
        fs::write(path(&t, &root), "{").unwrap();
        assert_eq!(read(&t, &root, 60), None, "unreadable is unknown");
    }
}
