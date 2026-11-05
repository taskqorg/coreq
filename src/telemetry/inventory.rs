//! Typed read-only host inventory: the one observation field that may hold a name or path (coreq.md R54).
use super::{code, ship::NOT_COLLECTED};
use serde_json::{Map, Value, json};

const NAME_BYTES: usize = 256;
const PATH_BYTES: usize = 4096;
/// Measures holding a fixed code; every other measure is a u64.
const CODES: &[&str] = &["status", "os", "os_version"];
const HOST: &[&str] = &["status", "os", "os_version", "uptime_s", "load_1_milli", "load_5_milli", "load_15_milli", "cpu_count", "cpu_used_milli", "ram_total_bytes", "ram_used_bytes", "swap_total_bytes", "swap_used_bytes"];
const MOUNT: &[&str] = &["status", "disk_total_bytes", "disk_used_bytes", "disk_free_bytes"];
const WORKLOAD: &[&str] = &["status", "cpu_used_milli", "ram_used_bytes", "disk_used_bytes"];
const FOLDER: &[&str] = &["status", "disk_used_bytes"];

/// Whether the entity is identified by an absolute path, and its measures.
fn entity(record: &Map<String, Value>) -> Option<(bool, &'static [&'static str])> {
    match record.get("entity")?.as_str()? {
        "host" => Some((false, HOST)),
        "mount" => Some((true, MOUNT)),
        "service" | "container" => Some((false, WORKLOAD)),
        "project_root" => Some((true, WORKLOAD)),
        "folder" => Some((true, FOLDER)),
        _ => None,
    }
}

fn single_line(text: &str, limit: usize) -> bool {
    !text.is_empty() && text.len() <= limit && !text.chars().any(char::is_control)
}

/// A display name, never an assignment or a URL, which is how environment values and credentials look.
fn name(text: &str) -> bool {
    single_line(text, NAME_BYTES) && !text.contains('=') && !text.contains("://")
}

/// An absolute normalized POSIX, drive, UNC or extended Windows path. A UNC server holds no login.
fn path(text: &str) -> bool {
    if !single_line(text, PATH_BYTES) {
        return false;
    }
    let (text, extended) = text.strip_prefix(r"\\?\").map_or((text, false), |rest| (rest, true));
    let bytes = text.as_bytes();
    let unc = if extended { text.strip_prefix(r"UNC\") } else { text.strip_prefix(r"\\") };
    let (rest, windows) = if let Some(rest) = unc {
        let mut parts = rest.split(['\\', '/']);
        let server = parts.next().is_some_and(|server| !server.is_empty() && !server.contains(['@', ':']));
        if !server || parts.next().is_none_or(str::is_empty) {
            return false;
        }
        (rest, true)
    } else if bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' && matches!(bytes[2], b'\\' | b'/') {
        (&text[3..], true)
    } else if extended {
        let Some((volume, rest)) = text.split_once('\\') else { return false };
        let Some(guid) = volume.strip_prefix("Volume{").and_then(|id| id.strip_suffix('}')) else { return false };
        if guid.len() != 36 || !guid.bytes().enumerate().all(|(i, b)| if [8, 13, 18, 23].contains(&i) { b == b'-' } else { b.is_ascii_hexdigit() }) {
            return false;
        }
        (rest, true)
    } else if let Some(rest) = text.strip_prefix('/') {
        (rest, false)
    } else {
        return false;
    };
    if rest.is_empty() {
        return true;
    }
    let separator = |c: char| c == '/' || (windows && c == '\\');
    let rest = rest.strip_suffix(separator).unwrap_or(rest);
    rest.split(separator).all(|part| !part.is_empty() && part != "." && part != "..")
}

/// Exactly the listed fields of one entity. A measure is null only with its reason; a reason needs a null or absent measure.
pub(super) fn valid(value: &Value) -> bool {
    let Some(record) = value.as_object() else { return false };
    let Some((is_path, measures)) = entity(record) else { return false };
    let empty = Map::new();
    let reasons = match record.get("unknown") {
        None => &empty,
        Some(Value::Object(reasons)) => reasons,
        Some(_) => return false,
    };
    let identity = record.get("identity").and_then(Value::as_str).is_some_and(|text| if is_path { path(text) } else { name(text) });
    identity
        && record.iter().all(|(key, field)| match key.as_str() {
            "entity" | "identity" | "unknown" => true,
            key if !measures.contains(&key) => false,
            key if field.is_null() => reasons.contains_key(key),
            key if reasons.contains_key(key) => false,
            key if CODES.contains(&key) => field.as_str().is_some_and(code),
            _ => field.is_u64(),
        })
        && reasons.iter().all(|(key, reason)| measures.contains(&key.as_str()) && record.get(key).is_none_or(Value::is_null) && reason.as_str().is_some_and(code))
}

/// Every measure of the entity present, null with its reason when not collected. Anything malformed stays as it is, and validation refuses it.
pub(super) fn shared(value: &Value) -> Value {
    let Some((mut record, (_, measures))) = value.as_object().and_then(|record| Some((record.clone(), entity(record)?))) else { return value.clone() };
    let mut reasons = match record.get("unknown") {
        None => Map::new(),
        Some(Value::Object(reasons)) => reasons.clone(),
        Some(_) => return value.clone(),
    };
    for measure in measures {
        if record.get(*measure).is_none_or(Value::is_null) {
            record.insert((*measure).into(), Value::Null);
            reasons.entry(*measure).or_insert_with(|| json!(NOT_COLLECTED));
        }
    }
    if !reasons.is_empty() {
        record.insert("unknown".into(), Value::Object(reasons));
    }
    Value::Object(record)
}

#[cfg(test)]
mod tests {
    use super::super::{Event, Store};
    use super::*;
    use crate::enroll::{Backoff, Wire};
    use std::path::PathBuf;

    const FILE_CONTENT: &str = "invented file content line";
    const ENVIRONMENT: &str = "API_KEY=invented-environment-value";
    const CREDENTIAL: &str = "ghp_aaaaaaaaaaaaaaaaaaaaaaaa";

    fn observation(inventory: Value) -> Value {
        json!({"product": "serverq", "version": "1.0.0", "operation": "inventory.sample", "kind": "inventory", "trace": "a".repeat(32), "span": "b".repeat(16),
            "started_ms": 1_700_000_000_000u64, "outcome": "ok", "error": "none", "host": "srv-1", "inventory": inventory})
    }

    /// One record per entity, Unicode and every path form included.
    fn samples() -> Vec<Value> {
        vec![
            json!({"entity": "host", "identity": "сервер-1.example", "status": "up", "os": "linux", "os_version": "24.04", "uptime_s": 86_400, "load_1_milli": 1250, "load_5_milli": 900, "load_15_milli": 0,
                "cpu_count": 8, "cpu_used_milli": 1500, "ram_total_bytes": 1u64 << 34, "ram_used_bytes": 1u64 << 33, "swap_total_bytes": 0, "swap_used_bytes": 0}),
            json!({"entity": "mount", "identity": "/", "status": "ok", "disk_total_bytes": 1000, "disk_used_bytes": 400, "disk_free_bytes": 600}),
            json!({"entity": "mount", "identity": r"C:\", "status": "ok", "disk_total_bytes": 1000, "disk_used_bytes": 400, "disk_free_bytes": 600}),
            json!({"entity": "mount", "identity": r"\\?\Volume{00000000-0000-0000-0000-000000000000}\", "status": "ok", "disk_total_bytes": 1000, "disk_used_bytes": null, "disk_free_bytes": 600, "unknown": {"disk_used_bytes": "unsupported-os"}}),
            json!({"entity": "service", "identity": "Windows Update", "status": "running", "cpu_used_milli": 10, "ram_used_bytes": 1024, "disk_used_bytes": null, "unknown": {"disk_used_bytes": "access-denied"}}),
            json!({"entity": "service", "identity": "getty@tty1.service", "status": "running"}),
            json!({"entity": "container", "identity": "web_1", "status": "exited", "cpu_used_milli": 0, "ram_used_bytes": 0, "disk_used_bytes": 2048}),
            json!({"entity": "project_root", "identity": "/home/deploy/проекты/site", "status": "ok", "cpu_used_milli": 5, "ram_used_bytes": 7, "disk_used_bytes": 9}),
            json!({"entity": "project_root", "identity": r"\\nas\share\проект", "status": "ok", "cpu_used_milli": 5, "ram_used_bytes": 7, "disk_used_bytes": 9}),
            json!({"entity": "folder", "identity": r"D:\данные\node_modules\@types", "status": "ok", "disk_used_bytes": 123}),
            json!({"entity": "folder", "identity": "/data/date=2026-11-06/日本語", "status": "partial", "disk_used_bytes": 456}),
        ]
    }

    /// A receiver that checks the inventory contract on its own tables, and DashQ's denied key names.
    #[derive(Default)]
    struct Receiver {
        bodies: String,
        inventory: Vec<Value>,
    }
    impl Wire for Receiver {
        fn post(&mut self, _: &str, route: &str, _: Option<&str>, body: &Value) -> Result<(u16, Value), String> {
            assert_eq!(route, "/ingest");
            self.bodies.push_str(&body.to_string());
            let mut receipts = Vec::new();
            for record in body["records"].as_array().unwrap() {
                let id = record["event_id"].as_str().unwrap();
                let got = &record["payload"]["observation"];
                assert_eq!(record["schema"], "coreq-observation/1");
                assert_eq!(record["payload"]["id"], id);
                assert_eq!(record["payload"].as_object().unwrap().len(), 4);
                assert_eq!((&got["schema"], &got["kind"]), (&json!(1), &json!("inventory")));
                let item = got["inventory"].as_object().unwrap();
                let fields: &[&str] = match item["entity"].as_str().unwrap() {
                    "host" => &["status", "os", "os_version", "uptime_s", "load_1_milli", "load_5_milli", "load_15_milli", "cpu_count", "cpu_used_milli", "ram_total_bytes", "ram_used_bytes", "swap_total_bytes", "swap_used_bytes"],
                    "mount" => &["status", "disk_total_bytes", "disk_used_bytes", "disk_free_bytes"],
                    "service" | "container" | "project_root" => &["status", "cpu_used_milli", "ram_used_bytes", "disk_used_bytes"],
                    "folder" => &["status", "disk_used_bytes"],
                    other => panic!("entity {other}"),
                };
                assert!(item["identity"].is_string());
                for (key, field) in item {
                    assert!(["entity", "identity", "unknown"].contains(&key.as_str()) || fields.contains(&key.as_str()), "{key}");
                    let denied = ["prompt", "command", "cmd", "args", "stdout", "stderr", "output", "input", "content", "text", "message", "path", "cwd", "env", "token", "secret", "password", "credential"];
                    assert!(!denied.iter().any(|d| key == d || key.ends_with(&format!("_{d}"))), "{key}");
                    assert!(field.is_string() || field.is_u64() || field.is_null() || key == "unknown", "{key}");
                }
                for field in fields {
                    assert!(item.contains_key(*field), "{field} is present");
                    assert_eq!(item[*field].is_null(), item.get("unknown").is_some_and(|reasons| reasons.get(*field).is_some_and(Value::is_string)), "{field}");
                }
                self.inventory.push(got["inventory"].clone());
                receipts.push(json!({"event_id": id, "state": "accepted"}));
            }
            Ok((200, json!({"protocol": 1, "receipts": receipts})))
        }
        fn now(&self) -> u64 { 0 }
        fn sleep(&mut self, _: u64) {}
        fn show(&mut self, _: &str, _: &str) {}
    }

    fn setup(name: &str) -> (PathBuf, PathBuf, PathBuf) {
        let dir = crate::scratch(name);
        let connection = dir.join("connection");
        std::fs::create_dir_all(&connection).unwrap();
        std::fs::write(connection.join("connection.json"), json!({"installation": "i", "active": {"destination": "https://d.example", "credential": "invented", "generation": 1}}).to_string()).unwrap();
        (dir.join("db"), connection, dir)
    }

    // Defect: the allowlist refused inventory, so ServerQ had no typed path for resources and disk growth. Protects R54 and R52.
    #[test]
    fn inventory_round_trips_through_store_export_and_shipper() {
        let _disk = crate::bulk_disk();
        let (db, connection, dir) = setup("telemetry-inventory-round-trip");
        let store = Store::open(&db).unwrap();
        let sent = samples();
        let events: Vec<Event> = sent.iter().map(|item| Event::observation(observation(item.clone())).unwrap()).collect();
        store.append(&events).unwrap();
        let exported = store.export(0, 0, 1024).unwrap();
        let mut receiver = Receiver::default();
        let shipped = store.ship(&connection, &mut Backoff::default(), &mut receiver).unwrap();
        assert_eq!((shipped.sent, shipped.rejected), (sent.len(), 0));
        assert_eq!(receiver.inventory.len(), sent.len());
        for (index, item) in sent.iter().enumerate() {
            let stored = &exported["events"][index]["observation"]["inventory"];
            assert_eq!(stored, &receiver.inventory[index], "export and wire agree");
            for (key, field) in item.as_object().unwrap() {
                if key != "unknown" {
                    assert_eq!(&stored[key], field, "{key} is unchanged");
                }
            }
        }
        // A real zero stays zero; the producer's reason wins; an omitted measure reads null with the default reason.
        assert_eq!(receiver.inventory[0]["load_15_milli"], 0);
        assert_eq!(receiver.inventory[3]["unknown"]["disk_used_bytes"], "unsupported-os");
        assert_eq!((&receiver.inventory[5]["ram_used_bytes"], &receiver.inventory[5]["unknown"]["ram_used_bytes"]), (&Value::Null, &json!(NOT_COLLECTED)));
        // A row stored through Event::new with omitted measures reaches the wire in the same shape.
        store.append(&[Event::new(observation(json!({"entity": "folder", "identity": "/srv"}))).unwrap()]).unwrap();
        store.ship(&connection, &mut Backoff::default(), &mut receiver).unwrap();
        assert_eq!(receiver.inventory.last().unwrap()["unknown"], json!({"status": NOT_COLLECTED, "disk_used_bytes": NOT_COLLECTED}));
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn refused(inventory: Value) -> String {
        Event::new(observation(inventory.clone())).expect_err(&inventory.to_string()).to_string()
    }

    // Defect: a free-form inventory field would carry contents, environment or credentials off the host. Protects R54 and R27.
    #[test]
    fn file_content_environment_and_credential_fields_never_leave_the_host() {
        let _disk = crate::bulk_disk();
        let (db, connection, dir) = setup("telemetry-inventory-leak");
        let store = Store::open(&db).unwrap();
        for key in ["content", "file_content", "text", "env", "environment", "API_KEY", "token", "credential", "password", "secret", "prompt", "command", "stdout", "output", "path"] {
            for secret in [FILE_CONTENT, ENVIRONMENT, CREDENTIAL] {
                let mut item = json!({"entity": "folder", "identity": "/srv", "status": "ok", "disk_used_bytes": 1});
                item[key] = json!(secret);
                assert_eq!(refused(item.clone()), "telemetry invalid field inventory", "{key}");
                assert!(Event::observation(observation(item)).is_err(), "{key}");
                let mut top = observation(json!({"entity": "folder", "identity": "/srv"}));
                top[key] = json!(secret);
                assert!(Event::observation(top).is_err(), "top-level {key}");
            }
        }
        for (entity, identity) in [("service", ENVIRONMENT), ("service", "https://user:invented@example.test/repo"), ("container", "line one\nline two"), ("folder", "/srv\n/invented file content"), ("mount", r"\\user:invented@nas\share"), ("mount", r"\\user@nas\share")] {
            assert!(refused(json!({"entity": entity, "identity": identity})).starts_with("telemetry invalid field"), "{identity}");
        }
        assert!(refused(json!({"entity": "folder", "identity": "/srv", "status": FILE_CONTENT})).starts_with("telemetry invalid field"));
        assert!(!refused(json!({"entity": "service", "identity": ENVIRONMENT})).contains("invented"), "an error never repeats the value");
        store.append(&samples().into_iter().map(|item| Event::observation(observation(item)).unwrap()).collect::<Vec<_>>()).unwrap();
        let mut receiver = Receiver::default();
        store.ship(&connection, &mut Backoff::default(), &mut receiver).unwrap();
        assert!(!receiver.bodies.is_empty());
        for secret in [FILE_CONTENT, ENVIRONMENT, CREDENTIAL, "invented file", "invented-environment"] {
            assert!(!receiver.bodies.contains(secret), "{secret}");
        }
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    // Defect: loose typing would turn an unknown into a zero or accept a foreign field. Protects R54 and R52.
    #[test]
    fn unknown_keys_wrong_types_and_oversized_identities_are_refused() {
        let folder = |key: &str, field: Value| {
            let mut item = json!({"entity": "folder", "identity": "/srv"});
            item[key] = field;
            item
        };
        for bad in [
            json!("host"), json!([]), json!({}), json!({"entity": "host"}), json!({"identity": "x"}), json!({"entity": "user", "identity": "x"}), json!({"entity": 1, "identity": "x"}), json!({"entity": "host", "identity": 1}), json!({"entity": "host", "identity": null}),
            folder("disk_used_bytes", json!("1")), folder("disk_used_bytes", json!(-1)), folder("disk_used_bytes", json!(1.5)), folder("disk_used_bytes", json!(true)), folder("disk_used_bytes", json!({})), folder("status", json!(1)), folder("status", json!("not a code")),
            folder("disk_used_bytes", Value::Null), folder("cpu_used_milli", json!(1)), folder("unknown", json!("reason")), folder("unknown", json!({"disk_used_bytes": 1})), folder("unknown", json!({"cpu_used_milli": "no-cgroup"})), folder("unknown", json!({"other": "reason"})), folder("unknown", json!({"status": "not a code"})),
            json!({"entity": "folder", "identity": "/srv", "disk_used_bytes": 1, "unknown": {"disk_used_bytes": "collected-anyway"}}),
            json!({"entity": "service", "identity": "x".repeat(NAME_BYTES + 1)}), json!({"entity": "service", "identity": "я".repeat(NAME_BYTES / 2 + 1)}), json!({"entity": "service", "identity": ""}),
            json!({"entity": "folder", "identity": format!("/{}", "x".repeat(PATH_BYTES))}), json!({"entity": "folder", "identity": ""}),
            json!({"entity": "folder", "identity": "relative/path"}), json!({"entity": "folder", "identity": "~/x"}), json!({"entity": "folder", "identity": "/a/../b"}), json!({"entity": "folder", "identity": "/a/./b"}), json!({"entity": "folder", "identity": "/a//b"}), json!({"entity": "folder", "identity": "C:relative"}), json!({"entity": "folder", "identity": r"C:\a\..\b"}), json!({"entity": "folder", "identity": r"\\server"}), json!({"entity": "folder", "identity": r"\\.\pipe\x"}),
        ] {
            refused(bad);
        }
        for good in [json!({"entity": "service", "identity": "x".repeat(NAME_BYTES)}), json!({"entity": "folder", "identity": format!("/{}", "x".repeat(PATH_BYTES - 1))}), json!({"entity": "folder", "identity": r"/posix\back\slash"}), json!({"entity": "folder", "identity": "/srv/"}), json!({"entity": "folder", "identity": r"C:\srv\"}), json!({"entity": "folder", "identity": "C:/srv"})] {
            Event::observation(observation(good.clone())).unwrap_or_else(|error| panic!("{good}: {error}"));
        }
        // Every backslash escaped, the largest identity still fits one record.
        Event::observation(observation(json!({"entity": "folder", "identity": format!(r"C:\{}", r"a\".repeat((PATH_BYTES - 4) / 2))}))).unwrap();
        // The record bound holds for inventory, too.
        let mut large = observation(json!({"entity": "folder", "identity": "/srv"}));
        large["counts"] = (0..2000).map(|i| (format!("counter.{i:05}.{}", "x".repeat(16)), json!(i))).collect::<Map<_, _>>().into();
        assert_eq!(Event::observation(large).unwrap_err(), "telemetry event too large");
    }

    // Defect: generic UNC parsing accepted relative extended paths and doubled root separators. Protects R54's absolute, normalized identities.
    #[test]
    fn path_identities_are_absolute_and_normalized() {
        let good = [
            "/", "/srv/", r"C:\", "C:/", r"C:\данные", r"/posix\backslash",
            r"\\nas\share", r"\\nas\share\", r"\\nas\share\日本語",
            r"\\?\C:\", r"\\?\C:\данные\", r"\\?\UNC\nas\share", r"\\?\UNC\nas\share\日本語\",
            r"\\?\Volume{00000000-0000-0000-0000-000000000000}\",
            r"\\?\Volume{ABCDEF01-2345-6789-abcd-0123456789ab}\данные\",
        ];
        let bad = [
            r"\\?\C:relative", r"\\?\UNC\server", "//", r"C:\\", "C://",
            "/srv//", r"C:\srv\\", r"\\nas\share\\", r"\\?\C:\\",
            r"\\?\UNC\", r"\\?\UNC\server\", r"\\?\UNC\server\\share", r"\\?\UNC\server\share\\",
            r"\\?\UNC\server\..", r"\\?\UNC\user@nas\share", r"\\?\C:\data\..", r"\\?\C:\data\.\x",
            r"\\?\Volume{00000000-0000-0000-0000-000000000000}",
            r"\\?\Volume{00000000-0000-0000-0000-000000000000}\\",
            r"\\?\Volume{not-a-guid}\", r"\\?\pipe\x", r"\\?\relative\path",
        ];
        for entity in ["mount", "project_root", "folder"] {
            for identity in good {
                let value = observation(json!({"entity": entity, "identity": identity}));
                assert!(Event::observation(value.clone()).is_ok(), "{entity}: {identity}");
                assert!(Event::new(value).is_ok(), "{entity}: {identity}");
            }
            let accepted: Vec<_> = bad.into_iter().filter(|identity| {
                let value = observation(json!({"entity": entity, "identity": identity}));
                Event::observation(value.clone()).is_ok() || Event::new(value).is_ok()
            }).collect();
            assert!(accepted.is_empty(), "{entity} accepted invalid paths: {accepted:?}");
        }
    }

    // Defect: allowing paths for inventory could weaken every telemetry string. Protects R27 and R54 for existing fields.
    #[test]
    fn existing_fields_stay_as_strict_as_before() {
        let item = json!({"entity": "folder", "identity": "/srv"});
        for (key, bad) in [("operation", "/var/lib"), ("source", "/var/lib"), ("host", r"C:\x"), ("actor", "сервер"), ("host", "name with space"), ("project", "/var/lib")] {
            let mut value = observation(item.clone());
            value[key] = json!(bad);
            assert_eq!(Event::observation(value).unwrap_err(), format!("telemetry invalid field {key}"));
        }
        let mut counts = observation(item.clone());
        counts["counts"] = json!({"disk": null});
        assert_eq!(Event::observation(counts).unwrap_err(), "telemetry invalid field counts");
        let mut reason = observation(item.clone());
        reason["unknown"] = json!({"inventory": "reason"});
        assert!(Event::observation(reason).is_err(), "inventory is no top-level unknown");
        // Only kind `inventory` holds the field, and through the shared record that kind needs it.
        let mut other = observation(item);
        other["kind"] = json!("check");
        assert_eq!(Event::observation(other).unwrap_err(), "telemetry invalid field inventory");
        let mut missing = observation(json!({}));
        missing.as_object_mut().unwrap().remove("inventory");
        assert_eq!(Event::observation(missing).unwrap_err(), "telemetry missing inventory");
    }
}
