//! Built-in profiling: always-on counters per operation, optional traces, a bounded store and a ranked report
//! (coreq.md § Profiling).

use crate::{Tool, write_atomic};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const OUTCOMES: [&str; 4] = ["ok", "failed", "cancelled", "retried"];
/// coreq's measured operations: name, p95 budget in ms, error budget as a failure share.
pub const BUDGETS: [(&str, u64, f64); 4] = [("forge.read", 2_000, 0.05), ("forge.write", 5_000, 0.05), ("release.read", 60_000, 0.05), ("token.read", 1_000, 0.01)];
const MAX_OPS: usize = 32;
const MAX_SAMPLES: usize = 64;
const MAX_CLASSES: usize = 8;
const MAX_SPANS: usize = 256;

#[derive(Clone, Default)]
struct Op {
    count: u64,
    ok: u64,
    failed: u64,
    cancelled: u64,
    retried: u64,
    max: u64,
    samples: Vec<u64>,
    classes: BTreeMap<String, u64>,
    since: u64,
    at: u64,
}

impl Op {
    fn class(&mut self, c: &str, n: u64) {
        let c = if self.classes.contains_key(c) || self.classes.len() < MAX_CLASSES - 1 { c } else { "other" };
        *self.classes.entry(c.into()).or_default() += n;
    }

    fn merge(&mut self, d: &Op) {
        self.count += d.count;
        self.ok += d.ok;
        self.failed += d.failed;
        self.cancelled += d.cancelled;
        self.retried += d.retried;
        self.max = self.max.max(d.max);
        self.samples.extend(&d.samples);
        self.samples.drain(..self.samples.len().saturating_sub(MAX_SAMPLES));
        d.classes.iter().for_each(|(c, n)| self.class(c, *n));
        self.since = if self.since == 0 { d.since } else { self.since.min(d.since) };
        self.at = self.at.max(d.at);
    }

    fn json(&self) -> Value {
        json!({"count": self.count, "ok": self.ok, "failed": self.failed, "cancelled": self.cancelled, "retried": self.retried,
            "max": self.max, "samples": self.samples, "classes": self.classes, "since": self.since, "at": self.at})
    }

    /// A record whose counts disagree or hold no sample is corrupt.
    fn parse(v: &Value) -> Option<Op> {
        let n = |k: &str| v[k].as_u64();
        let o = Op {
            count: n("count")?,
            ok: n("ok")?,
            failed: n("failed")?,
            cancelled: n("cancelled")?,
            retried: n("retried")?,
            max: n("max")?,
            samples: v["samples"].as_array()?.iter().map(Value::as_u64).collect::<Option<_>>()?,
            classes: v["classes"].as_object()?.iter().map(|(k, c)| Some((k.clone(), c.as_u64()?))).collect::<Option<_>>()?,
            since: n("since")?,
            at: n("at")?,
        };
        let sane = o.count == o.ok + o.failed + o.cancelled + o.retried && !o.samples.is_empty() && o.samples.len() <= MAX_SAMPLES && o.classes.keys().all(|c| code(c));
        sane.then_some(o)
    }
}

struct Mem {
    ops: BTreeMap<String, Op>,
    spans: VecDeque<Value>,
}

static MEM: Mutex<Mem> = Mutex::new(Mem { ops: BTreeMap::new(), spans: VecDeque::new() });

/// Names and classes are short codes: never a message, secret or path.
fn code(s: &str) -> bool {
    (1..=32).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
}

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Counts one call of `op`; `class` names the error of a failed call.
pub fn record(op: &str, took: Duration, outcome: &str, class: Option<&str>) {
    if !code(op) || !OUTCOMES.contains(&outcome) {
        return;
    }
    let (ms, at) = (took.as_millis() as u64, now());
    let class = class.map(|c| if code(c) { c } else { "other" });
    let mut m = MEM.lock().unwrap_or_else(|e| e.into_inner());
    let o = m.ops.entry(op.into()).or_default();
    o.count += 1;
    *[&mut o.ok, &mut o.failed, &mut o.cancelled, &mut o.retried][OUTCOMES.iter().position(|x| *x == outcome).unwrap()] += 1;
    o.max = o.max.max(ms);
    o.samples.push(ms);
    if o.samples.len() > MAX_SAMPLES {
        o.samples.remove(0);
    }
    if let Some(c) = class {
        o.class(c, 1);
    }
    o.since = if o.since == 0 { at } else { o.since };
    o.at = at;
    if m.spans.len() == MAX_SPANS {
        m.spans.pop_front();
    }
    m.spans.push_back(json!({"op": op, "ms": ms, "outcome": outcome, "class": class, "at": at}));
}

pub fn path(t: &Tool, root: &Path) -> PathBuf {
    root.join(format!(".{}", t.name)).join("profile.json")
}

/// Merges the counters of this process into the store and clears them.
// ponytail: last writer wins between processes of one root; one service writes per pass, add a lock if counts drift
pub fn flush(t: &Tool, root: &Path) -> Result<(), String> {
    let (ops, spans) = {
        let mut m = MEM.lock().unwrap_or_else(|e| e.into_inner());
        (std::mem::take(&mut m.ops), std::mem::take(&mut m.spans))
    };
    let trace = std::env::var(t.env("PROFILE")).is_ok_and(|v| v == "trace");
    let p = path(t, root);
    let old: Value = fs::read_to_string(&p).ok().and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
    let mut all: BTreeMap<String, Op> = old["ops"].as_object().map(|o| o.iter().filter(|(k, _)| code(k)).filter_map(|(k, v)| Some((k.clone(), Op::parse(v)?))).collect()).unwrap_or_default();
    ops.iter().for_each(|(k, d)| all.entry(k.clone()).or_default().merge(d));
    while all.len() > MAX_OPS {
        let stale = all.iter().min_by_key(|(_, o)| o.at).map(|(k, _)| k.clone()).unwrap();
        all.remove(&stale);
    }
    let ops: Map<String, Value> = all.iter().map(|(k, o)| (k.clone(), o.json())).collect();
    let spans: Vec<Value> = if trace { spans.into() } else { vec![] };
    fs::create_dir_all(p.parent().unwrap()).map_err(|e| e.to_string())?;
    write_atomic(&p, &json!({"schema": format!("{}-profile/1", t.name), "trace": trace, "ops": ops, "traces": spans}).to_string())
}

/// A measured operation for ranking: name, p95, failure rate, retried rate, spread.
type Measured<'a> = (&'a str, u64, f64, f64, f64);

/// Nearest-rank percentile of sorted samples.
fn pct(s: &[u64], q: f64) -> u64 {
    s[((q * s.len() as f64).ceil() as usize).clamp(1, s.len()) - 1]
}

fn rate(n: u64, of: u64) -> f64 {
    (n as f64 / of as f64 * 1000.0).round() / 1000.0
}

/// Every budgeted or stored operation, ranked slow, failing and unstable first; `budgets` adds the consumer's.
pub fn report(t: &Tool, root: &Path, budgets: &[(&str, u64, f64)]) -> Value {
    let unknown = |reason: &str| json!({"state": "unknown", "reason": reason});
    let store = match fs::read_to_string(path(t, root)) {
        Err(_) => Value::Null,
        Ok(s) => match serde_json::from_str::<Value>(&s) {
            Ok(v) if v["ops"].is_object() => v,
            _ => return unknown("corrupt store"),
        },
    };
    let budget: BTreeMap<&str, (u64, f64)> = BUDGETS.iter().chain(budgets).map(|(k, p, e)| (*k, (*p, *e))).collect();
    let mut names: Vec<&str> = budget.keys().copied().collect();
    store["ops"].as_object().into_iter().flatten().filter(|(k, _)| !budget.contains_key(k.as_str())).for_each(|(k, _)| names.push(k));
    let mut rows = Map::new();
    let mut measured: Vec<Measured> = vec![];
    for name in names {
        let rec = &store["ops"][name];
        let row = match Op::parse(rec) {
            None if rec.is_null() => unknown("no samples"),
            None => unknown("corrupt"),
            Some(o) => {
                let mut s = o.samples.clone();
                s.sort_unstable();
                let (p50, p95, failure, retried) = (pct(&s, 0.5), pct(&s, 0.95), rate(o.failed, o.count), rate(o.retried, o.count));
                let spread = (p95 as f64 / p50.max(1) as f64 * 100.0).round() / 100.0;
                let state = match budget.get(name) {
                    None => "unbudgeted",
                    Some((bp, be)) if p95 > *bp || failure > *be => "over",
                    Some(_) => "ok",
                };
                measured.push((name, p95, failure, retried, spread));
                json!({"state": state, "count": o.count, "since": o.since, "at": o.at, "p50": p50, "p95": p95, "max": o.max, "failure": failure,
                    "retried": retried, "spread": spread, "classes": o.classes, "budget": budget.get(name).map(|(p, e)| json!({"p95": p, "failure": e}))})
            }
        };
        rows.insert(name.into(), row);
    }
    let rank = |key: &dyn Fn(&Measured) -> (f64, f64)| {
        let mut m = measured.clone();
        m.sort_by(|a, b| key(b).partial_cmp(&key(a)).unwrap_or(std::cmp::Ordering::Equal));
        m.iter().map(|r| r.0).collect::<Vec<_>>()
    };
    let (state, reason) = if measured.is_empty() { ("unknown", Some("no samples")) } else { ("measured", None) };
    json!({"state": state, "reason": reason, "trace": store["trace"] == true, "ops": rows,
        "slow": rank(&|r| (r.1 as f64, 0.0)), "failing": rank(&|r| (r.2, 0.0)), "unstable": rank(&|r| (r.3, r.4))})
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Counters are process-wide: a parallel flush would take another test's records.
    static SERIAL: Mutex<()> = Mutex::new(());

    /// Chain: record, flush, report. Risks: an unsafe name in the store, an unbounded store, a wrong ranking, traces
    /// written while the switch is off.
    #[test]
    fn counters_flush_to_a_bounded_store_and_rank() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let root = crate::scratch("profile");
        let t = Tool { name: "profq", ..crate::tool(Path::new("/i")) };
        let ms = Duration::from_millis;
        for i in 0..10 {
            record("test.slow", ms(900 + i), "ok", None);
            record("test.flaky", ms(if i < 9 { 10 } else { 500 }), if i % 2 == 0 { "retried" } else { "ok" }, None);
            record("test.down", ms(5), if i < 5 { "failed" } else { "ok" }, (i < 5).then_some(if i == 0 { "/home/me secret" } else { "rate_limit" }));
        }
        record("/Users/me/repo", ms(1), "ok", None);
        record("test.bad", ms(1), "maybe", None);
        flush(&t, &root).unwrap();
        let store: Value = serde_json::from_str(&fs::read_to_string(path(&t, &root)).unwrap()).unwrap();
        let text = store.to_string();
        assert!(!text.contains("/Users") && !text.contains("secret") && !text.contains("test.bad"), "codes only: {text}");
        assert_eq!((store["trace"].as_bool(), store["traces"].as_array().map(Vec::len)), (Some(false), Some(0)));

        let r = report(&t, &root, &[("test.slow", 500, 0.0)]);
        assert_eq!(r["state"], "measured");
        let down = &r["ops"]["test.down"];
        assert_eq!((down["failure"].as_f64(), down["classes"]["rate_limit"].as_u64(), down["classes"]["other"].as_u64()), (Some(0.5), Some(4), Some(1)));
        assert_eq!((r["ops"]["test.slow"]["state"].as_str(), r["ops"]["test.slow"]["p95"].as_u64()), (Some("over"), Some(909)));
        assert_eq!(r["ops"]["forge.write"], json!({"state": "unknown", "reason": "no samples"}), "budgeted, never run: unknown, not zero");
        let first = |k: &str| r[k][0].as_str().map(str::to_string);
        assert_eq!((first("slow"), first("failing"), first("unstable")), (Some("test.slow".into()), Some("test.down".into()), Some("test.flaky".into())));

        // merge on the next pass, the switch writes traces, the store stays bounded
        unsafe { std::env::set_var("PROFQ_PROFILE", "trace") };
        for i in 0..MAX_OPS + 8 {
            record(&format!("test.many{i}"), ms(1), "ok", None);
        }
        record("test.slow", ms(1), "ok", None);
        flush(&t, &root).unwrap();
        unsafe { std::env::remove_var("PROFQ_PROFILE") };
        let store: Value = serde_json::from_str(&fs::read_to_string(path(&t, &root)).unwrap()).unwrap();
        assert_eq!(store["ops"].as_object().unwrap().len(), MAX_OPS);
        assert_eq!((store["ops"]["test.slow"]["count"].as_u64(), store["ops"]["test.slow"]["samples"].as_array().unwrap().len()), (Some(11), 11));
        assert_eq!(store["trace"], true);
        assert!(store["traces"].as_array().unwrap().iter().any(|s| s["op"] == "test.slow"));
    }

    /// Chain: report over an empty or damaged store. Risk: unknown read as zero or healthy.
    #[test]
    fn empty_store_and_corrupt_record_read_unknown() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let root = crate::scratch("profile-unknown");
        let t = Tool { name: "profu", ..crate::tool(Path::new("/i")) };
        let r = report(&t, &root, &[]);
        assert_eq!((r["state"].as_str(), r["reason"].as_str()), (Some("unknown"), Some("no samples")));
        assert_eq!(r["ops"]["forge.read"]["state"], "unknown");
        assert_eq!(r["slow"], json!([]));

        fs::create_dir_all(root.join(".profu")).unwrap();
        let good = json!({"count": 2, "ok": 1, "failed": 1, "cancelled": 0, "retried": 0, "max": 30, "samples": [10, 30], "classes": {"auth": 1}, "since": 1, "at": 2});
        let bad = json!({"count": 5, "ok": 1, "failed": 0, "cancelled": 0, "retried": 0, "max": 1, "samples": [1], "classes": {}, "since": 1, "at": 2});
        fs::write(path(&t, &root), json!({"ops": {"forge.read": good, "token.read": bad, "x.y": "junk"}}).to_string()).unwrap();
        let r = report(&t, &root, &[]);
        assert_eq!((r["ops"]["forge.read"]["p95"].as_u64(), r["ops"]["forge.read"]["failure"].as_f64()), (Some(30), Some(0.5)));
        assert_eq!(r["ops"]["token.read"], json!({"state": "unknown", "reason": "corrupt"}));
        assert_eq!(r["ops"]["x.y"]["reason"], "corrupt");
        assert_eq!(r["slow"], json!(["forge.read"]), "corrupt records are not ranked");

        fs::write(path(&t, &root), "{\"ops\":").unwrap();
        assert_eq!(report(&t, &root, &[]), json!({"state": "unknown", "reason": "corrupt store"}));
        flush(&t, &root).unwrap();
        assert_ne!(report(&t, &root, &[])["reason"], "corrupt store", "flush replaces an unreadable store");
    }
}
