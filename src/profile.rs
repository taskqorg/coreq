//! Built-in profiling: always-on counters per operation, optional traces, a bounded store and a ranked report
//! (coreq.md § Legacy profiling).

use crate::{Tool, write_atomic};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
        let e = self.classes.entry(c.into()).or_default();
        *e = e.saturating_add(n);
    }

    /// Saturating: a record pushed past `u64::MAX` fails `parse` and reads corrupt, never panics.
    fn merge(&mut self, d: &Op) {
        self.count = self.count.saturating_add(d.count);
        self.ok = self.ok.saturating_add(d.ok);
        self.failed = self.failed.saturating_add(d.failed);
        self.cancelled = self.cancelled.saturating_add(d.cancelled);
        self.retried = self.retried.saturating_add(d.retried);
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

    /// A record whose counts disagree, overflow or hold no sample is corrupt.
    fn parse(v: &Value) -> Option<Op> {
        let n = |k: &str| v[k].as_u64();
        let mut o = Op {
            count: n("count")?,
            ok: n("ok")?,
            failed: n("failed")?,
            cancelled: n("cancelled")?,
            retried: n("retried")?,
            max: n("max")?,
            samples: v["samples"].as_array()?.iter().map(Value::as_u64).collect::<Option<_>>()?,
            classes: BTreeMap::new(),
            since: n("since")?,
            at: n("at")?,
        };
        // a stored record past the class bound folds the rest into `other`, like `record`
        for (k, c) in v["classes"].as_object()? {
            if !code(k) {
                return None;
            }
            o.class(k, c.as_u64()?);
        }
        let sum = [o.failed, o.cancelled, o.retried].iter().try_fold(o.ok, |a, b| a.checked_add(*b));
        let sane = o.count > 0
            && sum == Some(o.count)
            && !o.samples.is_empty()
            && o.samples.len() <= MAX_SAMPLES.min(o.count as usize)
            && o.samples.iter().all(|s| *s <= o.max);
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

/// Shared operation evidence plus the existing bounded compatibility counters.
pub(crate) fn measure<T,E>(op:&'static str,action:impl FnOnce()->Result<T,E>,classify:impl FnOnce(&E)->&'static str)->Result<T,E> {
    let start=Instant::now();let mut class=None;
    let result=crate::telemetry::capture::operation(op,action,|error| {let code=classify(error);class=Some(code);code});
    record(op,start.elapsed(),if result.is_ok() {"ok"}else{"failed"},class);
    result
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

/// Merges the counters of this process into the store and clears them; a failed write keeps them for the next flush.
// ponytail: last writer wins between processes of one root; one service writes per pass, add a lock if counts drift
pub fn flush(t: &Tool, root: &Path) -> Result<(), String> {
    // ponytail: holds the counter lock through one small file write, so `record` waits; fine at one flush per pass
    let mut m = MEM.lock().unwrap_or_else(|e| e.into_inner());
    let trace = std::env::var(t.env("PROFILE")).is_ok_and(|v| v == "trace");
    let p = path(t, root);
    let old: Value = fs::read_to_string(&p).ok().and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
    let mut all: BTreeMap<String, Op> = old["ops"].as_object().map(|o| o.iter().filter(|(k, _)| code(k)).filter_map(|(k, v)| Some((k.clone(), Op::parse(v)?))).collect()).unwrap_or_default();
    m.ops.iter().for_each(|(k, d)| all.entry(k.clone()).or_default().merge(d));
    while all.len() > MAX_OPS {
        let stale = all.iter().min_by_key(|(_, o)| o.at).map(|(k, _)| k.clone()).unwrap();
        all.remove(&stale);
    }
    let ops: Map<String, Value> = all.iter().map(|(k, o)| (k.clone(), o.json())).collect();
    let spans: Vec<&Value> = if trace { m.spans.iter().collect() } else { vec![] };
    fs::create_dir_all(p.parent().unwrap()).map_err(|e| e.to_string())?;
    write_atomic(&p, &json!({"schema": format!("{}-profile/1", t.name), "trace": trace, "ops": ops, "traces": spans}).to_string())?;
    m.ops.clear();
    m.spans.clear();
    Ok(())
}

/// A measured operation for ranking: name, slowness (p95 over its budget; unbudgeted: -1 and p95 ms, ranked after
/// every budgeted one), failure rate, retried rate, spread.
type Measured<'a> = (&'a str, (f64, f64), f64, f64, f64);

/// Nearest-rank percentile of sorted samples; a consumer that keeps its own samples uses it too.
pub fn pct(s: &[u64], q: f64) -> u64 {
    s[((q * s.len() as f64).ceil() as usize).clamp(1, s.len()) - 1]
}

/// `cv` above this is `unstable`.
const UNSTABLE_CV: f64 = 1.0;

/// Statistics and budget verdict of one sample set a consumer keeps itself (docs/design/profile.md § Sample ranking).
#[derive(Clone, Debug, PartialEq)]
pub struct Summary {
    pub p50: u64,
    pub p95: u64,
    pub max: u64,
    pub mean: f64,
    pub stddev: f64,
    pub cv: f64,
    pub failure: f64,
    /// Breached budgets in this order: `time`, `failures`, `unstable`.
    pub over: Vec<&'static str>,
    /// Largest of p95/budget, failure rate/budget and cv/1; `rank` sorts by it.
    pub score: f64,
}

/// Summarizes durations in ms (any order), `failed` of them failures, against a p95 budget (none: time is not judged)
/// and a failure budget; no samples: None, never zeros.
pub fn summarize(durations: &[u64], failed: u64, p95_budget: Option<u64>, failure_budget: f64) -> Option<Summary> {
    let mut d = durations.to_vec();
    d.sort_unstable();
    let max = *d.last()?;
    let n = d.len() as f64;
    let mean = d.iter().map(|&x| x as f64).sum::<f64>() / n;
    let stddev = (d.iter().map(|&x| (x as f64 - mean).powi(2)).sum::<f64>() / n).sqrt();
    let cv = if mean > 0.0 { stddev / mean } else { 0.0 };
    let (p95, failure) = (pct(&d, 0.95), failed as f64 / n);
    let ratios = [("time", p95_budget.map(|b| p95 as f64 / b.max(1) as f64)), ("failures", Some(failure / failure_budget.max(f64::MIN_POSITIVE))), ("unstable", Some(cv / UNSTABLE_CV))];
    let over = ratios.iter().filter(|r| r.1.is_some_and(|x| x > 1.0)).map(|r| r.0).collect();
    let score = ratios.iter().filter_map(|r| r.1).fold(0.0, f64::max);
    Some(Summary { p50: pct(&d, 0.5), p95, max, mean, stddev, cv, failure, over, score })
}

/// Sorts worst score first; equal scores keep their order.
pub fn rank<T>(items: &mut [T], score: impl Fn(&T) -> f64) {
    items.sort_by(|a, b| score(b).partial_cmp(&score(a)).unwrap_or(std::cmp::Ordering::Equal));
}

/// Display value only: budgets and ranks compare the unrounded rate.
fn round(x: f64, places: i32) -> f64 {
    let f = 10f64.powi(places);
    (x * f).round() / f
}

/// Every budgeted or stored operation, ranked slow, failing and unstable first; `budgets` adds the consumer's.
pub fn report(t: &Tool, root: &Path, budgets: &[(&str, u64, f64)]) -> Value {
    let unknown = |reason: &str| json!({"state": "unknown", "reason": reason});
    let store = match fs::read_to_string(path(t, root)) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Value::Null,
        Err(_) => return unknown("corrupt store"),
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
                let (p50, p95, failure, retried) = (pct(&s, 0.5), pct(&s, 0.95), o.failed as f64 / o.count as f64, o.retried as f64 / o.count as f64);
                let spread = p95 as f64 / p50.max(1) as f64;
                let state = match budget.get(name) {
                    None => "unbudgeted",
                    Some((bp, be)) if p95 > *bp || failure > *be => "over",
                    Some(_) => "ok",
                };
                let slow = budget.get(name).map_or((-1.0, p95 as f64), |(bp, _)| (p95 as f64 / (*bp).max(1) as f64, 0.0));
                measured.push((name, slow, failure, retried, spread));
                json!({"state": state, "count": o.count, "since": o.since, "at": o.at, "p50": p50, "p95": p95, "max": o.max, "failure": round(failure, 3),
                    "retried": round(retried, 3), "spread": round(spread, 2), "classes": o.classes, "budget": budget.get(name).map(|(p, e)| json!({"p95": p, "failure": e}))})
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
        "slow": rank(&|r| r.1), "failing": rank(&|r| (r.2, 0.0)), "unstable": rank(&|r| (r.3, r.4))})
}

/// An incident's link to the profile report of its window: `window:<since>-<at>` of the stored measurements (else
/// `window:unknown:<reason>`), then up to 7 operations over coreq's budgets as `<op>:over`, slowest first.
pub fn window(t: &Tool, root: &Path) -> Vec<String> {
    let r = report(t, root, &[]);
    let ops = r["ops"].as_object().into_iter().flatten().map(|(_, v)| v);
    let (since, at) = (ops.clone().filter_map(|v| v["since"].as_u64()).min(), ops.filter_map(|v| v["at"].as_u64()).max());
    let first = match (since, at) {
        (Some(s), Some(a)) => format!("window:{s}-{a}"),
        _ => format!("window:unknown:{}", r["reason"].as_str().unwrap_or("corrupt").replace(' ', "-")),
    };
    let over = r["slow"].as_array().into_iter().flatten().filter_map(Value::as_str).filter(|n| r["ops"][*n]["state"] == "over");
    std::iter::once(first).chain(over.take(7).map(|n| format!("{n}:over"))).collect()
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
        let zero = json!({"count": 0, "ok": 0, "failed": 0, "cancelled": 0, "retried": 0, "max": 1, "samples": [1], "classes": {}, "since": 1, "at": 2});
        let wrap = json!({"count": 0, "ok": u64::MAX, "failed": 1, "cancelled": 0, "retried": 0, "max": 1, "samples": [1], "classes": {}, "since": 1, "at": 2});
        let ops = json!({"forge.read": good, "token.read": bad, "forge.write": zero, "release.read": wrap, "x.y": "junk"});
        fs::write(path(&t, &root), json!({ "ops": ops }).to_string()).unwrap();
        let r = report(&t, &root, &[]);
        assert_eq!((r["ops"]["forge.read"]["p95"].as_u64(), r["ops"]["forge.read"]["failure"].as_f64()), (Some(30), Some(0.5)));
        assert_eq!(r["ops"]["token.read"], json!({"state": "unknown", "reason": "corrupt"}));
        for k in ["forge.write", "release.read", "x.y"] {
            assert_eq!(r["ops"][k]["reason"], "corrupt", "{k}: zero count or overflowing sum is corrupt, never ok");
        }
        // a stored record at the counter limit merges without a panic
        let full = json!({"count": u64::MAX, "ok": u64::MAX, "failed": 0, "cancelled": 0, "retried": 0, "max": 1, "samples": [1], "classes": {"auth": u64::MAX}, "since": 1, "at": 2});
        fs::write(path(&t, &root), json!({"ops": {"forge.read": full}}).to_string()).unwrap();
        record("forge.read", Duration::from_millis(1), "failed", Some("auth"));
        flush(&t, &root).unwrap();
        assert_eq!(report(&t, &root, &[])["ops"]["forge.read"]["reason"], "corrupt", "saturated sum reads corrupt");
        assert_eq!(r["slow"], json!(["forge.read"]), "corrupt records are not ranked");

        fs::write(path(&t, &root), "{\"ops\":").unwrap();
        assert_eq!(report(&t, &root, &[]), json!({"state": "unknown", "reason": "corrupt store"}));
        flush(&t, &root).unwrap();
        assert_ne!(report(&t, &root, &[])["reason"], "corrupt store", "flush replaces an unreadable store");
    }

    /// Chain: flush fails, then succeeds. Risk: a failed write loses the pass's measurements.
    #[test]
    fn failed_flush_keeps_measurements_for_retry() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let root = crate::scratch("profile-retry");
        let t = Tool { name: "profr", ..crate::tool(Path::new("/i")) };
        fs::write(root.join(".profr"), "").unwrap();
        record("forge.read", Duration::from_millis(7), "ok", None);
        assert!(flush(&t, &root).is_err());
        fs::remove_file(root.join(".profr")).unwrap();
        flush(&t, &root).unwrap();
        assert_eq!(report(&t, &root, &[])["ops"]["forge.read"]["count"], 1);
    }

    /// Chain: a stored record past the class bound, then an unreadable store. Risks: an unbounded store, an unreadable
    /// store read as empty.
    #[test]
    fn stored_classes_fold_and_unreadable_store_reads_corrupt() {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let root = crate::scratch("profile-bound");
        let t = Tool { name: "profc", ..crate::tool(Path::new("/i")) };
        let classes: Map<String, Value> = (0..9).map(|i| (format!("c{i}"), json!(1))).collect();
        let rec = json!({"count": 9, "ok": 0, "failed": 9, "cancelled": 0, "retried": 0, "max": 1, "samples": [1], "classes": classes, "since": 1, "at": 2});
        fs::create_dir_all(root.join(".profc")).unwrap();
        fs::write(path(&t, &root), json!({"ops": {"forge.read": rec}}).to_string()).unwrap();
        flush(&t, &root).unwrap();
        let store: Value = serde_json::from_str(&fs::read_to_string(path(&t, &root)).unwrap()).unwrap();
        let kept = store["ops"]["forge.read"]["classes"].as_object().unwrap();
        assert_eq!((kept.len(), kept["other"].as_u64()), (MAX_CLASSES, Some(2)), "{kept:?}");

        fs::remove_file(path(&t, &root)).unwrap();
        fs::create_dir(path(&t, &root)).unwrap();
        assert_eq!(report(&t, &root, &[]), json!({"state": "unknown", "reason": "corrupt store"}));
    }

    /// Chain: stored rates to budget state and failure ranking. Risk: rounding before comparison hides a breach or a rank.
    #[test]
    fn budgets_and_ranks_compare_unrounded_rates() {
        let root = crate::scratch("profile-round");
        let t = Tool { name: "profb", ..crate::tool(Path::new("/i")) };
        let op = |count: u64, failed: u64| json!({"count": count, "ok": count - failed, "failed": failed, "cancelled": 0, "retried": 0,
            "max": 1, "samples": [1], "classes": {}, "since": 1, "at": 2});
        let ops = json!({"forge.read": op(1010, 51), "test.a": op(1000, 1), "test.b": op(999, 1)});
        fs::create_dir_all(root.join(".profb")).unwrap();
        fs::write(path(&t, &root), json!({"ops": ops}).to_string()).unwrap();
        let r = report(&t, &root, &[]);
        assert_eq!(r["ops"]["forge.read"]["state"], "over", "51/1010 = 5.0495% exceeds the 5% budget");
        assert_eq!((r["ops"]["test.a"]["failure"].as_f64(), r["ops"]["test.b"]["failure"].as_f64()), (Some(0.001), Some(0.001)));
        assert_eq!(r["failing"], json!(["forge.read", "test.b", "test.a"]), "1/999 ranks above 1/1000 despite one display value");
    }

    /// Chain: consumer samples to summary and rank. Risks: a breach hidden by rounding or a wrong budget ratio, an
    /// empty set read as zeros, an unstable but fast operation ranked healthy.
    #[test]
    fn consumer_samples_summarize_and_rank_worst_first() {
        assert_eq!(summarize(&[], 0, Some(10), 0.01), None);
        let hook = summarize(&[2500, 2400], 0, Some(1000), 0.01).unwrap();
        assert_eq!((hook.p50, hook.p95, hook.max, hook.over.clone(), hook.score), (2400, 2500, 2500, vec!["time"], 2.5));
        let run = summarize(&[10, 5000], 1, None, 0.01).unwrap();
        assert_eq!((run.failure, run.over.clone(), run.score), (0.5, vec!["failures"], 50.0), "unbudgeted time is not judged");
        let flat = summarize(&[50; 4], 0, Some(10_000), 0.05).unwrap();
        assert_eq!((flat.mean, flat.stddev, flat.cv, flat.over.clone()), (50.0, 0.0, 0.0, vec![]));
        let spiky = summarize(&[1, 1, 1, 1000], 0, Some(10_000), 0.05).unwrap();
        assert!(spiky.cv > 1.0 && spiky.over == ["unstable"], "{spiky:?}");
        assert_eq!(summarize(&[1], 1, Some(10), 0.0).unwrap().over, ["failures"], "a zero failure budget is breached by any failure");
        let mut rows = vec![("flat", flat.score), ("hook", hook.score), ("run", run.score), ("tie", hook.score)];
        rank(&mut rows, |r| r.1);
        assert_eq!(rows.iter().map(|r| r.0).collect::<Vec<_>>(), ["run", "hook", "tie", "flat"]);
    }

    /// Chain: stored durations to the slow rank and an incident's profile window. Risks: a slow rank by raw p95 hides a
    /// breach behind a long but in-budget call; an incident without its window or with unbounded evidence.
    #[test]
    fn slow_ranks_against_budget_and_feeds_the_incident_window() {
        let root = crate::scratch("profile-slow");
        let t = Tool { name: "profw", ..crate::tool(Path::new("/i")) };
        assert_eq!(window(&t, &root), ["window:unknown:no-samples"]);
        let op = |ms: u64, since: u64, at: u64| json!({"count": 1, "ok": 1, "failed": 0, "cancelled": 0, "retried": 0,
            "max": ms, "samples": [ms], "classes": {}, "since": since, "at": at});
        let ops = json!({"release.read": op(10_000, 5, 9), "forge.read": op(3_000, 7, 20), "test.long": op(90_000, 6, 8)});
        fs::create_dir_all(root.join(".profw")).unwrap();
        fs::write(path(&t, &root), json!({"ops": ops}).to_string()).unwrap();
        assert_eq!(report(&t, &root, &[])["slow"], json!(["forge.read", "release.read", "test.long"]), "3s of 2s above 10s of 60s");
        assert_eq!(window(&t, &root), ["window:5-20", "forge.read:over"]);

        // the window rides in the incident report and passes its code check
        let f = crate::incident::Fault { code: "test.fault", fields: &[], board: "github", runtime: "claude", site: "s", revision: "r", contract: "c", evidence: &[], observed: &[], inferred: &[] };
        let o = crate::incident::report(&t, &root, None, &f, None);
        let saved: Value = serde_json::from_str(&fs::read_to_string(o.path.unwrap()).unwrap()).unwrap();
        assert_eq!(saved["profile"], json!(["window:5-20", "forge.read:over"]));
        fs::write(path(&t, &root), "{").unwrap();
        assert_eq!(window(&t, &root), ["window:unknown:corrupt-store"]);
    }
}
