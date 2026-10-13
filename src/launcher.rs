//! The launcher `<install>/<name>` and its rollback (coreq.md § Launcher).

use crate::incident::Fault;
use crate::{Tool, read, write_atomic};
use std::fs;
#[cfg(test)]
use std::path::Path;
use std::process::Command;

/// The version to run. Only `service run` counts starts of a version that is not yet healthy, per project (the name of
/// the working directory); a project's third one restores `previous`.
pub fn pick(t: &Tool, service: bool) -> Result<String, String> {
    // ponytail: two projects with the same folder name share a counter; key by the full root if that matters.
    let project = std::env::current_dir().ok().and_then(|d| Some(d.file_name()?.to_string_lossy().into_owned()));
    pick_for(t, service, project.as_deref().unwrap_or("_"))
}

fn pick_for(t: &Tool, service: bool, project: &str) -> Result<String, String> {
    let mut v = t.current().ok_or(format!("no current release: run `{} update`", t.name))?;
    if service {
        let dir = t.install.join("releases").join(&v);
        if !dir.join("healthy").exists() {
            let starts = dir.join("starts");
            if starts.is_file() {
                let _ = fs::remove_file(&starts); // an older launcher's install-wide counter
            }
            let _ = fs::create_dir_all(&starts);
            let n = read(&starts.join(project)).and_then(|s| s.parse::<u32>().ok()).unwrap_or(0) + 1;
            let _ = fs::write(starts.join(project), n.to_string());
            if n >= 3
                && let Some(prev) = read(&t.install.join("previous")).filter(|p| *p != v)
            {
                eprintln!("{}: {v} failed to become healthy {n} times; back to {prev}", t.name);
                let failed = format!("failed:{v}");
                fault(t, "update.rollback", "launcher:rollback", &[&failed], &[&failed]);
                let _ = write_atomic(&t.install.join("current"), &prev);
                v = prev;
            }
        }
    }
    Ok(v)
}

/// Hand one detected fault to the consumer's hook; a panicking hook changes nothing.
fn fault(t: &Tool, code: &str, site: &str, fields: &[&str], evidence: &[&str]) {
    if let Some(hook) = t.fault {
        let f = Fault { code, fields, board: "", runtime: "", site, revision: "", contract: "", evidence, observed: &[], inferred: &[] };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook(t, &f)));
    }
}

/// Windows `service run` ended with `code`: 101 is a panic of the service.
#[cfg_attr(unix, allow(dead_code))]
fn service_exit(t: &Tool, code: i32) {
    if code == 101 {
        fault(t, "dispatch.panic", "launcher:exit", &[], &["exit:101"]);
    }
}

/// Run the current release with `args`; never returns.
pub fn launch(t: &Tool, args: &[String]) -> ! {
    let service = args.first().map(String::as_str) == Some("service") && args.get(1).map(String::as_str) == Some("run");
    let pick = || pick(t, service).unwrap_or_else(|e| t.die(&e));
    #[cfg(unix)]
    {
        let v = pick();
        let mut c = Command::new(t.release_exe(&v));
        c.args(args);
        use std::os::unix::process::CommandExt;
        let e = c.exec();
        t.die(&format!("exec {v}: {e}"));
    }
    // ponytail: a Scheduled Task restarts only a failed launch, so on Windows the launcher supervises `service run`:
    // exit 75 (updated) runs the new current at once, any other exit waits a minute; stderr goes to <install>/<name>.log.
    // The task runs in the interactive session, so the launcher drops its console window (closing it was STATUS_CONTROL_C_EXIT
    // for the service) and the child gets none. The std handles are reset too: stale console handles made spawn fail and
    // the error print panic (exit 101).
    #[cfg(windows)]
    if service {
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn FreeConsole() -> i32;
            fn SetStdHandle(id: u32, h: *mut std::ffi::c_void) -> i32;
        }
        unsafe {
            FreeConsole();
            for id in [-10i32, -11, -12] {
                SetStdHandle(id as u32, std::ptr::null_mut());
            }
        }
    }
    #[cfg(not(unix))]
    {
        let log = || fs::OpenOptions::new().create(true).append(true).open(t.install.join(format!("{}.log", t.name))).ok();
        let note = |msg: &str| {
            use std::io::Write;
            match log().filter(|_| service) {
                Some(mut f) => {
                    let _ = writeln!(f, "{}: {msg}", t.name);
                }
                None => eprintln!("{}: {msg}", t.name),
            }
        };
        loop {
            let v = pick();
            let mut c = Command::new(t.release_exe(&v));
            c.args(args);
            if service && let Some(out) = log() {
                c.stdin(std::process::Stdio::null());
                if let Ok(err) = out.try_clone() {
                    c.stderr(err);
                }
                c.stdout(out);
                crate::quiet(&mut c);
            }
            let code = match c.status() {
                Ok(s) => s.code().unwrap_or(1),
                Err(e) if service => {
                    note(&format!("start {v}: {e}"));
                    1
                }
                Err(e) => t.die(&format!("start {v}: {e}")),
            };
            if !service {
                std::process::exit(code);
            }
            service_exit(t, code);
            if code != 75 {
                note(&format!("{v} exited with {code}; restart in 60 s"));
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    thread_local!(static SEEN: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) });

    fn record(_: &Tool, f: &Fault) {
        SEEN.with(|s| s.borrow_mut().push(format!("{} {} {} {}", f.code, f.site, f.fields.join(","), f.evidence.join(","))));
    }

    fn seen() -> Vec<String> {
        SEEN.with(|s| s.take())
    }

    #[test]
    fn rollback_hands_one_fault_to_the_hook() {
        let d = crate::scratch("launcher-fault");
        let t = Tool { fault: Some(record), ..crate::tool(&d) };
        fs::create_dir_all(d.join("releases/2.0.0")).unwrap();
        fs::write(d.join("current"), "2.0.0").unwrap();
        for _ in 0..3 {
            pick(&t, true).unwrap();
        }
        assert_eq!(seen(), [] as [String; 0], "no previous: no fault");
        fs::write(d.join("previous"), "1.0.0").unwrap();
        assert_eq!(pick(&t, true), Ok("1.0.0".into()));
        assert_eq!(seen(), ["update.rollback launcher:rollback failed:2.0.0 failed:2.0.0"]);
        assert_eq!(pick(&t, true), Ok("1.0.0".into()));
        assert_eq!(seen(), [] as [String; 0], "restored release counts its own starts");
        let t = Tool { fault: Some(|_, _| panic!("hook")), ..t };
        fs::write(d.join("current"), "2.0.0").unwrap();
        assert_eq!(pick(&t, true), Ok("1.0.0".into()), "a panicking hook changes nothing");
        assert_eq!(t.current().as_deref(), Some("1.0.0"));
    }

    #[test]
    fn service_exit_101_is_a_panic_fault() {
        let t = Tool { fault: Some(record), ..crate::tool(Path::new("/i")) };
        for code in [0, 1, 75] {
            service_exit(&t, code);
        }
        assert_eq!(seen(), [] as [String; 0]);
        service_exit(&t, 101);
        assert_eq!(seen(), ["dispatch.panic launcher:exit  exit:101"]);
        service_exit(&Tool { fault: Some(|_, _| panic!("hook")), ..t }, 101);
    }

    #[test]
    fn third_unhealthy_service_start_rolls_back() {
        let d = crate::scratch("launcher");
        let t = crate::tool(&d);
        assert_eq!(pick(&t, true), Err("no current release: run `demoq update`".into()));
        fs::create_dir_all(d.join("releases/2.0.0")).unwrap();
        fs::write(d.join("current"), "2.0.0").unwrap();
        fs::write(d.join("previous"), "1.0.0").unwrap();
        assert_eq!(pick(&t, false), Ok("2.0.0".into()), "only `service run` counts");
        assert_eq!(pick(&t, true), Ok("2.0.0".into()));
        assert_eq!(pick(&t, true), Ok("2.0.0".into()));
        assert_eq!(pick(&t, true), Ok("1.0.0".into()), "3rd start");
        assert_eq!(t.current().as_deref(), Some("1.0.0"));
        fs::write(d.join("current"), "2.0.0").unwrap();
        fs::write(d.join("releases/2.0.0/healthy"), "").unwrap();
        assert_eq!(pick(&t, true), Ok("2.0.0".into()), "healthy stays");
    }

    #[test]
    fn starts_of_different_projects_do_not_roll_back() {
        let d = crate::scratch("launcher-projects");
        let t = crate::tool(&d);
        fs::create_dir_all(d.join("releases/2.0.0")).unwrap();
        fs::write(d.join("releases/2.0.0/starts"), "2").unwrap();
        fs::write(d.join("current"), "2.0.0").unwrap();
        fs::write(d.join("previous"), "1.0.0").unwrap();
        for p in ["a", "b", "c"] {
            assert_eq!(pick_for(&t, true, p), Ok("2.0.0".into()), "first start of {p}");
        }
        assert_eq!(pick_for(&t, true, "a"), Ok("2.0.0".into()));
        assert_eq!(pick_for(&t, true, "a"), Ok("1.0.0".into()), "3rd start of one project");
    }
}
