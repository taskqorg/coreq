//! Process-owned resource samples; no arguments, environment or file contents (coreq.md R56).
use crate::telemetry::Event;
use serde_json::json;
use std::{fs, path::Path, sync::mpsc, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

/// Native host label; missing or unsafe identities stay null, never environment contents.
pub fn hostname() -> Option<String> {
    #[cfg(unix)] let name = {
        let mut bytes=[0u8;256];
        if unsafe {libc::gethostname(bytes.as_mut_ptr().cast(),bytes.len())}!=0 {return None;}
        String::from_utf8(bytes.iter().copied().take_while(|b|*b!=0).collect()).ok()?
    };
    #[cfg(windows)] let name = {
        #[link(name="kernel32")] unsafe extern "system" {fn GetComputerNameW(buffer:*mut u16,size:*mut u32)->i32;}
        let mut bytes=[0u16;256]; let mut size=bytes.len() as u32;
        if unsafe {GetComputerNameW(bytes.as_mut_ptr(),&mut size)}==0 {return None;}
        String::from_utf16(&bytes[..size as usize]).ok()?
    };
    (!name.is_empty() && name.len()<=128 && name.bytes().all(|b|b.is_ascii_alphanumeric()||b"._:-".contains(&b))).then_some(name)
}

#[derive(Default)]
struct Usage { cpu: Option<u64>, ram: Option<u64>, peak: Option<u64>, read: Option<u64>, written: Option<u64> }

pub struct Sampler {
    previous: Option<(Instant, u64)>,
    storage: Option<(Instant, Result<u64, &'static str>)>,
    incarnation: String,
}
impl Default for Sampler {
    fn default() -> Self { Self { previous: None, storage: None, incarnation: crate::incident::uuid() } }
}
impl Sampler {
    pub fn sample(&mut self, home: &Path, product: &str, version: &str) -> Result<Vec<Event>, String> {
        let now = Instant::now();
        let usage = usage();
        let cpu = self.previous.zip(usage.cpu).and_then(|((at, old), new)| {
            let elapsed = now.duration_since(at).as_micros();
            (elapsed > 0).then(|| new.checked_sub(old).map(|delta| (u128::from(delta) * 1000 / elapsed) as u64)).flatten()
        });
        self.previous = usage.cpu.map(|cpu| (now, cpu));
        let storage_due=self.storage.as_ref().is_none_or(|(at, _)| at.elapsed() >= Duration::from_secs(300));
        if storage_due {
            self.storage = Some((now, storage(home)));
        }
        let disk = self.storage.as_ref().unwrap().1;
        let mut inventory = json!({"entity":"service", "identity":format!("{product}:self"),
            "status":"running", "cpu_used_milli":cpu, "ram_used_bytes":usage.ram, "disk_used_bytes":null, "unknown":{}});
        for (field, reason) in [("cpu_used_milli", "no-baseline"), ("ram_used_bytes", "measurement-unavailable"), ("disk_used_bytes", "storage-reported-separately")] {
            if inventory[field].is_null() { inventory["unknown"][field] = reason.into(); }
        }
        let at = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| "clock-unavailable")?.as_millis() as u64;
        let base = |operation: &str| json!({"product":product,"version":version,"kind":"operation","operation":operation,
            "started_ms":at,"host":hostname(),"process_id":std::process::id(),"trace":self.incarnation,"span":null,
            "runtime":format!("{}-{}",std::env::consts::OS,std::env::consts::ARCH),"outcome":"ok","error":"none","unknown":{"span":"no-caller"}});
        let mut row = base("resource.self");
        if row["host"].is_null() {row["unknown"]["host"]="host-unavailable".into();}
        row["kind"] = "inventory".into(); row["inventory"] = inventory;
        if cpu.is_none() || usage.ram.is_none() { row["outcome"]="failed".into(); row["error"]="measurement-incomplete".into(); }
        let mut events = vec![Event::observation(row)?];
        if storage_due {
            let mut row=base("resource.storage");row["kind"]="inventory".into();
            row["inventory"]=json!({"entity":"service","identity":format!("{product}:storage"),"status":"observed","cpu_used_milli":null,"ram_used_bytes":null,"disk_used_bytes":disk.ok(),"unknown":{"cpu_used_milli":"not-applicable","ram_used_bytes":"not-applicable"}});
            if let Err(reason)=disk {row["inventory"]["unknown"]["disk_used_bytes"]=reason.into();row["outcome"]="failed".into();row["error"]=reason.into();}
            events.push(Event::observation(row)?);
        }
        for (operation, quantity, unit) in [("resource.cpu-time", usage.cpu, "cpu-us"), ("resource.ram-peak", usage.peak, "bytes"), ("resource.io-read",usage.read,"bytes"), ("resource.io-write",usage.written,"bytes")] {
            let mut row = base(operation); if row["host"].is_null() {row["unknown"]["host"]="host-unavailable".into();} row["units"]=quantity.into(); row["unit_kind"]=unit.into();
            if quantity.is_none() { row["unknown"]["units"]="measurement-unavailable".into(); row["outcome"]="failed".into(); row["error"]="measurement-unavailable".into(); }
            events.push(Event::observation(row)?);
        }
        Ok(events)
    }
}

/// The receiver disappears on drop; no wait on filesystem I/O or foreign work at shutdown.
pub struct Watch { stop: Option<mpsc::Sender<()>> }
impl Drop for Watch { fn drop(&mut self) { self.stop.take(); } }
pub fn watch(home: &Path, product: &str, version: &str, mut append: impl FnMut(&[Event]) -> Result<(), String> + Send + 'static) -> Result<Watch,String> {
    let (tx, rx)=mpsc::channel(); let home=home.to_owned(); let product=product.to_owned(); let version=version.to_owned();
    std::thread::Builder::new().name("coreq-resources".into()).spawn(move || {
        let mut sampler=Sampler::default();
        loop {
            if let Err(error)=sampler.sample(&home,&product,&version).and_then(|events|append(&events)) {
                let _=std::io::Write::write_all(&mut std::io::stderr(),format!("Resource telemetry: unknown ({error})\n").as_bytes());
            }
            if !matches!(rx.recv_timeout(Duration::from_secs(30)),Err(mpsc::RecvTimeoutError::Timeout)) { break; }
        }
    }).map_err(|e|e.to_string())?;
    Ok(Watch {stop:Some(tx)})
}

fn storage(root: &Path) -> Result<u64, &'static str> {
    fn walk(path: &Path, entries: &mut usize, until: Instant, depth: usize) -> Result<u64,&'static str> {
        if depth > 32 || *entries == 0 || Instant::now() >= until { return Err("storage-scan-limit"); }
        *entries -= 1;
        let meta=fs::symlink_metadata(path).map_err(|_|"storage-unavailable")?;
        if meta.file_type().is_symlink() { return Ok(0); }
        #[cfg(windows)] { use std::os::windows::fs::MetadataExt; if meta.file_attributes() & 0x400 != 0 { return Ok(0); } }
        if meta.is_file() { return Ok(meta.len()); }
        let mut total=0u64;
        for entry in fs::read_dir(path).map_err(|_|"storage-unavailable")? {
            total=total.checked_add(walk(&entry.map_err(|_|"storage-unavailable")?.path(),entries,until,depth+1)?).ok_or("size-overflow")?;
        }
        Ok(total)
    }
    walk(root,&mut 10_000,Instant::now()+Duration::from_secs(1),0)
}

#[cfg(unix)]
fn usage() -> Usage {
    let mut result=Usage::default();
    let mut r:libc::rusage=unsafe {std::mem::zeroed()};
    if unsafe {libc::getrusage(libc::RUSAGE_SELF,&mut r)}==0 {
        result.cpu=Some((r.ru_utime.tv_sec as u64+r.ru_stime.tv_sec as u64)*1_000_000+r.ru_utime.tv_usec as u64+r.ru_stime.tv_usec as u64);
        result.peak=Some(r.ru_maxrss as u64 * if cfg!(target_os="macos") {1} else {1024});
    }
    #[cfg(target_os="linux")] {
        result.ram=fs::read_to_string("/proc/self/statm").ok().and_then(|s|s.split_whitespace().nth(1)?.parse::<u64>().ok()).and_then(|pages| {
            let page=unsafe {libc::sysconf(libc::_SC_PAGESIZE)}; (page>0).then(||pages.checked_mul(page as u64)).flatten()
        });
        if let Ok(io)=fs::read_to_string("/proc/self/io") {
            for line in io.lines() {
                if let Some((key,value))=line.split_once(':') {
                    match key {"read_bytes"=>result.read=value.trim().parse().ok(),"write_bytes"=>result.written=value.trim().parse().ok(),_=>{}}
                }
            }
        }
    }
    #[cfg(target_os="macos")] {
        let mut task:libc::proc_taskinfo=unsafe {std::mem::zeroed()};
        let size=std::mem::size_of::<libc::proc_taskinfo>() as i32;
        if unsafe {libc::proc_pidinfo(std::process::id() as i32,4,0,(&mut task as *mut libc::proc_taskinfo).cast(),size)}==size {result.ram=Some(task.pti_resident_size);}
    }
    result
}

#[cfg(windows)]
fn usage() -> Usage {
    use std::ffi::c_void;
    #[repr(C)] struct Memory { cb:u32,faults:u32,peak:usize,working:usize,paged_peak:usize,paged:usize,nonpaged_peak:usize,nonpaged:usize,pagefile:usize,pagefile_peak:usize }
    #[repr(C)] struct Io {read_ops:u64,write_ops:u64,other_ops:u64,read:u64,written:u64,other:u64}
    #[link(name="kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess()->*mut c_void;
        fn GetProcessTimes(p:*mut c_void,c:*mut u64,e:*mut u64,k:*mut u64,u:*mut u64)->i32;
        fn GetProcessIoCounters(p:*mut c_void,io:*mut Io)->i32;
    }
    #[link(name="psapi")]
    unsafe extern "system" {fn GetProcessMemoryInfo(p:*mut c_void,info:*mut Memory,size:u32)->i32;}
    let mut result=Usage::default();
    unsafe {
        let p=GetCurrentProcess(); let (mut c,mut e,mut k,mut u)=(0,0,0,0);
        if GetProcessTimes(p,&mut c,&mut e,&mut k,&mut u)!=0 {result.cpu=k.checked_add(u).map(|n|n/10);}
        let mut m:Memory=std::mem::zeroed();m.cb=std::mem::size_of::<Memory>() as u32;
        if GetProcessMemoryInfo(p,&mut m,m.cb)!=0 {result.ram=Some(m.working as u64);result.peak=Some(m.peak as u64);}
        let mut io:Io=std::mem::zeroed(); if GetProcessIoCounters(p,&mut io)!=0 {result.read=Some(io.read);result.written=Some(io.written);}
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_self_resources_and_owned_storage_have_explicit_coverage() {
        let root=crate::scratch("resource-self");fs::write(root.join("owned"),vec![0u8;4096]).unwrap();
        #[cfg(unix)] std::os::unix::fs::symlink("/",root.join("foreign")).unwrap();
        assert_eq!(storage(&root).unwrap(),4096);
        let mut sampler=Sampler::default();
        let first=sampler.sample(&root,"fixtureq","1.0.0").unwrap();
        let store=crate::telemetry::Store::open(&root.join("test.db")).unwrap();
        store.append(&first).unwrap();
        let rows=store.export(0,0,100).unwrap();
        let value=&rows["events"][0]["observation"];
        assert_eq!(value["inventory"]["unknown"]["cpu_used_milli"],"no-baseline");
        assert!(value["inventory"]["ram_used_bytes"].as_u64().is_some_and(|n|n>0));
        assert_eq!(rows["events"][1]["observation"]["inventory"]["disk_used_bytes"],4096);
        let second=sampler.sample(&root,"fixtureq","1.0.0").unwrap();
        store.append(&second).unwrap();
        let rows=store.export(0,0,100).unwrap();
        let value=&rows["events"][first.len()]["observation"];assert!(value["inventory"]["cpu_used_milli"].is_u64());
        assert!(!rows.to_string().contains("foreign") && !rows.to_string().contains("owned"));
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }
}
