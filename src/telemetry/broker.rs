//! Append-only local transport; no remote SQL, reads, grants or command execution.
use super::{Event, Store};
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::{Value, json};
use std::{fs, io::{Read, Write}, path::{Path, PathBuf}, sync::{Arc, atomic::{AtomicBool, Ordering}}, thread::JoinHandle, time::{Duration, Instant}};
#[cfg(unix)]
use std::os::unix::net::{UnixListener as Listener, UnixStream as Stream};
#[cfg(windows)]
use std::net::{TcpListener as Listener, TcpStream as Stream};
const FRAME: usize = 17 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(1);
type Res<T> = Result<T, String>;

pub(super) enum Sink { Local { store: Arc<Store>, _server: Server }, Remote(PathBuf, Client) }
impl Sink {
    fn append_once(&self, events: &[Event]) -> Res<()> {
        match self { Self::Local { store, .. } => store.append(events).map(|_| ()), Self::Remote(_,client) => client.append(events) }
    }
    pub fn append(&mut self, events: &[Event]) -> Res<()> {
        match self.append_once(events) {
            Ok(())=>Ok(()),
            Err(error)=> {
                if let Self::Remote(path,_)=self { let replacement=open(path)?; *self=replacement; self.append_once(events) }
                else { Err(error) }
            }
        }
    }
}
#[derive(Clone)]
pub(super) struct Client { endpoint: String, key: [u8;32] }
pub(super) struct Server { stop: Arc<AtomicBool>, worker: Option<JoinHandle<()>>, _lock: crate::file_lock::Lock }
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() { let _ = worker.join(); }
    }
}
fn setup(stream: &Stream) -> Res<()> {
    stream.set_nonblocking(false).map_err(|_|"telemetry IPC blocking mode failed")?;
    #[cfg(windows)] stream.set_nodelay(true).map_err(|_|"telemetry IPC nodelay failed")?;
    stream.set_read_timeout(Some(TIMEOUT)).and_then(|_|stream.set_write_timeout(Some(TIMEOUT))).map_err(|_|"telemetry IPC timeout setup failed".into())
}
fn read_all(stream: &mut Stream, mut bytes: &mut [u8], until: Instant) -> std::io::Result<()> {
    while !bytes.is_empty() {
        let left=until.saturating_duration_since(Instant::now());
        if left.is_zero() { return Err(std::io::ErrorKind::TimedOut.into()); }
        stream.set_read_timeout(Some(left))?;
        let n=stream.read(bytes)?;if n==0 { return Err(std::io::ErrorKind::UnexpectedEof.into()); }
        bytes=&mut bytes[n..];
    } Ok(())
}
fn write_all(stream: &mut Stream, mut bytes: &[u8], until: Instant) -> std::io::Result<()> {
    while !bytes.is_empty() {
        let left=until.saturating_duration_since(Instant::now());
        if left.is_zero() { return Err(std::io::ErrorKind::TimedOut.into()); }
        stream.set_write_timeout(Some(left))?;
        let n=stream.write(bytes)?;if n==0 { return Err(std::io::ErrorKind::WriteZero.into()); }
        bytes=&bytes[n..];
    } Ok(())
}
#[cfg(unix)]
fn connect_unix(path: &str) -> Res<Stream> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    // std UnixStream has no connect deadline; nonblocking connect bounds a saturated accept queue.
    let mut address: libc::sockaddr_un=unsafe { std::mem::zeroed() };
    address.sun_family=libc::AF_UNIX as _;
    let bytes=path.as_bytes();
    if bytes.contains(&0)||bytes.len()>=address.sun_path.len() { return Err("telemetry IPC invalid socket path".into()); }
    for (to,from) in address.sun_path.iter_mut().zip(bytes) { *to=*from as _; }
    #[cfg(target_os="macos")] { address.sun_len=std::mem::size_of_val(&address) as _; }
    let fd=unsafe { libc::socket(libc::AF_UNIX,libc::SOCK_STREAM,0) };
    if fd<0 { return Err("telemetry IPC socket create failed".into()); }
    let owned=unsafe { OwnedFd::from_raw_fd(fd) };
    if unsafe { libc::fcntl(fd,libc::F_SETFD,libc::FD_CLOEXEC) }<0 || unsafe { libc::fcntl(fd,libc::F_SETFL,libc::O_NONBLOCK) }<0 { return Err("telemetry IPC socket setup failed".into()); }
    let connected=unsafe { libc::connect(fd,(&address as *const libc::sockaddr_un).cast(),std::mem::size_of_val(&address) as _) };
    if connected!=0 {
        let error=std::io::Error::last_os_error().raw_os_error();
        if error!=Some(libc::EINPROGRESS) && error!=Some(libc::EWOULDBLOCK) { return Err("telemetry IPC unavailable".into()); }
        // A full Unix accept queue may return EAGAIN without starting a connection.
        if error==Some(libc::EWOULDBLOCK) { return Err("telemetry IPC accept queue full".into()); }
        let until=Instant::now()+TIMEOUT;
        loop {
            let left=until.saturating_duration_since(Instant::now());
            if left.is_zero() { return Err("telemetry IPC connect deadline".into()); }
            let mut poll=libc::pollfd {fd:owned.as_raw_fd(),events:libc::POLLOUT,revents:0};
            let ready=unsafe { libc::poll(&mut poll,1,left.as_millis().max(1).min(i32::MAX as u128) as _) };
            if ready>0 { break; }
            if ready<0 && std::io::Error::last_os_error().raw_os_error()!=Some(libc::EINTR) { return Err("telemetry IPC connect failed".into()); }
        }
        let mut error: libc::c_int=0;let mut length=std::mem::size_of_val(&error) as libc::socklen_t;
        if unsafe { libc::getsockopt(fd,libc::SOL_SOCKET,libc::SO_ERROR,(&mut error as *mut libc::c_int).cast(),&mut length) }!=0 || error!=0 { return Err("telemetry IPC unavailable".into()); }
    }
    let stream=Stream::from(owned);stream.set_nonblocking(false).map_err(|_|"telemetry IPC socket mode failed")?;
    Ok(stream)
}
impl Client {
    fn connect(&self) -> Res<Stream> {
        #[cfg(unix)]
        let stream = connect_unix(&self.endpoint)?;
        #[cfg(windows)]
        let stream = {
            let address: std::net::SocketAddr = self.endpoint.parse().map_err(|_|"telemetry IPC invalid endpoint")?;
            if !matches!(address, std::net::SocketAddr::V4(a) if a.ip() == &std::net::Ipv4Addr::LOCALHOST) { return Err("telemetry IPC requires IPv4 loopback".into()); }
            Stream::connect_timeout(&address, TIMEOUT).map_err(|_|"telemetry IPC unavailable")?
        };
        setup(&stream)?;
        Ok(stream)
    }
    fn request(&self, bytes: &[u8]) -> Res<()> {
        if bytes.len()>FRAME { return Err("telemetry IPC oversized frame".into()); }
        let mut stream=self.connect()?;
        let until=Instant::now()+TIMEOUT;
        write_all(&mut stream,&self.key,until).and_then(|_|write_all(&mut stream,&(bytes.len() as u32).to_be_bytes(),until)).and_then(|_|write_all(&mut stream,bytes,until)).map_err(|_|"telemetry IPC write unacknowledged")?;
        let mut ack=[0;65]; read_all(&mut stream,&mut ack,Instant::now()+Duration::from_secs(2)).map_err(|_|"telemetry IPC commit unacknowledged")?;
        if ack[0]==1 {
            let code=std::str::from_utf8(&ack[1..]).map_err(|_|"telemetry IPC invalid receipt")?.trim_end_matches('\0');
            if !super::FAILURE_CODES.contains(&code) {return Err("telemetry IPC invalid receipt".into());}
            return Err(format!("telemetry IPC rejected: {code}"));
        }
        if ack[0]!=0 || &ack[1..] != crate::sha256_hex(bytes).as_bytes() { return Err("telemetry IPC invalid receipt".into()); }
        Ok(())
    }
    fn append(&self, events: &[Event]) -> Res<()> {
        if events.len()>super::BATCH { return Err("telemetry IPC batch too large".into()); }
        let mut bytes=Vec::new(); bytes.extend_from_slice(&(events.len() as u16).to_be_bytes());
        for event in events {
            bytes.extend_from_slice(&(event.payload.len() as u32).to_be_bytes());
            bytes.extend_from_slice(event.id.as_bytes()); bytes.extend_from_slice(event.payload.as_bytes());
        }
        // An uncertain first commit retries the same serialized ids, never a fresh observation.
        self.request(&bytes)
    }
}
fn serve(mut stream: Stream, key: &[u8;32], store: &Store) -> Res<()> {
    setup(&stream)?;
    let result=receive(&mut stream,key,store,Instant::now()+TIMEOUT);
    let mut response=[0;65];
    match result {
        Ok(receipt)=>response[1..].copy_from_slice(receipt.as_bytes()),
        Err(error)=>{response[0]=1;let code=super::failure_code(&error);response[1..1+code.len()].copy_from_slice(code.as_bytes());}
    }
    write_all(&mut stream,&response,Instant::now()+TIMEOUT).map_err(|_|"telemetry IPC receipt lost".into())
}
fn receive(stream: &mut Stream, key: &[u8;32], store: &Store, until: Instant) -> Res<String> {
    let mut supplied=[0;32];read_all(stream,&mut supplied,until).map_err(|_|"telemetry IPC missing key")?;
    // Hash comparison has fixed-size inputs and does not disclose the credential.
    let authorized=crate::sha256_hex(&supplied)==crate::sha256_hex(key);
    let mut size=[0;4];read_all(stream,&mut size,until).map_err(|_|"telemetry IPC missing size")?;
    let size=u32::from_be_bytes(size) as usize;
    if size>FRAME { return Err("telemetry IPC oversized frame".into()); }
    if !authorized {
        // Drain the bounded request before replying so TCP close cannot discard its rejection as unread data.
        let mut remaining=size;let mut discard=[0;4096];
        while remaining>0 {let n=remaining.min(discard.len());read_all(stream,&mut discard[..n],until).map_err(|_|"telemetry IPC refused")?;remaining-=n;}
        return Err("telemetry IPC refused".into());
    }
    let mut bytes=vec![0;size];read_all(stream,&mut bytes,until).map_err(|_|"telemetry IPC incomplete frame")?;
    let mut input=std::io::Cursor::new(&bytes);
    let mut count=[0;2];input.read_exact(&mut count).map_err(|_|"telemetry IPC missing batch count")?;
    let count=u16::from_be_bytes(count) as usize;
    if count>super::BATCH { return Err("telemetry IPC batch too large".into()); }
    let mut events=Vec::with_capacity(count);
    for _ in 0..count {
        let mut length=[0;4];input.read_exact(&mut length).map_err(|_|"telemetry IPC missing event size")?;
        let length=u32::from_be_bytes(length) as usize;
        if length>super::EVENT_BYTES { return Err("telemetry IPC event too large".into()); }
        let mut id=[0;32];input.read_exact(&mut id).map_err(|_|"telemetry IPC missing id")?;
        if !id.iter().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase()) { return Err("telemetry IPC invalid id".into()); }
        let mut payload=vec![0;length];input.read_exact(&mut payload).map_err(|_|"telemetry IPC missing observation")?;
        let id=std::str::from_utf8(&id).map_err(|_|"telemetry IPC invalid id")?;
        events.push(Event::with_id(serde_json::from_slice(&payload).map_err(|_|"telemetry IPC invalid observation")?,Some(id))?);
    }
    if input.position()!=bytes.len() as u64 { return Err("telemetry IPC trailing bytes".into()); }
    store.append(&events)?;
    Ok(crate::sha256_hex(&bytes))
}
fn read_client(dir: &Path) -> Res<Client> {
    let path=dir.join("endpoint");
    if fs::symlink_metadata(dir).map_err(|_|"telemetry IPC absent")?.file_type().is_symlink() || fs::symlink_metadata(&path).map_err(|_|"telemetry IPC absent")?.file_type().is_symlink() { return Err("telemetry IPC refuses symlinks".into()); }
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        if fs::metadata(dir).map_err(|_|"telemetry IPC absent")?.permissions().mode()&0o077!=0 || fs::metadata(&path).map_err(|_|"telemetry IPC absent")?.permissions().mode()&0o077!=0 { return Err("telemetry IPC state not protected".into()); }
    }
    let mut bytes=Vec::new();fs::File::open(&path).map_err(|_|"telemetry IPC absent")?.take(4097).read_to_end(&mut bytes).map_err(|_|"telemetry IPC state unreadable")?;
    if bytes.len()>4096 { return Err("telemetry IPC state oversized".into()); }
    let state: Value=serde_json::from_slice(&bytes).map_err(|_|"telemetry IPC state invalid")?;
    if state["wire"]!=1 {return Err("telemetry IPC unsupported wire version".into());}
    let endpoint=state["endpoint"].as_str().ok_or("telemetry IPC endpoint missing")?.to_owned();
    #[cfg(unix)] if Path::new(&endpoint)!=dir.join("s") { return Err("telemetry IPC endpoint mismatch".into()); }
    let key: [u8;32]=serde_json::from_value(state["key"].clone()).map_err(|_|"telemetry IPC key invalid")?;
    Ok(Client { endpoint, key })
}
fn publish(dir: &Path, client: &Client) -> Res<()> {
    let tmp=dir.join("endpoint.tmp");
    // The private directory and held election lock own this temporary file.
    if tmp.exists() { fs::remove_file(&tmp).map_err(|_|"telemetry IPC stale temporary state")?; }
    let mut options=fs::OpenOptions::new();options.write(true).create_new(true);
    #[cfg(unix)] { use std::os::unix::fs::OpenOptionsExt; options.mode(0o600); }
    let mut file=options.open(&tmp).map_err(|_|"telemetry IPC state create failed")?;
    file.write_all(json!({"wire":1,"endpoint":client.endpoint,"key":client.key}).to_string().as_bytes()).and_then(|_|file.sync_all()).map_err(|_|"telemetry IPC state write failed")?;
    drop(file);
    #[cfg(windows)] if dir.join("endpoint").exists() { fs::remove_file(dir.join("endpoint")).map_err(|_|"telemetry IPC stale endpoint remove failed")?; }
    fs::rename(tmp,dir.join("endpoint")).map_err(|_|"telemetry IPC state publish failed".into())
}
/// Connect first: sandbox clients need only read the endpoint and append over the transport.
pub(super) fn open(path: &Path) -> Res<Sink> {
    let parent=path.parent().ok_or("telemetry IPC database parent missing")?;
    let dir: PathBuf=parent.join("telemetry-ipc");
    if let Ok(client)=read_client(&dir) { if client.request(&[0,0]).is_ok() { return Ok(Sink::Remote(path.to_owned(),client)); } }
    if fs::symlink_metadata(&dir).is_ok_and(|m|m.file_type().is_symlink()) { return Err("telemetry IPC refuses symlinks".into()); }
    crate::enroll::protect(&dir).map_err(|_|"telemetry IPC writer directory unavailable")?;
    let until=Instant::now()+TIMEOUT;
    let lock=loop {
        match crate::file_lock::try_lock(&dir.join("writer.lock")) {
            Ok(lock)=>break lock,
            Err(std::fs::TryLockError::WouldBlock) if Instant::now()<until=> {
                if let Ok(client)=read_client(&dir) { if client.request(&[0,0]).is_ok() { return Ok(Sink::Remote(path.to_owned(),client)); } }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(_)=>return Err("telemetry IPC writer election unavailable".into()),
        }
    };
    let store=Arc::new(Store::open(path)?);
    #[cfg(unix)]
    let (listener,endpoint)={
        let socket=dir.join("s");
        if socket.exists() { fs::remove_file(&socket).map_err(|_|"telemetry IPC stale socket remove failed")?; }
        let listener=Listener::bind(&socket).map_err(|_|"telemetry IPC socket bind failed")?;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&socket,fs::Permissions::from_mode(0o600)).map_err(|_|"telemetry IPC socket not protected")?;
        (listener,socket.to_str().ok_or("telemetry IPC socket must be UTF-8")?.to_owned())
    };
    #[cfg(windows)]
    let (listener,endpoint)={let l=Listener::bind("127.0.0.1:0").map_err(|_|"telemetry IPC bind failed")?;let address=l.local_addr().map_err(|_|"telemetry IPC address failed")?.to_string();(l,address)};
    listener.set_nonblocking(true).map_err(|_|"telemetry IPC nonblocking failed")?;
    let mut key=[0;32];SystemRandom::new().fill(&mut key).map_err(|_|"telemetry IPC key generation failed")?;
    let client=Client { endpoint,key };
    let stop=Arc::new(AtomicBool::new(false));let halted=stop.clone();let database=store.clone();
    let worker=std::thread::Builder::new().name("telemetry-local-writer".into()).spawn(move|| {
        let mut workers: Vec<JoinHandle<()>>=vec![];
        while !halted.load(Ordering::Acquire) {
            let mut i=0;while i<workers.len() { if workers[i].is_finished() { let _=workers.swap_remove(i).join(); } else { i+=1; } }
            if workers.len()<8 {
                match listener.accept() {
                    Ok((stream,_))=>{let db=database.clone();workers.push(std::thread::spawn(move||{let _=serve(stream,&key,&db);}));}
                    Err(error) if error.kind()==std::io::ErrorKind::WouldBlock=>std::thread::sleep(Duration::from_millis(5)),
                    Err(_)=>break,
                }
            } else { std::thread::sleep(Duration::from_millis(5)); }
        }
        for worker in workers { let _=worker.join(); }
    }).map_err(|_|"telemetry IPC writer thread failed")?;
    let server=Server { stop,worker:Some(worker),_lock:lock };
    publish(&dir,&client)?;
    Ok(Sink::Local { store, _server: server })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event() -> Event { Event::new(json!({"product":"fixtureq","version":"1.0.0","operation":"method.call","kind":"method","trace":"trace","span":"span","started_ms":1,"duration_us":1,"outcome":"ok","error":"none"})).unwrap() }

    #[test]
    fn clients_append_idempotently_fail_closed_and_replace_dead_writer() {
        let dir=crate::scratch("ipc"); let path=dir.join("db");
        let owner=open(&path).unwrap();
        let client=read_client(&dir.join("telemetry-ipc")).unwrap();
        let mut remote=open(&path).unwrap();assert!(matches!(remote,Sink::Remote(..)));
        let event=event();remote.append(&[event.clone()]).unwrap();remote.append(&[event.clone()]).unwrap();
        let mut wrong=client.clone();wrong.key[0]^=1;assert_eq!(wrong.request(&[0,0]).unwrap_err(),"telemetry IPC rejected: auth");
        assert_eq!(client.request(&[255,255]).unwrap_err(),"telemetry IPC rejected: invalid-frame");
        let mut conflict=event.clone();conflict.payload=conflict.payload.replace("method.call","method.other");
        assert_eq!(remote.append(&[event.clone(),conflict]).unwrap_err(),"telemetry IPC rejected: id-conflict");
        let mut invalid=event.clone();let mut value: Value=serde_json::from_str(&invalid.payload).unwrap();
        value["prompt"]=json!("private-sentinel-never-returned");invalid.payload=value.to_string();
        assert_eq!(client.append(&[invalid]).unwrap_err(),"telemetry IPC rejected: invalid-observation");
        let store=Store::open(&path).unwrap();assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),1);
        drop(store);
        // Remaining clients promote under the election lock after the owner exits; ids do not change.
        drop(owner);remote.append(&[event.clone()]).unwrap();assert!(matches!(remote,Sink::Local {..}));
        let next=read_client(&dir.join("telemetry-ipc")).unwrap();assert!(client.key!=next.key);
        let store=Store::open(&path).unwrap();assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),1);
        drop(store);drop(remote);fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn concurrent_clients_share_one_store_and_slow_peers_have_a_deadline() {
        let dir=crate::scratch("ipc-many");let path=dir.join("db");let owner=open(&path).unwrap();
        std::thread::scope(|scope| {
            for _ in 0..4 { let path=&path;scope.spawn(move|| {
                let mut remote=open(path).unwrap();for _ in 0..10 { remote.append(&[event()]).unwrap(); }
            }); }
        });
        let client=read_client(&dir.join("telemetry-ipc")).unwrap();let mut stream=client.connect().unwrap();
        let began=Instant::now();stream.write_all(&[0]).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();let mut byte=[0];
        assert_eq!(stream.read(&mut byte).unwrap(),1);assert_eq!(byte[0],1,"stalled peer receives a failure response");
        assert!(began.elapsed()<Duration::from_secs(2));
        let store=Store::open(&path).unwrap();assert_eq!(store.export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),40);
        drop(store);drop(owner);fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn accepted_nonblocking_stream_waits_for_fragmented_frame() {
        let dir=crate::scratch("ipc-parts");
        #[cfg(unix)] let (listener,endpoint)={let path=dir.join("s");(Listener::bind(&path).unwrap(),path.to_str().unwrap().to_owned())};
        #[cfg(windows)] let (listener,endpoint)={let l=Listener::bind("127.0.0.1:0").unwrap();let address=l.local_addr().unwrap().to_string();(l,address)};
        let client=Client {endpoint,key:[0xaa;32]};
        let peer=std::thread::spawn(move|| {let mut stream=client.connect().unwrap();
            stream.write_all(&[1]).unwrap();std::thread::sleep(Duration::from_millis(50));stream.write_all(&[2]).unwrap();});
        let (mut stream,_)=listener.accept().unwrap();stream.set_nonblocking(true).unwrap();
        setup(&stream).unwrap();let mut frame=[0;2];read_all(&mut stream,&mut frame,Instant::now()+TIMEOUT).unwrap();
        assert_eq!(frame,[1,2]);peer.join().unwrap();drop(stream);drop(listener);fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn forged_receipts_and_unrecognized_failure_codes_are_not_accepted() {
        let dir=crate::scratch("ipc-ack");
        #[cfg(unix)] let (listener,endpoint)={let path=dir.join("s");(Listener::bind(&path).unwrap(),path.to_str().unwrap().to_owned())};
        #[cfg(windows)] let (listener,endpoint)={let l=Listener::bind("127.0.0.1:0").unwrap();let address=l.local_addr().unwrap().to_string();(l,address)};
        let peer=std::thread::spawn(move|| {
            for rejected in [false,true] {
                let (mut stream,_)=listener.accept().unwrap();setup(&stream).unwrap();
                let mut request=[0;38];stream.read_exact(&mut request).unwrap();
                let mut response=[b'0';65];response[0]=0;
                if rejected {response=[0;65];response[0]=1;response[1..19].copy_from_slice(b"unrecognized-class");}
                stream.write_all(&response).unwrap();
            }
        });
        let client=Client {endpoint,key:[0xaa;32]};
        for _ in 0..2 {assert_eq!(client.request(&[0,0]).unwrap_err(),"telemetry IPC invalid receipt");}
        peer.join().unwrap();fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn separate_process_uses_existing_writer_without_database_access() {
        let dir=crate::scratch("ipc-proc");let path=dir.join("db");let owner=open(&path).unwrap();
        #[cfg(unix)] let permissions={
            use std::os::unix::fs::PermissionsExt;
            let old=fs::metadata(&path).unwrap().permissions();
            fs::set_permissions(&path,fs::Permissions::from_mode(0)).unwrap();old
        };
        let status=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","telemetry::broker::tests::client_process","--ignored"])
            .env("COREQ_IPC_TEST_DB",&path).status().unwrap();
        #[cfg(unix)] fs::set_permissions(&path,permissions).unwrap();
        assert!(status.success());
        let store=Store::open(&path).unwrap();let rows=store.export(0,0,1024).unwrap();
        assert_eq!(rows["events"].as_array().unwrap().len(),2);
        assert!(rows["events"].as_array().unwrap().iter().all(|r|r["observation"]["operation"]=="fixtureq.ipc.child"));
        drop(store);drop(owner);fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn child_process_flushes_full_selftest_sized_trace_through_resident_writer() {
        let dir=crate::scratch("ipc-bulk");let path=dir.join("db");let owner=open(&path).unwrap();
        let status=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","telemetry::broker::tests::client_process","--ignored"])
            .env("COREQ_IPC_TEST_DB",&path).env("COREQ_IPC_BULK","1").status().unwrap();
        assert!(status.success(),"child failed the unchanged 3-second qualification barrier");
        let store=Store::open(&path).unwrap();let mut after=0;let mut count=0;
        loop {let page=store.export(0,after,1024).unwrap();count+=page["events"].as_array().unwrap().len();
            after=page["next"].as_i64().unwrap();if page["more"]==false {break;}}
        assert_eq!(count,8000);drop(store);drop(owner);fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    #[ignore="subprocess helper exercised by separate_process_uses_existing_writer_without_database_access"]
    fn client_process() {
        let path=PathBuf::from(std::env::var_os("COREQ_IPC_TEST_DB").unwrap());
        let _runtime=super::super::capture::Runtime::install(&path,"fixtureq","1.0.0").unwrap();
        let count=if std::env::var_os("COREQ_IPC_BULK").is_some(){4000}else{1};
        for _ in 0..count {let scope=tracing::info_span!("ipc.child").entered();drop(scope);}
        super::super::capture::qualify().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn connected_clients_leave_protected_endpoint_permissions_unchanged() {
        use std::os::unix::fs::PermissionsExt;
        let dir=crate::scratch("ipc-ro");let path=dir.join("db");let owner=open(&path).unwrap();
        let ipc=dir.join("telemetry-ipc");fs::set_permissions(&ipc,fs::Permissions::from_mode(0o500)).unwrap();
        let mut client=open(&path).unwrap();assert!(matches!(client,Sink::Remote(..)));client.append(&[event()]).unwrap();
        assert_eq!(fs::metadata(&ipc).unwrap().permissions().mode()&0o777,0o500);
        fs::set_permissions(&ipc,fs::Permissions::from_mode(0o700)).unwrap();drop(client);drop(owner);fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(all(test,unix))]
mod unix_deadline {
    use super::*;
    #[test]
    fn saturated_accept_queue_never_blocks_connect_indefinitely() {
        use std::os::fd::AsRawFd;
        let dir=crate::scratch("ipc-full");let socket=dir.join("s");
        let listener=Listener::bind(&socket).unwrap();
        assert_eq!(unsafe { libc::listen(listener.as_raw_fd(),1) },0);
        let began=Instant::now();let mut held=Vec::new();let mut refused=false;
        for _ in 0..8 { match connect_unix(socket.to_str().unwrap()) { Ok(stream)=>held.push(stream), Err(_)=>{refused=true;break;} } }
        assert!(refused,"the bounded accept queue was not saturated");
        assert!(began.elapsed()<Duration::from_secs(2));
        drop(held);drop(listener);fs::remove_dir_all(dir).unwrap();
    }
}
