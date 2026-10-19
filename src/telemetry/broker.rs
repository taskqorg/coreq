//! Append-only local transport; no remote SQL, reads, grants or command execution.
use super::{Budget, Event, Store};
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

pub(super) enum Sink { Local { store: Arc<Store>, _server: Server }, Remote(PathBuf, Client, Option<Budget>) }
impl Sink {
    fn append_once(&self, events: &[Event]) -> Res<()> {
        match self { Self::Local { store, .. } => store.append_group(events).map(|_| ()), Self::Remote(_,client,_) => client.append(events) }
    }
    pub fn append(&mut self, events: &[Event]) -> Res<()> {
        match self.append_once(events) {
            Ok(())=>Ok(()),
            Err(error)=> {
                if let Self::Remote(path,_,budget)=self { let replacement=open_with_budget(path,budget.clone())?; *self=replacement; self.append_once(events) }
                else { Err(error) }
            }
        }
    }
}
#[derive(Clone)]
pub(super) struct Client { endpoint: String, key: [u8;32], max_events: usize, database: String, budget: Option<String> }
pub(super) struct Server { stop: Arc<AtomicBool>, worker: Option<JoinHandle<()>>, #[cfg(target_os="macos")] wake:Arc<std::os::fd::OwnedFd>, #[cfg(unix)] socket: Option<Socket>, _lock: crate::file_lock::Lock }
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        #[cfg(target_os="macos")]
        if let Err(error)=wake_listener(&self.wake) {let _=writeln!(std::io::stderr(),"Telemetry: unknown ({error})");}
        if let Some(worker) = self.worker.take() { let _ = worker.join(); }
        #[cfg(unix)] drop(self.socket.take()); // Retire the address before releasing writer election.
    }
}

#[cfg(unix)]
struct Socket { path: PathBuf, fallback: bool }
#[cfg(unix)]
impl Drop for Socket {
    fn drop(&mut self) {
        let _=fs::remove_file(&self.path);
        if self.fallback { let _=fs::remove_dir(self.path.parent().unwrap()); }
    }
}

#[cfg(unix)]
fn socket_path(dir: &Path, database: &str) -> PathBuf {
    use std::os::unix::ffi::OsStrExt;
    let path=dir.join("s");
    let address:libc::sockaddr_un=unsafe {std::mem::zeroed()};
    if path.as_os_str().as_bytes().len()<address.sun_path.len() {path}
    else {PathBuf::from(format!("/tmp/coreq-ipc-{}-{database}/s",unsafe {libc::geteuid()}))}
}

#[cfg(unix)]
fn fallback_directory(socket: &Path, create: bool) -> Res<()> {
    use std::os::unix::fs::{DirBuilderExt,MetadataExt};
    let dir=socket.parent().ok_or("telemetry IPC socket parent missing")?;
    if create {
        match fs::DirBuilder::new().mode(0o700).create(dir) {
            Ok(())=>{},Err(e) if e.kind()==std::io::ErrorKind::AlreadyExists=>{},
            Err(_)=>return Err("telemetry IPC fallback directory unavailable".into()),
        }
    }
    let meta=fs::symlink_metadata(dir).map_err(|_|"telemetry IPC fallback directory unavailable")?;
    if !meta.file_type().is_dir() || meta.uid()!=unsafe {libc::geteuid()} || meta.mode()&0o777!=0o700 {
        return Err("telemetry IPC fallback directory not protected; unchanged".into());
    }
    Ok(())
}
// Keep native readiness registrations across shutdown waits.
#[cfg(target_os="macos")]
fn wake_listener(queue:&std::os::fd::OwnedFd)->Res<()> {
    use std::os::fd::AsRawFd;
    let event=libc::kevent {ident:1,filter:libc::EVFILT_USER,flags:0,fflags:libc::NOTE_TRIGGER,data:0,udata:std::ptr::null_mut()};
    loop {
        if unsafe {libc::kevent(queue.as_raw_fd(),&event,1,std::ptr::null_mut(),0,std::ptr::null())}>=0 {return Ok(());}
        if std::io::Error::last_os_error().raw_os_error()!=Some(libc::EINTR) {return Err("telemetry IPC shutdown wake failed; listener fallback pending".into());}
    }
}
struct Readiness {listener:Listener,#[cfg(target_os="macos")] queue:Arc<std::os::fd::OwnedFd>,#[cfg(windows)] event:usize}
impl Readiness {
    fn new(listener:Listener)->Res<Self> {
        let _listener=&listener;
        #[cfg(target_os="macos")]
        {
            use std::os::fd::{AsRawFd,FromRawFd,OwnedFd};
            let fd=unsafe {libc::kqueue()};if fd<0 {return Err("telemetry IPC readiness queue unavailable".into());}
            let queue=Arc::new(unsafe {OwnedFd::from_raw_fd(fd)});
            if unsafe {libc::fcntl(fd,libc::F_SETFD,libc::FD_CLOEXEC)}<0 {return Err("telemetry IPC readiness queue setup failed".into());}
            let changes=[libc::kevent {ident:_listener.as_raw_fd() as _,filter:libc::EVFILT_READ,flags:libc::EV_ADD,fflags:0,data:0,udata:std::ptr::null_mut()},
                libc::kevent {ident:1,filter:libc::EVFILT_USER,flags:libc::EV_ADD|libc::EV_CLEAR,fflags:0,data:0,udata:std::ptr::null_mut()}];
            if unsafe {libc::kevent(queue.as_raw_fd(),changes.as_ptr(),2,std::ptr::null_mut(),0,std::ptr::null())}<0 {return Err("telemetry IPC listener registration failed".into());}
            Ok(Self {listener,queue})
        }
        #[cfg(windows)]
        {
            use std::{ffi::c_void,os::windows::io::AsRawSocket};
            #[link(name="Ws2_32")] unsafe extern "system" {
                fn WSACreateEvent()->*mut c_void;
                fn WSACloseEvent(event:*mut c_void)->i32;
                fn WSAEventSelect(socket:usize,event:*mut c_void,events:i32)->i32;
            }
            let event=unsafe {WSACreateEvent()};if event.is_null() {return Err("telemetry IPC readiness event unavailable".into());}
            if unsafe {WSAEventSelect(_listener.as_raw_socket() as usize,event,8)}!=0 {
                unsafe {WSACloseEvent(event)};return Err("telemetry IPC listener registration failed".into());
            }
            Ok(Self {listener,event:event as usize})
        }
        #[cfg(all(not(target_os="macos"),not(windows)))] {Ok(Self {listener})}
    }
    fn readable(&self)->Res<bool> {
        let _listener=&self.listener;
        #[cfg(target_os="macos")]
        let (result,events,read,error)={
            use std::os::fd::AsRawFd;
            let mut event:libc::kevent=unsafe {std::mem::zeroed()};let timeout=libc::timespec {tv_sec:1,tv_nsec:0};
            let result=unsafe {libc::kevent(self.queue.as_raw_fd(),std::ptr::null(),0,&mut event,1,&timeout)};
            if result>0 && event.filter==libc::EVFILT_USER && event.flags&libc::EV_ERROR==0 {return Ok(false);}
            (result,i16::from(event.filter==libc::EVFILT_READ),1,(result<0 && std::io::Error::last_os_error().raw_os_error()!=Some(libc::EINTR)) || (result>0 && event.flags&libc::EV_ERROR!=0))
        };
    #[cfg(all(unix,not(target_os="macos")))]
    let (result,events,read,error)={
        use std::os::fd::AsRawFd;
        let mut fd=libc::pollfd {fd:_listener.as_raw_fd(),events:libc::POLLIN,revents:0};
        let result=unsafe {libc::poll(&mut fd,1,5)};
        (result,fd.revents,libc::POLLIN,result<0 && std::io::Error::last_os_error().raw_os_error()!=Some(libc::EINTR))
    };
    #[cfg(windows)]
    let (result,events,read,error)={
        use std::{ffi::c_void,os::windows::io::AsRawSocket};
        #[repr(C)] struct NetworkEvents {events:i32,errors:[i32;10]}
        #[link(name="kernel32")] unsafe extern "system" {fn WaitForSingleObject(event:*mut c_void,millis:u32)->u32;}
        #[link(name="Ws2_32")] unsafe extern "system" {fn WSAEnumNetworkEvents(socket:usize,event:*mut c_void,events:*mut NetworkEvents)->i32;}
        let event=self.event as *mut c_void;
        match unsafe {WaitForSingleObject(event,5)} {
            258=>(0,0,8,false),
            0=>{let mut events=NetworkEvents {events:0,errors:[0;10]};
                let status=unsafe {WSAEnumNetworkEvents(_listener.as_raw_socket() as usize,event,&mut events)};
                (1,events.events,8,status!=0||events.errors[3]!=0)},
            _=>(-1,0,8,true),
        }
    };
    if error || (result>0 && events&read==0) {return Err("telemetry IPC listener readiness failed".into());}
    Ok(result>0)
    }
}

#[cfg(windows)]
impl Drop for Readiness {
    fn drop(&mut self) {
        use std::{ffi::c_void,os::windows::io::AsRawSocket};
        #[link(name="Ws2_32")] unsafe extern "system" {
            fn WSAEventSelect(socket:usize,event:*mut c_void,events:i32)->i32;
            fn WSACloseEvent(event:*mut c_void)->i32;
        }
        // The owned listener remains valid through disassociation, including unwinding.
        unsafe {WSAEventSelect(self.listener.as_raw_socket() as usize,std::ptr::null_mut(),0);WSACloseEvent(self.event as *mut c_void);}
    }
}

fn setup(stream: &Stream) -> Res<()> {
    #[cfg(windows)]
    {
        use std::{ffi::c_void,os::windows::io::AsRawSocket};
        #[link(name="Ws2_32")] unsafe extern "system" {fn WSAEventSelect(socket:usize,event:*mut c_void,events:i32)->i32;}
        // Accepted sockets inherit the listener event; clear it before restoring blocking I/O.
        if unsafe {WSAEventSelect(stream.as_raw_socket() as usize,std::ptr::null_mut(),0)}!=0 {return Err("telemetry IPC blocking mode failed".into());}
    }
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

impl Client {
    fn connect(&self) -> Res<Stream> {
        #[cfg(unix)]
        let stream = crate::local_socket::connect(Path::new(&self.endpoint),Instant::now()+TIMEOUT)?;
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
        if events.len()>super::COMMIT_EVENTS||events.iter().map(|e|e.payload.len()).sum::<usize>()>super::COMMIT_BYTES {return Err("telemetry IPC commit group too large".into());}
        for chunk in chunks(events,self.max_events)? {
            let mut bytes=Vec::new();bytes.extend_from_slice(&(chunk.len() as u16).to_be_bytes());
            for event in chunk {
                bytes.extend_from_slice(&(event.payload.len() as u32).to_be_bytes());
                bytes.extend_from_slice(event.id.as_bytes());bytes.extend_from_slice(event.payload.as_bytes());
            }
            self.request(&bytes)?;
        }
        Ok(())
    }
}
// Older endpoints accept smaller batches; never cut a logical count summary between requests.
fn chunks(events:&[Event],limit:usize)->Res<Vec<&[Event]>> {
    let mut chunks=Vec::new();let mut start=0;
    while start<events.len() {
        let mut end=(start+limit).min(events.len());
        if end<events.len() {
            let next:Value=serde_json::from_str(&events[end].payload).map_err(|_|"telemetry IPC invalid observation")?;
            if let Some(index)=next["part_index"].as_u64() {
                end=end.checked_sub(index as usize).filter(|end|*end>start).ok_or("telemetry IPC endpoint cannot admit complete summary")?;
            }
        }
        chunks.push(&events[start..end]);start=end;
    }
    Ok(chunks)
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
    if count>super::COMMIT_EVENTS { return Err("telemetry IPC batch too large".into()); }
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
    if !events.is_empty() {store.append_group(&events)?;}
    Ok(crate::sha256_hex(&bytes))
}
fn database_id(path: &Path) -> Res<String> {
    let parent=path.parent().filter(|p|!p.as_os_str().is_empty()).unwrap_or_else(||Path::new("."));
    let name=path.file_name().ok_or("telemetry IPC database filename missing")?;
    let canonical=fs::canonicalize(parent).map_err(|_|"telemetry IPC database parent unavailable")?.join(name);
    #[cfg(unix)] {use std::os::unix::ffi::OsStrExt;Ok(crate::sha256_hex(canonical.as_os_str().as_bytes()))}
    #[cfg(windows)] {
        use std::os::windows::ffi::OsStrExt;
        let mut bytes=Vec::new();for unit in canonical.as_os_str().encode_wide() {bytes.extend_from_slice(&unit.to_le_bytes());}
        Ok(crate::sha256_hex(&bytes))
    }
}
#[cfg(test)]
fn read_client(dir: &Path, database_path: &Path) -> Res<Client> {read_bounded_client(dir,database_path,None)}
fn read_bounded_client(dir:&Path,database_path:&Path,budget:Option<&Budget>)->Res<Client> {
    let path=dir.join("endpoint");
    if fs::symlink_metadata(dir).map_err(|_|"telemetry IPC absent")?.file_type().is_symlink() || fs::symlink_metadata(&path).map_err(|_|"telemetry IPC absent")?.file_type().is_symlink() { return Err("telemetry IPC refuses symlinks".into()); }
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        if fs::metadata(dir).map_err(|_|"telemetry IPC absent")?.permissions().mode()&0o077!=0 || fs::metadata(&path).map_err(|_|"telemetry IPC absent")?.permissions().mode()&0o077!=0 { return Err("telemetry IPC state not protected".into()); }
    }
    let mut bytes=Vec::new();fs::File::open(&path).map_err(|_|"telemetry IPC absent")?.take(4097).read_to_end(&mut bytes).map_err(|_|"telemetry IPC state unreadable")?;
    if bytes.len()>4096 { return Err("telemetry IPC state oversized".into()); }
    let state: Value=serde_json::from_slice(&bytes).map_err(|_|"telemetry IPC state invalid")?;
    if state["wire"]!=2 {return Err("telemetry IPC unsupported wire version; quiesce the old writer".into());}
    let database=state["database"].as_str().ok_or("telemetry IPC database binding missing")?;
    if database!=database_id(database_path)? {return Err("telemetry IPC database mismatch".into());}
    let budget=budget.map(Budget::identity).transpose()?;
    if state["budget"]!=json!(budget) {return Err("telemetry IPC installation budget mismatch; quiesce incompatible writer".into());}
    let endpoint=state["endpoint"].as_str().ok_or("telemetry IPC endpoint missing")?.to_owned();
    #[cfg(unix)] {
        let expected=socket_path(dir,database);
        if Path::new(&endpoint)!=expected { return Err("telemetry IPC endpoint mismatch".into()); }
        if expected!=dir.join("s") {fallback_directory(&expected,false)?;}
    }
    let key: [u8;32]=serde_json::from_value(state["key"].clone()).map_err(|_|"telemetry IPC key invalid")?;
    let max_events=match state.get("max_events") {
        None=>super::BATCH,
        Some(value)=>value.as_u64().filter(|n|*n>=1&&*n<=super::COMMIT_EVENTS as u64).ok_or("telemetry IPC invalid group limit")? as usize,
    };
    Ok(Client { endpoint, key, max_events, database:database.to_owned(), budget })
}
fn publish(dir: &Path, client: &Client) -> Res<()> {
    let tmp=dir.join("endpoint.tmp");
    // The private directory and held election lock own this temporary file.
    if tmp.exists() { fs::remove_file(&tmp).map_err(|_|"telemetry IPC stale temporary state")?; }
    let mut options=fs::OpenOptions::new();options.write(true).create_new(true);
    #[cfg(unix)] { use std::os::unix::fs::OpenOptionsExt; options.mode(0o600); }
    let mut file=options.open(&tmp).map_err(|_|"telemetry IPC state create failed")?;
    file.write_all(json!({"wire":2,"database":client.database,"endpoint":client.endpoint,"key":client.key,"max_events":client.max_events,"budget":client.budget}).to_string().as_bytes()).and_then(|_|file.sync_all()).map_err(|_|"telemetry IPC state write failed")?;
    drop(file);
    #[cfg(windows)] if dir.join("endpoint").exists() { fs::remove_file(dir.join("endpoint")).map_err(|_|"telemetry IPC stale endpoint remove failed")?; }
    fs::rename(tmp,dir.join("endpoint")).map_err(|_|"telemetry IPC state publish failed".into())
}
fn protect_directory(dir: &Path) -> Res<()> {
    // Collector recovery must not enqueue observations about its own native helpers.
    tracing::dispatcher::with_default(&tracing::Dispatch::none(), || crate::enroll::protect(dir))
}

/// Fence writer bootstrap for quiesced maintenance; a live owner refuses the lock.
pub(super) fn maintenance_lock(path: &Path) -> Res<crate::file_lock::Lock> {
    let parent=path.parent().ok_or("telemetry IPC database parent missing")?;
    let dir=parent.join("telemetry-ipc");
    if fs::symlink_metadata(&dir).is_ok_and(|m|m.file_type().is_symlink()) {return Err("telemetry IPC refuses symlinks".into());}
    protect_directory(&dir).map_err(|e|format!("telemetry maintenance directory unavailable: {e}"))?;
    crate::file_lock::try_lock(&dir.join("writer.lock")).map_err(|error|match error {
        std::fs::TryLockError::WouldBlock=>"telemetry maintenance requires quiesced IPC writer".into(),
        _=>"telemetry maintenance writer election unavailable".into(),
    })
}

/// Connect first: sandbox clients need only read the endpoint and append over the transport.
#[cfg(test)]
pub(super) fn open(path: &Path) -> Res<Sink> {open_with_budget(path,None)}
pub(super) fn open_with_budget(path:&Path,budget:Option<Budget>)->Res<Sink> {
    let parent=path.parent().ok_or("telemetry IPC database parent missing")?;
    let dir: PathBuf=parent.join("telemetry-ipc");
    let mut state_error=match read_bounded_client(&dir,path,budget.as_ref()) {
        Ok(client)=>{if client.request(&[0,0]).is_ok() {return Ok(Sink::Remote(path.to_owned(),client,budget.clone()));} None},
        Err(error)=>Some(error),
    };
    if fs::symlink_metadata(&dir).is_ok_and(|m|m.file_type().is_symlink()) { return Err("telemetry IPC refuses symlinks".into()); }
    protect_directory(&dir).map_err(|e|format!("telemetry IPC writer directory unavailable: {e}"))?;
    let until=Instant::now()+TIMEOUT;
    let lock=loop {
        match crate::file_lock::try_lock(&dir.join("writer.lock")) {
            Ok(lock)=>break lock,
            Err(std::fs::TryLockError::WouldBlock) if Instant::now()<until=> {
                match read_bounded_client(&dir,path,budget.as_ref()) {
                    Ok(client)=>{if client.request(&[0,0]).is_ok() {return Ok(Sink::Remote(path.to_owned(),client,budget.clone()));}},
                    Err(error)=>state_error=Some(error),
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(std::fs::TryLockError::WouldBlock)=>return Err(state_error.unwrap_or_else(||"telemetry IPC writer election unavailable".into())),
            Err(_)=>return Err("telemetry IPC writer election unavailable".into()),
        }
    };
    let database=database_id(path)?;
    let store=Arc::new(Store::open_with_budget(path,budget.clone())?);
    #[cfg(unix)]
    let (listener,endpoint,socket)={
        use std::os::unix::fs::{FileTypeExt,MetadataExt,PermissionsExt};
        let path=socket_path(&dir,&database);let fallback=path!=dir.join("s");
        if fallback {fallback_directory(&path,true)?;}
        match fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_socket() && meta.uid()==unsafe {libc::geteuid()}=>
                fs::remove_file(&path).map_err(|_|"telemetry IPC stale socket remove failed")?,
            Ok(_)=>return Err("telemetry IPC stale socket identity invalid; unchanged".into()),
            Err(e) if e.kind()==std::io::ErrorKind::NotFound=>{},
            Err(_)=>return Err("telemetry IPC stale socket unreadable".into()),
        }
        let listener=Listener::bind(&path).map_err(|_|"telemetry IPC socket bind failed")?;
        let socket=Socket {path,fallback};
        fs::set_permissions(&socket.path,fs::Permissions::from_mode(0o600)).map_err(|_|"telemetry IPC socket not protected")?;
        let endpoint=socket.path.to_str().ok_or("telemetry IPC socket must be UTF-8")?.to_owned();
        (listener,endpoint,socket)
    };
    #[cfg(windows)]
    let (listener,endpoint)={let l=Listener::bind("127.0.0.1:0").map_err(|_|"telemetry IPC bind failed")?;let address=l.local_addr().map_err(|_|"telemetry IPC address failed")?.to_string();(l,address)};
    listener.set_nonblocking(true).map_err(|_|"telemetry IPC nonblocking failed")?;
    let readiness=Readiness::new(listener)?;
    #[cfg(target_os="macos")] let wake=readiness.queue.clone();
    let mut key=[0;32];SystemRandom::new().fill(&mut key).map_err(|_|"telemetry IPC key generation failed")?;
    let client=Client { endpoint,key,max_events:crate::telemetry::COMMIT_EVENTS,database,budget:budget.as_ref().map(Budget::identity).transpose()? };
    let stop=Arc::new(AtomicBool::new(false));let halted=stop.clone();let database=store.clone();
    let worker=std::thread::Builder::new().name("telemetry-local-writer".into()).spawn(move|| {
        let mut workers: Vec<JoinHandle<()>>=vec![];
        while !halted.load(Ordering::Acquire) {
            let mut i=0;while i<workers.len() { if workers[i].is_finished() { let _=workers.swap_remove(i).join(); } else { i+=1; } }
            if workers.len()<8 {
                match readiness.readable() {Ok(true)=>{},Ok(false)=>continue,Err(_)=>break}
                if halted.load(Ordering::Acquire) {break;}
                match readiness.listener.accept() {
                    Ok((stream,_))=>{let db=database.clone();workers.push(std::thread::spawn(move||{let _=serve(stream,&key,&db);}));}
                    Err(error) if error.kind()==std::io::ErrorKind::WouldBlock=>{},
                    Err(_)=>break,
                }
            } else { std::thread::sleep(Duration::from_millis(5)); }
        }
        for worker in workers { let _=worker.join(); }
    }).map_err(|_|"telemetry IPC writer thread failed")?;
    let server=Server { stop,worker:Some(worker),#[cfg(target_os="macos")] wake,#[cfg(unix)] socket:Some(socket),_lock:lock };
    publish(&dir,&client)?;
    Ok(Sink::Local { store, _server: server })
}

#[cfg(test)]
mod tests {
    #[cfg(target_os="macos")]
    #[test]
    fn explicit_shutdown_interrupts_the_idle_listener() {
        let dir=crate::scratch("telemetry-shutdown-wake");let listener=Listener::bind(dir.join("socket")).unwrap();listener.set_nonblocking(true).unwrap();
        let readiness=Readiness::new(listener).unwrap();let wake=readiness.queue.clone();let (send,receive)=std::sync::mpsc::channel();
        let worker=std::thread::spawn(move||{let result=readiness.readable();send.send(result).unwrap();});
        wake_listener(&wake).unwrap();assert_eq!(receive.recv_timeout(Duration::from_millis(800)).unwrap(),Ok(false));worker.join().unwrap();std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn listener_waits_for_a_pending_connection_without_delaying_it() {
        let dir=crate::scratch("telemetry-readiness");
        #[cfg(unix)] let listener=Listener::bind(dir.join("socket")).unwrap();
        #[cfg(windows)] let listener=Listener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        #[cfg(windows)] let address=listener.local_addr().unwrap();
        let readiness=Readiness::new(listener).unwrap();assert!(!readiness.readable().unwrap());
        // A queued backlog and a later arrival both rearm after each nonblocking accept.
        for count in [2,1] {
            let peers:Vec<_>=(0..count).map(|_| {
                #[cfg(unix)] {crate::local_socket::connect(&dir.join("socket"),Instant::now()+TIMEOUT).unwrap()}
                #[cfg(windows)] {Stream::connect_timeout(&address,TIMEOUT).unwrap()}
            }).collect();
            for _ in 0..count {assert!(readiness.readable().unwrap());let (stream,_)=readiness.listener.accept().unwrap();setup(&stream).unwrap();drop(stream);}
            assert!(!readiness.readable().unwrap());drop(peers);
        }
        drop(readiness);std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn bounded_client_refuses_an_incompatible_resident_writer() {
        let dir=crate::scratch("telemetry-budget-ipc");let path=dir.join("db");
        let budget=Budget {lock:dir.join("installation.lock"),peers:vec![dir.join("archive")],total:8<<20,reserved:1<<20};
        let mut owner=open_with_budget(&path,Some(budget.clone())).unwrap();
        let mut client=open_with_budget(&path,Some(budget.clone())).unwrap();
        client.append(&[event()]).unwrap();
        {let _lock=budget.acquire().unwrap();std::fs::File::create(dir.join("archive")).unwrap().set_len(7<<20).unwrap();}
        assert_eq!(client.append(&[event()]).unwrap_err(),"telemetry IPC rejected: budget-exhausted");
        {let _lock=budget.acquire().unwrap();std::fs::remove_file(dir.join("archive")).unwrap();}
        assert!(read_bounded_client(&dir.join("telemetry-ipc"),&path,None).err().unwrap().contains("budget mismatch"));
        let mut different=budget.clone();different.reserved+=1;
        assert!(read_bounded_client(&dir.join("telemetry-ipc"),&path,Some(&different)).err().unwrap().contains("budget mismatch"));
        drop(owner);
        owner=open_with_budget(&path,Some(budget)).unwrap();client.append(&[event()]).unwrap();
        drop(client);drop(owner);std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn bounded_client_accepts_the_same_budget_through_a_directory_alias() {
        let install=crate::scratch("telemetry-budget-alias");let path=install.join("telemetry.log/universal.sqlite3");
        let alias=install.join("alias");std::fs::create_dir(&alias).unwrap();let alias=alias.join("..");
        let budget=|root:PathBuf| Budget {lock:root.join("taskq-profile.lock"),peers:vec![root.join("telemetry.log/telemetry.sqlite3"),root.join("telemetry.log/telemetry.sqlite3-wal"),root.join("telemetry.log/telemetry.sqlite3-shm"),root.join("taskq.log")],total:64<<20,reserved:6<<20};
        let owner=open_with_budget(&path,Some(budget(std::fs::canonicalize(&install).unwrap()))).unwrap();
        let mut client=open_with_budget(&path,Some(budget(alias))).unwrap();client.append(&[event()]).unwrap();
        drop(client);drop(owner);std::fs::remove_dir_all(install).unwrap();
    }

    #[test]
    fn transport_batches_keep_summary_parts_together() {
        let base=json!({"product":"fixtureq","version":"1","operation":"summary","kind":"requests",
            "trace":"trace","span":"span","started_ms":1,"outcome":"ok","error":"none"});
        let single=Event::new(base.clone()).unwrap();let mut value=base;
        value["counts"]=json!((0..1000).map(|i|(format!("route{i}"),u64::MAX)).collect::<std::collections::BTreeMap<_,_>>());
        let parts=Event::split(value).unwrap();assert!(parts.len()>1);
        let mut events=vec![single.clone();1023];events.extend(parts.clone());events.push(single);
        let batches=chunks(&events,1024).unwrap();assert_eq!(batches[0].len(),1023);
        assert_eq!(batches[1].len(),parts.len()+1);
        assert_eq!(batches[1][0].id(),parts[0].id());
        assert!(chunks(&parts,1).err().unwrap().contains("complete summary"));
    }

    #[cfg(windows)]
    #[test]
    fn native_protection_is_observed_except_inside_collector_bootstrap() {
        use tracing_subscriber::prelude::*;
        let dir=crate::scratch("ipc-protect-capture");
        let store=Arc::new(Store::open(&dir.join("capture")).unwrap());
        let capture=super::super::capture::Capture::new(store.clone(),"fixtureq","1").unwrap();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()),|| {
            let owner=open(&dir.join("db")).unwrap();
            assert_eq!(capture.health()["accepted"],0,"collector bootstrap observed itself");
            crate::enroll::protect(&dir.join("app")).unwrap();
            drop(owner);
        });
        capture.flush().unwrap();
        let rows=store.export(0,0,1024).unwrap();
        for name in ["fixtureq.operation.storage.identity","fixtureq.operation.storage.protect"] {
            assert!(rows["events"].as_array().unwrap().iter().any(|r|r["observation"]["operation"]==name && r["observation"]["kind"]=="method" && r["observation"]["outcome"]=="ok"),"{rows}");
        }
        assert!(!rows.to_string().contains(&dir.to_string_lossy().replace('\\',"\\\\")),"native paths entered telemetry");
        assert_eq!(capture.health()["lost"],0);
        drop(capture);drop(store);fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn maintenance_fences_new_writer_bootstrap_without_creating_the_database() {
        let dir=crate::scratch("ipc-maintenance");let path=dir.join("db");
        let guard=crate::telemetry::maintenance(&path).unwrap();
        assert!(open(&path).is_err());assert!(!path.exists());
        drop(guard);let mut owner=open(&path).unwrap();owner.append(&[event()]).unwrap();
        assert!(crate::telemetry::maintenance(&path).is_err(),"maintenance stole a live writer");
        assert_eq!(Store::open(&path).unwrap().export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),1);
        drop(owner);fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn database_selection_cannot_route_to_another_live_store_or_after_handover() {
        let dir=crate::scratch("ipc-database-binding");let first=dir.join("first.sqlite3");let second=dir.join("second.sqlite3");
        let owner=open(&first).unwrap();let mut previous=open(&first).unwrap();
        let mut different=open(&second);
        if let Ok(sink)=&mut different {sink.append(&[event()]).unwrap();}
        assert!(Store::open(&first).unwrap().export(0,0,1024).unwrap()["events"].as_array().unwrap().is_empty(),"second database request contaminated first store");
        assert!(different.is_err(),"live writer for another database must be refused");assert!(!second.exists());
        drop(owner);let mut next=open(&second).unwrap();next.append(&[event()]).unwrap();
        assert!(previous.append(&[event()]).is_err(),"cached first-store client must not attach to second-store owner");
        assert_eq!(Store::open(&second).unwrap().export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),1);
        drop(previous);drop(next);fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unbound_legacy_or_changed_endpoint_identity_is_refused_until_writer_quiesces() {
        let dir=crate::scratch("ipc-unbound");let path=dir.join("db");let owner=open(&path).unwrap();
        let ipc=dir.join("telemetry-ipc");let endpoint=ipc.join("endpoint");let original:Value=serde_json::from_slice(&fs::read(&endpoint).unwrap()).unwrap();
        assert_eq!(original["wire"],2);assert_eq!(original["database"],database_id(&path).unwrap());
        for fault in ["legacy","missing","different"] {
            let mut state=original.clone();
            match fault {"legacy"=>state["wire"]=json!(1),"missing"=>{state.as_object_mut().unwrap().remove("database");},_=>state["database"]=json!("f".repeat(64))}
            fs::write(&endpoint,state.to_string()).unwrap();
            assert!(read_client(&ipc,&path).is_err());
            let error=open(&path).err().expect("must refuse unbound writer");
            assert!(error.contains(if fault=="legacy" {"quiesce"} else {"database"}),"{error}");
            assert!(Store::open(&path).unwrap().export(0,0,1024).unwrap()["events"].as_array().unwrap().is_empty());
        }
        fs::write(&endpoint,original.to_string()).unwrap();drop(owner);
        let mut replacement=open(&path).unwrap();replacement.append(&[event()]).unwrap();
        assert_eq!(Store::open(&path).unwrap().export(0,0,1024).unwrap()["events"].as_array().unwrap().len(),1);
        drop(replacement);fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn database_binding_does_not_collapse_distinct_native_filenames() {
        let dir=crate::scratch("ipc-native-name");
        #[cfg(unix)] let (first,second)={
            use std::os::unix::ffi::OsStringExt;
            (std::ffi::OsString::from_vec(vec![b'd',0xff]),std::ffi::OsString::from_vec(vec![b'd',0xfe]))
        };
        #[cfg(windows)] let (first,second)={
            use std::os::windows::ffi::OsStringExt;
            (std::ffi::OsString::from_wide(&[b'd' as u16,0xd800]),std::ffi::OsString::from_wide(&[b'd' as u16,0xd801]))
        };
        assert_eq!(first.to_string_lossy(),second.to_string_lossy());
        let first=dir.join(first);let second=dir.join(second);
        assert_ne!(database_id(&first).unwrap(),database_id(&second).unwrap());
        assert!(!first.exists()&&!second.exists(),"identity must not open or create SQLite files");
        fs::remove_dir_all(dir).unwrap();
    }
    use super::*;
    // Chain: deep installation path -> authenticated append -> writer restart -> same database readback.
    #[cfg(unix)]
    #[test]
    fn long_installation_path_keeps_authenticated_ipc_and_recovery() {
        let base=crate::scratch("ipc-long");let dir=base.join("deep".repeat(40));fs::create_dir(&dir).unwrap();
        let path=dir.join("db");let owner=open(&path).unwrap();let mut client=open(&path).unwrap();
        let first=event();client.append(&[first.clone()]).unwrap();
        let state=read_client(&dir.join("telemetry-ipc"),&path).unwrap();
        let socket=PathBuf::from(&state.endpoint);
        assert!(state.endpoint.len()<104,"native socket address must remain bounded");
        let mut unauthenticated=state.clone();unauthenticated.key=[0;32];
        assert!(unauthenticated.request(&[0,0]).is_err());
        drop(owner);
        let replacement=open(&path).unwrap();let second=event();client.append(&[second.clone()]).unwrap();
        if let Sink::Local{store,..}=&replacement {
            let rows=store.export(0,0,32).unwrap();let rows=rows["events"].as_array().unwrap();
            assert_eq!(rows.len(),2);assert!(rows.iter().any(|r|r["id"]==first.id()));assert!(rows.iter().any(|r|r["id"]==second.id()));
        } else {panic!("replacement must own the writer");}
        drop(client);drop(replacement);
        assert!(!socket.exists(),"normal shutdown must retire the owned socket");
        use std::os::unix::fs::{PermissionsExt,symlink};
        let parent=socket.parent().unwrap();
        fs::create_dir(parent).unwrap();fs::set_permissions(parent,fs::Permissions::from_mode(0o777)).unwrap();
        assert!(open(&path).err().unwrap().contains("not protected"));
        assert_eq!(fs::metadata(parent).unwrap().permissions().mode()&0o777,0o777,"refusal must not rewrite existing permissions");
        fs::remove_dir(parent).unwrap();
        symlink(&base,parent).unwrap();assert!(open(&path).err().unwrap().contains("not protected"));
        assert!(fs::symlink_metadata(parent).unwrap().file_type().is_symlink());fs::remove_file(parent).unwrap();
        fallback_directory(&socket,true).unwrap();fs::write(&socket,"preserve foreign file").unwrap();
        assert!(open(&path).err().unwrap().contains("identity invalid"));
        assert_eq!(fs::read_to_string(&socket).unwrap(),"preserve foreign file");
        fs::remove_file(&socket).unwrap();fs::remove_dir(parent).unwrap();
        fs::remove_dir_all(base).unwrap();
    }
    fn event() -> Event { Event::new(json!({"product":"fixtureq","version":"1.0.0","operation":"method.call","kind":"method","trace":"trace","span":"span","started_ms":1,"duration_us":1,"outcome":"ok","error":"none"})).unwrap() }

    #[test]
    fn additive_group_limit_keeps_legacy_endpoint_batches_and_validates_bounds() {
        let dir=crate::scratch("ipc-legacy-group");let path=dir.join("db");let owner=open(&path).unwrap();
        let endpoint=dir.join("telemetry-ipc/endpoint");let mut state: Value=serde_json::from_slice(&fs::read(&endpoint).unwrap()).unwrap();
        assert_eq!(state["max_events"],crate::telemetry::COMMIT_EVENTS);
        for invalid in [json!(0),json!(8193),json!("8192")] {
            state["max_events"]=invalid;fs::write(&endpoint,state.to_string()).unwrap();assert!(read_client(&dir.join("telemetry-ipc"),&path).is_err());
        }
        state.as_object_mut().unwrap().remove("max_events");fs::write(&endpoint,state.to_string()).unwrap();
        let client=read_client(&dir.join("telemetry-ipc"),&path).unwrap();assert_eq!(client.max_events,crate::telemetry::BATCH);
        let events=(0..1100).map(|_|event()).collect::<Vec<_>>();client.append(&events).unwrap();client.append(&events).unwrap();
        let store=Store::open(&path).unwrap();let page=store.export(0,0,1024).unwrap();
        assert_eq!(page["events"].as_array().unwrap().len(),1024);
        assert_eq!(store.export(0,page["next"].as_i64().unwrap(),1024).unwrap()["events"].as_array().unwrap().len(),76);
        drop(store);drop(owner);fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn clients_append_idempotently_fail_closed_and_replace_dead_writer() {
        let dir=crate::scratch("ipc"); let path=dir.join("db");
        let owner=open(&path).unwrap();
        let client=read_client(&dir.join("telemetry-ipc"),&path).unwrap();
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
        let next=read_client(&dir.join("telemetry-ipc"),&path).unwrap();assert!(client.key!=next.key);
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
        let client=read_client(&dir.join("telemetry-ipc"),&path).unwrap();let mut stream=client.connect().unwrap();
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
        let client=Client {endpoint,key:[0xaa;32],max_events:crate::telemetry::COMMIT_EVENTS,database:"0".repeat(64),budget:None};
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
        let client=Client {endpoint,key:[0xaa;32],max_events:crate::telemetry::COMMIT_EVENTS,database:"0".repeat(64),budget:None};
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
        for _ in 0..8 { match crate::local_socket::connect(&socket,Instant::now()+TIMEOUT) { Ok(stream)=>held.push(stream), Err(_)=>{refused=true;break;} } }
        assert!(refused,"the bounded accept queue was not saturated");
        assert!(began.elapsed()<Duration::from_secs(2));
        drop(held);drop(listener);fs::remove_dir_all(dir).unwrap();
    }
}
