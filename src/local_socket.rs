//! Shared Unix connection deadline; callers own peer trust and wire semantics.
use std::{os::unix::net::UnixStream as Stream,time::Instant};

#[cfg(unix)]
pub fn connect(path: &std::path::Path, until: Instant) -> Result<Stream,String> {
    use std::os::unix::ffi::OsStrExt;
    if until<=Instant::now() {return Err("local socket connect deadline".into());}
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    // std UnixStream has no connect deadline; nonblocking connect bounds a saturated accept queue.
    let mut address: libc::sockaddr_un=unsafe { std::mem::zeroed() };
    address.sun_family=libc::AF_UNIX as _;
    let bytes=path.as_os_str().as_bytes();
    if bytes.contains(&0)||bytes.len()>=address.sun_path.len() { return Err("local socket invalid socket path".into()); }
    for (to,from) in address.sun_path.iter_mut().zip(bytes) { *to=*from as _; }
    #[cfg(target_os="macos")] { address.sun_len=std::mem::size_of_val(&address) as _; }
    let fd=unsafe { libc::socket(libc::AF_UNIX,libc::SOCK_STREAM,0) };
    if fd<0 { return Err("local socket socket create failed".into()); }
    let owned=unsafe { OwnedFd::from_raw_fd(fd) };
    if unsafe { libc::fcntl(fd,libc::F_SETFD,libc::FD_CLOEXEC) }<0 || unsafe { libc::fcntl(fd,libc::F_SETFL,libc::O_NONBLOCK) }<0 { return Err("local socket socket setup failed".into()); }
    let connected=unsafe { libc::connect(fd,(&address as *const libc::sockaddr_un).cast(),std::mem::size_of_val(&address) as _) };
    if connected!=0 {
        let error=std::io::Error::last_os_error().raw_os_error();
        if error!=Some(libc::EINPROGRESS) && error!=Some(libc::EWOULDBLOCK) { return Err("local socket unavailable".into()); }
        // A full Unix accept queue may return EAGAIN without starting a connection.
        if error==Some(libc::EWOULDBLOCK) { return Err("local socket accept queue full".into()); }
        loop {
            let left=until.saturating_duration_since(Instant::now());
            if left.is_zero() { return Err("local socket connect deadline".into()); }
            let mut poll=libc::pollfd {fd:owned.as_raw_fd(),events:libc::POLLOUT,revents:0};
            let ready=unsafe { libc::poll(&mut poll,1,left.as_millis().max(1).min(i32::MAX as u128) as _) };
            if ready>0 { break; }
            if ready<0 && std::io::Error::last_os_error().raw_os_error()!=Some(libc::EINTR) { return Err("local socket connect failed".into()); }
        }
        let mut error: libc::c_int=0;let mut length=std::mem::size_of_val(&error) as libc::socklen_t;
        if unsafe { libc::getsockopt(fd,libc::SOL_SOCKET,libc::SO_ERROR,(&mut error as *mut libc::c_int).cast(),&mut length) }!=0 || error!=0 { return Err("local socket unavailable".into()); }
    }
    let stream=Stream::from(owned);stream.set_nonblocking(false).map_err(|_|"local socket socket mode failed")?;
    Ok(stream)
}
