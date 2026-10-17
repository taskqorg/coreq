use std::{fs,io::{BufRead,BufReader,Write},process::{Command,Stdio}};
const COMMITTED:&[u8]=b"committed\0\xff";

#[test]
fn acknowledged_file_survives_writer_termination_and_refuses_unowned_temporary() {
    let dir=std::env::temp_dir().join(format!("coreq-durable-{}",std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let path=dir.join("state");
    coreq::write_durable(&path,"old").unwrap();
    let mut child=Command::new(std::env::current_exe().unwrap())
        .args(["--exact","durable_writer","--ignored","--nocapture"])
        .env("COREQ_DURABLE_FIXTURE",&path).stdout(Stdio::piped()).spawn().unwrap();
    let output=child.stdout.take().unwrap();
    let (tx,rx)=std::sync::mpsc::channel();
    let reader=std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            if line.unwrap()=="ACK" { let _=tx.send(());break; }
        }
    });
    let acknowledged=rx.recv_timeout(std::time::Duration::from_secs(10));
    let _=child.kill();child.wait().unwrap();reader.join().unwrap();
    acknowledged.unwrap();
    assert_eq!(fs::read(&path).unwrap(),COMMITTED);
    assert!(!dir.join("state.pending").exists());
    fs::write(dir.join("state.pending"),"interrupted").unwrap();
    assert!(coreq::write_durable(&path,"next").is_err());
    assert_eq!(fs::read(&path).unwrap(),COMMITTED);
    assert_eq!(fs::read_to_string(dir.join("state.pending")).unwrap(),"interrupted");
    let target=dir.join("directory");fs::create_dir(&target).unwrap();
    assert!(coreq::write_durable(&target,"refused").is_err());
    assert!(target.is_dir());assert!(!dir.join("directory.pending").exists());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[ignore]
fn durable_writer() {
    let path=std::env::var_os("COREQ_DURABLE_FIXTURE").unwrap();
    coreq::write_durable(std::path::Path::new(&path),COMMITTED).unwrap();
    println!("ACK");std::io::stdout().flush().unwrap();
    loop { std::thread::park(); }
}
