//! Release trust: no candidate executes before provenance; unframed downloads cannot fill disk.
#![cfg(unix)]
use std::{fs,path::Path,process::Command};
use std::os::unix::fs::PermissionsExt;
fn executable(path:&Path,body:&str) {fs::write(path,body).unwrap();fs::set_permissions(path,fs::Permissions::from_mode(0o700)).unwrap();}
#[test]
fn attestation_precedes_execution_and_streaming_downloads_are_bounded() {
    let root=std::env::temp_dir().join(format!("coreq-update-trust-{}",std::process::id()));
    fs::create_dir(&root).unwrap();let bin=root.join("bin");let release=root.join("release");let install=root.join("install");
    for p in [&bin,&release,&install] {fs::create_dir(p).unwrap();}
    fs::write(install.join("current"),"1.0.0").unwrap();fs::create_dir_all(install.join("releases/1.0.0")).unwrap();
    fs::write(install.join("releases/1.0.0/fixtureq"),"old").unwrap();
    let name=format!("fixtureq-2.0.0-{}",coreq::update::target());
    let bytes=b"#!/bin/sh\nprintf executed > \"$COREQ_TRUST_MARKER\"\nexit 0\n";
    fs::write(release.join(&name),bytes).unwrap();
    let digest:String=ring::digest::digest(&ring::digest::SHA256,bytes).as_ref().iter().map(|b|format!("{b:02x}")).collect();
    fs::write(release.join("SHA256SUMS"),format!("{digest}  {name}\n")).unwrap();
    let manifest=serde_json::json!({"version":"2.0.0","files":{coreq::update::target():name}}).to_string();
    fs::write(release.join("latest.json"),&manifest).unwrap();
    executable(&bin.join("curl"),"#!/bin/sh\nwhile [ $# -gt 1 ]; do shift; done\nexec /bin/cat \"$COREQ_TRUST_RELEASE/${1##*/}\"\n");
    executable(&bin.join("gh"),"#!/bin/sh\nexit 1\n");
    fs::write(release.join("provenance.sigstore.jsonl"),"invented test bundle verified by the fake gh").unwrap();
    let marker=root.join("executed");
    let run=|expected:&str|Command::new(std::env::current_exe().unwrap()).args(["--exact","trust_child","--ignored","--nocapture"])
        .env("PATH",format!("{}:/usr/bin:/bin",bin.display())).env("COREQ_TRUST_INSTALL",&install).env("COREQ_TRUST_RELEASE",&release)
        .env("COREQ_TRUST_MARKER",&marker).env("COREQ_TRUST_EXPECT",expected).output().unwrap();
    let result=run("reject");assert!(result.status.success(),"{}",String::from_utf8_lossy(&result.stderr));
    assert!(!marker.exists());assert_eq!(fs::read_to_string(install.join("current")).unwrap(),"1.0.0");
    assert!(!install.join("releases/2.0.0.tmp").exists());
    fs::write(release.join("latest.json"),vec![b' ';(1<<20)+8192]).unwrap();
    let result=run("oversize");assert!(result.status.success(),"{}",String::from_utf8_lossy(&result.stderr));
    assert!(!install.join("releases/latest.json.tmp").exists());assert!(!marker.exists());
    fs::write(release.join("latest.json"),manifest).unwrap();executable(&bin.join("gh"),"#!/bin/sh\nexit 0\n");
    let result=run("accept");assert!(result.status.success(),"{}",String::from_utf8_lossy(&result.stderr));
    assert_eq!(fs::read_to_string(marker).unwrap(),"executed");
    assert_eq!(fs::read_to_string(install.join("current")).unwrap(),"2.0.0");
    fs::remove_dir_all(root).unwrap();
}
#[test]
#[ignore="owned child of attestation_precedes_execution_and_streaming_downloads_are_bounded"]
fn trust_child() {
    let mut tool=coreq::Tool::new("fixtureq","FixtureQ","taskqorg/fixtureq","1.0.0");
    tool.install=std::env::var_os("COREQ_TRUST_INSTALL").unwrap().into();
    let result=coreq::update::update_attested(&tool,false);
    match std::env::var("COREQ_TRUST_EXPECT").unwrap().as_str() {
        "accept"=>assert_eq!(result.unwrap().as_deref(),Some("2.0.0")),
        "oversize"=>assert!(result.unwrap_err().contains("output-limit")),
        _=>assert!(result.unwrap_err().contains("provenance not verified")),
    }
}
