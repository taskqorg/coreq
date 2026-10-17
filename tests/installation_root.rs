use std::{fs,path::PathBuf,process::Command,time::{Duration,SystemTime,UNIX_EPOCH}};

// Chain: OS-like environment -> installed entrypoint -> one process root, or refusal before another root is used.
#[test]
fn installed_entrypoints_keep_their_root_without_ambient_environment() {
    let base=std::env::temp_dir().join(format!("coreq-install-root-{}-{}",std::process::id(),SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
    fs::create_dir(&base).unwrap();
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
    let _cleanup=Cleanup(base.clone());
    let primary=format!("fixtureq{}",std::env::consts::EXE_SUFFIX);
    let mut modes=vec!["source","launcher","release","old","old-numbered","bundle","alias","foreign","bad-current","oversized-current","directory-current","no-current","missing-release","stage","stage-explicit","cache","relative","explicit"];
    if cfg!(unix) {modes.push("smoke");}
    for mode in modes {
        let directory=base.join(mode);let install=directory.join("selected install");let home=directory.join("home");let local=directory.join("local");
        fs::create_dir_all(install.join("releases/1.0.0")).unwrap();fs::create_dir(&home).unwrap();fs::create_dir(&local).unwrap();
        fs::write(install.join("current"),"1.0.0\n").unwrap();fs::write(install.join("releases/1.0.0").join(&primary),"owned primary").unwrap();
        let executable=match mode {
            "source"|"smoke"=>std::env::current_exe().unwrap(),
            "release"=>install.join("releases/1.0.0").join(&primary),
            "stage"|"stage-explicit"=>install.join("releases/2.0.0.tmp").join(&primary),
            "old"=>install.join(format!("{primary}.old")),
            "old-numbered"=>install.join(format!("{primary}.old.7")),
            "bundle"=>install.join("FixtureQ.app/Contents/MacOS").join(&primary),
            "alias"=>install.join(format!("fixtureq-tray{}",std::env::consts::EXE_SUFFIX)),
            "foreign"=>install.join(format!("unrelated-helper{}",std::env::consts::EXE_SUFFIX)),
            _=>install.join(&primary),
        };
        if mode!="source"&&mode!="smoke" {fs::create_dir_all(executable.parent().unwrap()).unwrap();fs::copy(std::env::current_exe().unwrap(),&executable).unwrap();}
        if mode=="bad-current"||mode=="explicit" {fs::write(install.join("current"),"../other").unwrap();}
        if mode=="oversized-current" {fs::write(install.join("current"),"1".repeat(129)).unwrap();}
        if mode=="directory-current" {fs::remove_file(install.join("current")).unwrap();fs::create_dir(install.join("current")).unwrap();}
        if mode=="no-current"||mode=="stage-explicit"||mode=="smoke" {fs::remove_file(install.join("current")).unwrap();}
        if mode=="missing-release" {fs::remove_file(install.join("releases/1.0.0").join(&primary)).unwrap();}
        let report=directory.join("report.json");
        let mut command=Command::new(executable);
        command.args(["--exact","root_child","--ignored","--nocapture"])
            .current_dir(&directory).env("HOME",&home).env("LOCALAPPDATA",&local).env_remove("FIXTUREQ_INSTALL")
            .env("COREQ_ROOT_REPORT",&report).env("COREQ_ROOT_MODE",mode).env("COREQ_ROOT_EXPECT_INSTALL",&install);
        if mode=="explicit" {command.env("FIXTUREQ_INSTALL",directory.join("override"));}
        if mode=="stage-explicit" {command.env("FIXTUREQ_INSTALL",&install);}
        if mode=="relative" {command.env("FIXTUREQ_INSTALL","relative install");}
        #[cfg(unix)]
        if mode=="smoke" {
            use std::os::unix::fs::PermissionsExt;
            let downloads=directory.join("downloads");let tools=directory.join("tools");fs::create_dir(&downloads).unwrap();fs::create_dir(&tools).unwrap();
            let candidate=b"#!/bin/sh\n[ \"$1\" = selftest ] && [ \"$FIXTUREQ_SELFTEST_OFFLINE\" = 1 ] && [ \"$FIXTUREQ_INSTALL\" = \"$COREQ_ROOT_EXPECT_INSTALL\" ] && [ ! -e \"$FIXTUREQ_INSTALL/current\" ] || exit 9\nprintf '%s' \"$FIXTUREQ_INSTALL\" > \"$COREQ_ROOT_SMOKE_REPORT\"\n";
            let file=format!("fixtureq-2.0.0-{}",coreq::update::target());fs::write(downloads.join(&file),candidate).unwrap();
            let sha=ring::digest::digest(&ring::digest::SHA256,candidate).as_ref().iter().map(|v|format!("{v:02x}")).collect::<String>();
            fs::write(downloads.join("SHA256SUMS"),format!("{sha}  {file}\n")).unwrap();
            fs::write(downloads.join("latest.json"),serde_json::json!({"version":"2.0.0","files":{coreq::update::target():file}}).to_string()).unwrap();
            let curl=tools.join("curl");fs::write(&curl,"#!/bin/sh\n[ \"$1 $2 $3 $4\" = '-fsSL --max-time 300 -o' ] || exit 9\nexec /bin/cp \"$COREQ_ROOT_DOWNLOADS/${6##*/}\" \"$5\"\n").unwrap();fs::set_permissions(&curl,fs::Permissions::from_mode(0o700)).unwrap();
            command.env("PATH",format!("{}:/usr/bin:/bin",tools.display())).env("COREQ_ROOT_DOWNLOADS",downloads).env("COREQ_ROOT_SMOKE_REPORT",directory.join("smoke-root"));
        }
        let output=match coreq::process::output("test.install-root",&mut command,Duration::from_secs(15)) {
            Ok(output)|Err(coreq::process::Failure::Exit(output))=>output,Err(error)=>panic!("{mode}: {error}"),
        };
        let refused=matches!(mode,"bad-current"|"oversized-current"|"directory-current"|"no-current"|"missing-release"|"stage");
        if refused {
            assert_eq!(output.status.code(),Some(1),"{mode}: {output:?}");assert!(!report.exists(),"refused layout selected a root");
            assert!(String::from_utf8_lossy(&output.stderr).contains("installation"),"{output:?}");
        } else {
            assert!(output.status.success(),"{mode}: {output:?}");
            let actual:PathBuf=serde_json::from_slice(&fs::read(&report).unwrap()).unwrap();
            let expected=if mode=="explicit" {directory.join("override")}
                else if mode=="stage-explicit"||mode=="smoke" {install.clone()}
                else if mode=="relative" {directory.canonicalize().unwrap().join("relative install")}
                else if mode=="source"||mode=="foreign" {if cfg!(windows) {local.join("fixtureq")}else{home.join(".local/share/fixtureq")}}
                else {install.canonicalize().unwrap()};
            assert_eq!(actual,expected,"{mode}: entrypoint used another installation");
        }
        assert!(!home.join(".local").exists()&&!local.join("fixtureq").exists(),"resolution wrote to a fallback installation");
        fs::remove_dir_all(directory).unwrap();
    }
    fs::remove_dir(base).unwrap();
}

#[test]
#[ignore="owned installation-root subprocess; exercised by installed_entrypoints_keep_their_root_without_ambient_environment"]
fn root_child() {
    #[cfg(unix)]
    if std::env::var("COREQ_ROOT_MODE").as_deref()==Ok("smoke") {
        let install=PathBuf::from(std::env::var_os("COREQ_ROOT_EXPECT_INSTALL").unwrap());
        let tool=coreq::Tool {name:"fixtureq",title:"FixtureQ",repo:"fixture/unused",version:"0.9.0",install:install.clone(),fault:None};
        assert_eq!(coreq::update::update(&tool,false).unwrap().as_deref(),Some("2.0.0"));
        assert_eq!(fs::read_to_string(std::env::var_os("COREQ_ROOT_SMOKE_REPORT").unwrap()).unwrap(),install.to_str().unwrap());
        fs::write(std::env::var_os("COREQ_ROOT_REPORT").unwrap(),serde_json::to_vec(&install).unwrap()).unwrap();return;
    }
    let alias=format!("fixtureq-tray{}",std::env::consts::EXE_SUFFIX);
    let install=if std::env::var("COREQ_ROOT_MODE").as_deref()==Ok("alias") {coreq::install_dir("fixtureq","FixtureQ",&[&alias]).unwrap()}
        else {coreq::Tool::new("fixtureq","FixtureQ","fixture/unused","1.0.0").install};
    if std::env::var("COREQ_ROOT_MODE").as_deref()==Ok("cache") {
        fs::write(install.join("current"),"changed-after-root-selection").unwrap();
        assert_eq!(coreq::Tool::new("fixtureq","FixtureQ","fixture/unused","1.0.0").install,install,"process root was reselected");
    }
    if std::env::var("COREQ_ROOT_MODE").as_deref()==Ok("relative") {
        std::env::set_current_dir(std::env::temp_dir()).unwrap();
        assert_eq!(coreq::Tool::new("fixtureq","FixtureQ","fixture/unused","1.0.0").install,install,"relative root followed a later cwd");
    }
    fs::write(std::env::var_os("COREQ_ROOT_REPORT").unwrap(),serde_json::to_vec(&install).unwrap()).unwrap();
}
