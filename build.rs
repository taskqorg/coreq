fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    coreq_instrument::generate(
        &root.join("tests/fixtures/telemetry"),
        "application.rs",
        &std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap()).join("telemetry-fixture"),
    )
    .expect("telemetry source instrumentation");
}
