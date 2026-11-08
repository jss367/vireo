fn main() {
    // Release builds use the exact stamp embedded in the Python sidecar.
    // Debug builds run from source and do not require a sidecar build.
    let stamp = std::path::Path::new("../build/build-commit.txt");
    let commit = if std::env::var("PROFILE").as_deref() == Ok("release") {
        println!("cargo:rerun-if-changed={}", stamp.display());
        std::fs::read_to_string(stamp).expect("build the sidecar before the desktop app")
    } else {
        // Watching HEAD alone misses new commits on an existing branch.
        for name in ["HEAD", "refs"] {
            let args = ["rev-parse", "--git-path", name];
            if let Ok(output) = std::process::Command::new("git").args(args).output() {
                println!(
                    "cargo:rerun-if-changed={}",
                    String::from_utf8_lossy(&output.stdout).trim()
                );
            }
        }
        std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
            .unwrap_or_else(|| "unknown".to_owned())
    };
    let commit = commit.trim();
    assert!(
        commit == "unknown"
            || (commit.len() == 40 && commit.bytes().all(|c| c.is_ascii_hexdigit())),
        "invalid build commit stamp"
    );
    println!("cargo:rustc-env=VIREO_BUILD_COMMIT={commit}");
    // Debug builds (including `cargo test`) do not spawn the bundled Python
    // sidecar.  Keeping the release-only external binary in Tauri's build
    // configuration made otherwise self-contained Rust unit tests fail before
    // compilation whenever a developer had not run the PyInstaller build.
    // Strip only that bundle input for debug profiles; release builds still
    // require and validate the real sidecar artifact.
    if std::env::var("PROFILE").as_deref() != Ok("release")
        && std::env::var_os("TAURI_CONFIG").is_none()
    {
        std::env::set_var("TAURI_CONFIG", r#"{"bundle":{"externalBin":[]}}"#);
    }
    let windows = tauri_build::WindowsAttributes::new()
        .app_manifest(include_str!("windows-app-manifest.xml"));
    let attributes = tauri_build::Attributes::new().windows_attributes(windows);
    tauri_build::try_build(attributes).expect("failed to run Tauri build script")
}
