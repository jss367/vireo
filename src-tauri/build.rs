fn main() {
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
