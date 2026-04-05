use std::sync::Mutex;
use std::time::Duration;
use tauri::AppHandle;
use tauri_plugin_shell::ShellExt;
use tauri_plugin_shell::process::CommandChild;

/// Holds the sidecar child process so we can shut it down on exit.
pub struct SidecarState {
    pub child: Mutex<Option<CommandChild>>,
    pub port: u16,
}

/// Find a free TCP port.
/// NOTE: There is an inherent TOCTOU race between releasing this port and the
/// sidecar binding to it. If another process grabs it first, the sidecar will
/// fail to start and the health-check will surface the error.
fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Wait for the sidecar to respond to /api/health.
fn wait_for_health(port: u16, timeout: Duration) -> Result<(), String> {
    let start = std::time::Instant::now();
    let url = format!("http://127.0.0.1:{}/api/health", port);
    loop {
        if start.elapsed() > timeout {
            return Err(format!(
                "Sidecar did not become healthy within {}s",
                timeout.as_secs()
            ));
        }
        match ureq::get(&url).call() {
            Ok(resp) if resp.status() == 200 => return Ok(()),
            _ => std::thread::sleep(Duration::from_millis(200)),
        }
    }
}

/// Spawn the Python sidecar and wait for it to be ready.
pub fn start_sidecar(app: &AppHandle) -> Result<SidecarState, String> {
    let port = find_free_port();

    // macOS GUI apps get a minimal PATH that excludes Homebrew directories,
    // so tools like exiftool won't be found. Prepend common Homebrew paths.
    let path = std::env::var("PATH").unwrap_or_default();
    let extended_path = format!("/opt/homebrew/bin:/usr/local/bin:{}", path);

    let (mut rx, child) = app
        .shell()
        .sidecar("vireo-server")
        .map_err(|e| format!("Failed to create sidecar command: {}", e))?
        .env("PATH", &extended_path)
        .args([
            "--port", &port.to_string(),
            "--no-browser",
            "--db", &dirs::home_dir()
                .unwrap_or_default()
                .join(".vireo/vireo.db")
                .to_string_lossy(),
        ])
        .spawn()
        .map_err(|e| format!("Failed to spawn sidecar: {}", e))?;

    // Log sidecar stdout/stderr in a background task
    tauri::async_runtime::spawn(async move {
        while let Some(event) = rx.recv().await {
            match event {
                tauri_plugin_shell::process::CommandEvent::Stdout(line) => {
                    log::info!("[sidecar] {}", String::from_utf8_lossy(&line));
                }
                tauri_plugin_shell::process::CommandEvent::Stderr(line) => {
                    log::warn!("[sidecar] {}", String::from_utf8_lossy(&line));
                }
                tauri_plugin_shell::process::CommandEvent::Terminated(payload) => {
                    log::info!("[sidecar] terminated: {:?}", payload);
                    break;
                }
                _ => {}
            }
        }
    });

    // Wait up to 30 seconds for the sidecar to be healthy
    wait_for_health(port, Duration::from_secs(30))?;

    Ok(SidecarState {
        child: Mutex::new(Some(child)),
        port,
    })
}

/// Send POST /api/shutdown to the sidecar for a clean exit.
/// In dev mode (child is None), this is a no-op — we don't want to
/// kill the developer's manually-started Flask server.
pub fn stop_sidecar(state: &SidecarState) {
    let has_child = state.child
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_some();

    if !has_child {
        return;
    }

    let url = format!("http://127.0.0.1:{}/api/shutdown", state.port);
    let _ = ureq::post(&url)
        .set("X-Vireo-Shutdown", "1")
        .call();
    // Give the sidecar a moment to shut down gracefully
    std::thread::sleep(Duration::from_millis(500));
    // Force-kill if still running
    if let Some(child) = state.child.lock().unwrap_or_else(|e| e.into_inner()).take() {
        let _ = child.kill();
    }
}
