use std::sync::Mutex;
use std::time::Duration;
use tauri::AppHandle;
use tauri_plugin_shell::ShellExt;
use tauri_plugin_shell::process::CommandChild;

const RUNTIME_HEALTH_TIMEOUT: Duration = Duration::from_millis(500);

/// Holds the sidecar child process so we can shut it down on exit.
pub struct SidecarState {
    pub child: Mutex<Option<CommandChild>>,
    pub port: u16,
}

#[derive(serde::Deserialize)]
struct RuntimeInfo {
    port: u16,
    token: String,
}

#[derive(serde::Deserialize)]
struct HealthResponse {
    service: Option<String>,
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

fn runtime_json_path() -> Option<std::path::PathBuf> {
    dirs::home_dir().map(|home| home.join(".vireo").join("runtime.json"))
}

fn read_runtime_json(path: &std::path::Path) -> Option<RuntimeInfo> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice::<RuntimeInfo>(&bytes).ok()
}

fn runtime_health_is_vireo(port: u16, token: &str, timeout: Duration) -> bool {
    let url = format!("http://127.0.0.1:{}/api/v1/health", port);
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(timeout)
        .timeout_read(timeout)
        .build();
    let Ok(resp) = agent.get(&url).set("X-Vireo-Token", token).call() else {
        return false;
    };
    if resp.status() != 200 {
        return false;
    }
    let Ok(body) = resp.into_string() else {
        return false;
    };
    let Ok(health) = serde_json::from_str::<HealthResponse>(&body) else {
        return false;
    };
    health.service.as_deref() == Some("vireo")
}

fn existing_runtime_port() -> Option<u16> {
    let path = runtime_json_path()?;
    let runtime = read_runtime_json(&path)?;
    if runtime_health_is_vireo(runtime.port, &runtime.token, RUNTIME_HEALTH_TIMEOUT) {
        Some(runtime.port)
    } else {
        None
    }
}

/// Spawn the Python sidecar and wait for it to be ready.
pub fn start_sidecar(app: &AppHandle) -> Result<SidecarState, String> {
    if let Some(port) = existing_runtime_port() {
        log::info!(
            "Using existing Vireo backend from runtime.json on port {}",
            port
        );
        return Ok(SidecarState {
            child: Mutex::new(None),
            port,
        });
    }

    let port = find_free_port();

    // macOS GUI apps get a minimal PATH that excludes Homebrew directories,
    // so tools like exiftool won't be found. Prepend common Homebrew paths.
    #[cfg(target_os = "macos")]
    let extended_path = {
        let path = std::env::var("PATH").unwrap_or_default();
        format!("/opt/homebrew/bin:/usr/local/bin:{}", path)
    };

    let mut cmd = app
        .shell()
        .sidecar("vireo-server")
        .map_err(|e| format!("Failed to create sidecar command: {}", e))?;

    #[cfg(target_os = "macos")]
    {
        cmd = cmd.env("PATH", &extended_path);
    }

    let (mut rx, child) = cmd
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_runtime_json_accepts_valid_payload() {
        let dir = std::env::temp_dir().join(format!(
            "vireo-runtime-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime.json");
        std::fs::write(
            &path,
            r#"{"port":8080,"pid":123,"token":"secret","service":"ignored"}"#,
        )
        .unwrap();

        let runtime = read_runtime_json(&path).unwrap();
        assert_eq!(runtime.port, 8080);
        assert_eq!(runtime.token, "secret");

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir(dir);
    }

    #[test]
    fn read_runtime_json_rejects_missing_token() {
        let dir = std::env::temp_dir().join(format!(
            "vireo-runtime-test-missing-token-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime.json");
        std::fs::write(&path, r#"{"port":8080}"#).unwrap();

        assert!(read_runtime_json(&path).is_none());

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir(dir);
    }
}
