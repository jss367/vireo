use fs2::FileExt;
use std::sync::Mutex;
use std::time::Duration;
use tauri::AppHandle;
use tauri_plugin_shell::process::CommandChild;
use tauri_plugin_shell::ShellExt;

const RUNTIME_HEALTH_TIMEOUT: Duration = Duration::from_millis(500);
const RUNTIME_BOOT_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const RUNTIME_LOCK_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const RUNTIME_BOOT_WAIT_INTERVAL: Duration = Duration::from_millis(200);
const GUI_CLIENTS_DIR: &str = ".vireo/gui-clients";

/// Holds the sidecar child process so we can shut it down on exit.
pub struct SidecarState {
    pub child: Mutex<Option<CommandChild>>,
    pub port: u16,
    shutdown_on_exit: bool,
    client_marker: Option<std::path::PathBuf>,
}

impl SidecarState {
    pub fn unmanaged(port: u16) -> Self {
        Self {
            child: Mutex::new(None),
            port,
            shutdown_on_exit: false,
            client_marker: None,
        }
    }

    fn owned(child: CommandChild, port: u16) -> Self {
        Self {
            child: Mutex::new(Some(child)),
            port,
            shutdown_on_exit: true,
            client_marker: register_gui_client(),
        }
    }

    fn attached(runtime: RuntimeInfo) -> Option<Self> {
        let shutdown_on_exit = runtime.mode.as_deref() == Some("gui");
        let client_marker = shutdown_on_exit.then(register_gui_client).flatten();
        if !runtime_health_is_vireo(runtime.port, &runtime.token, RUNTIME_HEALTH_TIMEOUT) {
            remove_gui_client(&client_marker);
            return None;
        }
        Some(Self {
            child: Mutex::new(None),
            port: runtime.port,
            shutdown_on_exit,
            client_marker,
        })
    }
}

#[derive(serde::Deserialize)]
struct RuntimeInfo {
    port: u16,
    token: String,
    mode: Option<String>,
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

fn runtime_lock_path() -> Option<std::path::PathBuf> {
    dirs::home_dir().map(|home| home.join(".vireo").join("runtime.lock"))
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

fn runtime_lock_is_held(path: &std::path::Path) -> bool {
    if !path.exists() {
        return false;
    }
    let Ok(file) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    else {
        return false;
    };
    match file.try_lock_exclusive() {
        Ok(()) => {
            let _ = file.unlock();
            false
        }
        Err(_) => true,
    }
}

fn existing_runtime() -> Option<RuntimeInfo> {
    let path = runtime_json_path()?;
    let lock_path = runtime_lock_path()?;
    let start = std::time::Instant::now();

    loop {
        if let Some(runtime) = read_runtime_json(&path) {
            if runtime_health_is_vireo(runtime.port, &runtime.token, RUNTIME_HEALTH_TIMEOUT) {
                return Some(runtime);
            }
        }

        let lock_held = runtime_lock_is_held(&lock_path);
        let runtime_present = path.exists();
        if !runtime_present && !lock_held {
            return None;
        }
        let timeout = if lock_held {
            RUNTIME_LOCK_WAIT_TIMEOUT
        } else {
            RUNTIME_BOOT_WAIT_TIMEOUT
        };
        if start.elapsed() >= timeout {
            return None;
        }

        std::thread::sleep(RUNTIME_BOOT_WAIT_INTERVAL);
    }
}

fn gui_clients_dir() -> Option<std::path::PathBuf> {
    dirs::home_dir().map(|home| home.join(GUI_CLIENTS_DIR))
}

fn register_gui_client() -> Option<std::path::PathBuf> {
    let dir = gui_clients_dir()?;
    if let Err(e) = std::fs::create_dir_all(&dir) {
        log::warn!("Failed to create Vireo GUI client directory: {}", e);
        return None;
    }
    let marker = dir.join(format!("{}.client", std::process::id()));
    if let Err(e) = std::fs::write(&marker, b"") {
        log::warn!("Failed to write Vireo GUI client marker: {}", e);
        return None;
    }
    Some(marker)
}

fn remove_gui_client(marker: &Option<std::path::PathBuf>) {
    if let Some(marker) = marker {
        let _ = std::fs::remove_file(marker);
    }
}

fn live_gui_client_count() -> usize {
    let Some(dir) = gui_clients_dir() else {
        return 0;
    };
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return 0;
    };

    entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let pid = path
                .file_stem()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse::<u32>().ok())?;
            if process_is_alive(pid) {
                Some(())
            } else {
                let _ = std::fs::remove_file(path);
                None
            }
        })
        .count()
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    let pid_arg = pid.to_string();
    std::process::Command::new("kill")
        .args(["-0", &pid_arg])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(windows)]
fn process_is_alive(pid: u32) -> bool {
    let filter = format!("PID eq {}", pid);
    let Ok(output) = std::process::Command::new("tasklist")
        .args(["/FI", &filter, "/FO", "CSV", "/NH"])
        .output()
    else {
        return false;
    };
    if !output.status.success() {
        return false;
    }

    let pid_arg = pid.to_string();
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.lines().any(|line| {
        line.split(',')
            .nth(1)
            .map(|field| field.trim_matches('"') == pid_arg)
            .unwrap_or(false)
    })
}

#[cfg(not(any(unix, windows)))]
fn process_is_alive(_pid: u32) -> bool {
    false
}

/// Spawn the Python sidecar and wait for it to be ready.
pub fn start_sidecar(app: &AppHandle) -> Result<SidecarState, String> {
    if let Some(runtime) = existing_runtime() {
        log::info!(
            "Using existing Vireo backend from runtime.json on port {}",
            runtime.port
        );
        if let Some(state) = SidecarState::attached(runtime) {
            return Ok(state);
        }
        log::info!("Existing Vireo backend stopped while attaching; spawning a new sidecar");
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
            "--port",
            &port.to_string(),
            "--no-browser",
            "--db",
            &dirs::home_dir()
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

    Ok(SidecarState::owned(child, port))
}

/// Send POST /api/shutdown to the sidecar for a clean exit.
/// In dev mode (child is None), this is a no-op — we don't want to
/// kill the developer's manually-started Flask server.
pub fn stop_sidecar(state: &SidecarState) {
    if !state.shutdown_on_exit {
        return;
    }

    remove_gui_client(&state.client_marker);
    if live_gui_client_count() > 0 {
        log::info!("Leaving Vireo backend running for another GUI client");
        return;
    }

    let has_child = state
        .child
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_some();

    let url = format!("http://127.0.0.1:{}/api/shutdown", state.port);
    let _ = ureq::post(&url).set("X-Vireo-Shutdown", "1").call();
    // Give the sidecar a moment to shut down gracefully
    std::thread::sleep(Duration::from_millis(500));
    // Force-kill if still running
    if has_child {
        let child = state.child.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(child) = child {
            let _ = child.kill();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_runtime_json_accepts_valid_payload() {
        let dir = std::env::temp_dir().join(format!("vireo-runtime-test-{}", std::process::id()));
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
        assert_eq!(runtime.mode.as_deref(), None);

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

    #[test]
    fn read_runtime_json_preserves_mode() {
        let dir =
            std::env::temp_dir().join(format!("vireo-runtime-test-mode-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("runtime.json");
        std::fs::write(&path, r#"{"port":8080,"token":"secret","mode":"gui"}"#).unwrap();

        let runtime = read_runtime_json(&path).unwrap();
        assert_eq!(runtime.mode.as_deref(), Some("gui"));

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir(dir);
    }
}
