mod config;
mod menu;
mod sidecar;
mod tray;
mod updater;
use sidecar::SidecarState;
use tauri::{Manager, RunEvent};
use tauri::window::{ProgressBarState, ProgressBarStatus};
use tauri_plugin_opener::OpenerExt;

/// Open the given URL in the user's default web browser.
///
/// Logs failures rather than propagating them — failing to open a browser
/// shouldn't crash the app; the user can still reach the UI by clicking
/// the tray icon menu item.
fn open_in_browser(app: &tauri::AppHandle, url: &str) {
    if let Err(e) = app.opener().open_url(url, None::<&str>) {
        log::error!("Failed to open browser at {}: {}", url, e);
    } else {
        log::info!("Opened default browser at {}", url);
    }
}

#[tauri::command]
fn get_server_port(state: tauri::State<'_, SidecarState>) -> u16 {
    state.port
}

// Update the OS-level progress indicator on the dock/taskbar icon.
// `progress` is 0-100 (None = clear). When `indeterminate` is true the bar
// pulses instead of filling — used for phases without a known total.
#[tauri::command]
fn set_job_progress(
    window: tauri::WebviewWindow,
    progress: Option<u64>,
    indeterminate: bool,
) -> Result<(), String> {
    let (status, progress) = if indeterminate {
        (ProgressBarStatus::Indeterminate, Some(0))
    } else if let Some(p) = progress {
        (ProgressBarStatus::Normal, Some(p.min(100)))
    } else {
        (ProgressBarStatus::None, None)
    };
    window
        .set_progress_bar(ProgressBarState {
            status: Some(status),
            progress,
        })
        .map_err(|e| e.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .setup(|app| {
            // Read the user's launch-time config (~/.vireo/config.json).
            // Failures fall back to defaults — see config::load_launch_config.
            let launch_cfg = config::load_launch_config();
            let browser_mode = launch_cfg.open_in_browser();

            if cfg!(debug_assertions) {
                // In dev mode, don't spawn sidecar — developer runs Flask manually
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
                // Use a placeholder state pointing to the dev server
                app.manage(SidecarState {
                    child: std::sync::Mutex::new(None),
                    port: 8080,
                });
            } else {
                // Production: spawn the sidecar
                match sidecar::start_sidecar(app.handle()) {
                    Ok(state) => {
                        let port = state.port;
                        app.manage(state);
                        // Only navigate the WKWebView when we're actually
                        // going to show it. In browser mode the window is
                        // about to be hidden/closed anyway.
                        if !browser_mode {
                            if let Some(window) = app.get_webview_window("main") {
                                let url = format!("http://127.0.0.1:{}", port);
                                let _ = window.navigate(url.parse().unwrap());
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to start sidecar: {}", e);
                        eprintln!("Vireo: Failed to start Python backend: {}", e);
                        std::process::exit(1);
                    }
                }
            }

            // Build and attach the native menu bar
            let menu = menu::build_menu(app.handle())?;
            app.set_menu(menu)?;

            let port = app.state::<SidecarState>().port;
            tray::create_tray(app.handle(), port, browser_mode)?;

            // The main window is created with `visible: false` (see
            // tauri.conf.json) so we can decide here whether to show it
            // (classic mode) or leave it hidden (browser mode) — avoids a
            // visible flash before we'd otherwise hide it.
            if browser_mode {
                // Sidecar is healthy by this point — start_sidecar blocks
                // on /api/health in production, and in dev the developer's
                // Flask is presumed already running.
                let url = format!("http://127.0.0.1:{}", port);
                open_in_browser(app.handle(), &url);
            } else if let Some(window) = app.get_webview_window("main") {
                let _ = window.show();
                let _ = window.set_focus();
            }

            // Background update checks (production only)
            if !cfg!(debug_assertions) {
                let update_handle = app.handle().clone();
                std::thread::spawn(move || {
                    // Let the app finish loading before first check
                    std::thread::sleep(std::time::Duration::from_secs(5));
                    updater::spawn_update_check(&update_handle, false);

                    // Check every 24 hours
                    loop {
                        std::thread::sleep(std::time::Duration::from_secs(24 * 60 * 60));
                        updater::spawn_update_check(&update_handle, false);
                    }
                });
            }

            Ok(())
        })
        .on_window_event(|window, event| {
            match event {
                tauri::WindowEvent::CloseRequested { api, .. } => {
                    if window.label() == "main" {
                        // Don't close — just hide the window (minimize to tray)
                        api.prevent_close();
                        let _ = window.hide();
                    }
                }
                _ => {}
            }
        })
        .on_menu_event(|app, event| {
            let id = event.id().0.as_str();

            if id == menu::ids::CHECK_FOR_UPDATES {
                updater::spawn_update_check(app, true);
                return;
            }

            // "Report an Issue" — open GitHub issues in the default browser
            if id == menu::ids::REPORT_ISSUE {
                use tauri_plugin_opener::OpenerExt;
                let _ = app.opener().open_url("https://github.com/jss367/vireo/issues", None::<&str>);
                return;
            }

            // "Open in Browser" — flip from the WKWebView to the user's
            // default browser at runtime. Does not persist; it's a one-shot
            // flip for this session. Routes through tray::open_ui_in_browser
            // so the runtime mode flag flips and later menu/tray actions
            // also route to the browser.
            if id == menu::ids::OPEN_IN_BROWSER {
                tray::open_ui_in_browser(app);
                return;
            }

            // Navigation items — evaluate JS in the main webview, or in
            // browser mode open the route in the user's default browser.
            if let Some(route) = menu::route_for_id(id) {
                if tray::is_browser_mode(app) {
                    let port = app.state::<SidecarState>().port;
                    let url = format!("http://127.0.0.1:{}{}", port, route);
                    open_in_browser(app, &url);
                } else if let Some(window) = app.get_webview_window("main") {
                    let js = format!("window.location.href = '{}'", route);
                    if let Err(e) = window.eval(&js) {
                        log::error!("Failed to navigate to {}: {}", route, e);
                    }
                }
            }
        })
        .invoke_handler(tauri::generate_handler![
            get_server_port,
            set_job_progress,
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app_handle, event| {
            if let RunEvent::Exit = event {
                if let Some(window) = app_handle.get_webview_window("main") {
                    let _ = window.set_progress_bar(ProgressBarState {
                        status: Some(ProgressBarStatus::None),
                        progress: None,
                    });
                }
                if let Some(state) = app_handle.try_state::<SidecarState>() {
                    sidecar::stop_sidecar(&state);
                }
            }
        });
}
