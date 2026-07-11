mod config;
mod menu;
mod navigation;
mod sidecar;
mod tray;
mod updater;
use sidecar::{SidecarStartError, SidecarState};
use tauri::webview::NewWindowResponse;
use tauri::window::{ProgressBarState, ProgressBarStatus};
use tauri::{Manager, RunEvent};
use tauri_plugin_dialog::{DialogExt, MessageDialogKind};
use tauri_plugin_opener::OpenerExt;

const INDETERMINATE_PROGRESS_FALLBACK: u64 = 10;

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

fn dispatch_menu_command(app: &tauri::AppHandle, command: &str) {
    if tray::is_browser_mode(app) {
        log::warn!(
            "Native menu command '{}' ignored because Vireo is running in browser mode",
            command
        );
        return;
    }

    if let Some(window) = app.get_webview_window("main") {
        let js = format!(
            "if (window.handleNativeMenuCommand) {{ window.handleNativeMenuCommand({:?}); }}",
            command
        );
        if let Err(e) = window.eval(&js) {
            log::error!("Failed to dispatch menu command {}: {}", command, e);
        }
    }
}

fn reload_main_window(app: &tauri::AppHandle) {
    if tray::is_browser_mode(app) {
        log::warn!("Native reload ignored because Vireo is running in browser mode");
        return;
    }

    if let Some(window) = app.get_webview_window("main") {
        if let Err(e) = window.eval("window.location.reload()") {
            log::error!("Failed to reload main window: {}", e);
        }
    }
}

#[tauri::command]
fn get_server_port(state: tauri::State<'_, SidecarState>) -> u16 {
    state.port
}

// Update the OS-level progress indicator on the dock/taskbar icon.
// `progress` is 0-100 (None = clear). Tauri treats indeterminate as a normal
// bar on macOS/Linux, so use a small visible value for unknown-total phases.
#[tauri::command]
fn set_job_progress(
    window: tauri::WebviewWindow,
    progress: Option<u64>,
    indeterminate: bool,
) -> Result<(), String> {
    let (status, progress) = if indeterminate {
        // On Windows this requests the native indeterminate taskbar state.
        // On macOS/Linux Tauri treats Indeterminate as Normal, where progress
        // is a 0-100 percentage. Use 10% so unknown-total work is visible;
        // ProgressBarStatus::None remains the explicit hidden state.
        (
            ProgressBarStatus::Indeterminate,
            Some(INDETERMINATE_PROGRESS_FALLBACK),
        )
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

#[tauri::command]
fn open_external_url(app: tauri::AppHandle, url: String) -> Result<(), String> {
    navigation::open_external_url(&app, &url)
}

/// Build the logging plugin used in BOTH dev and release builds.
///
/// Previously logging was only initialized under `cfg!(debug_assertions)`, so
/// installed builds wrote nothing to disk — every `log::` call from the sidecar
/// supervisor, updater, tray, and browser opener silently vanished, which made
/// field bugs (such as the iNaturalist quick-open failing) impossible to
/// diagnose. Now it runs everywhere, writing to the OS log directory
/// (`~/Library/Logs/com.vireo.app/Vireo.log` on macOS) and stdout.
///
/// The webview forwards its own logs and uncaught errors here via the plugin's
/// `log` command (see tauri-bridge.js), so the single file captures both the
/// Rust side and the JS side.
fn build_log_plugin<R: tauri::Runtime>() -> tauri::plugin::TauriPlugin<R> {
    use tauri_plugin_log::{RotationStrategy, Target, TargetKind};
    tauri_plugin_log::Builder::default()
        .level(log::LevelFilter::Info)
        // The plugin default is only 40 KB; match the Flask sidecar's 5 MB so a
        // real debugging session's worth of logs survives before rotating.
        .max_file_size(5 * 1024 * 1024)
        .rotation_strategy(RotationStrategy::KeepSome(3))
        .targets([
            Target::new(TargetKind::Stdout),
            Target::new(TargetKind::LogDir { file_name: None }),
        ])
        .build()
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(build_log_plugin())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .setup(|app| {
            // Build the main window ourselves so the native shell, rather than
            // scattered page JavaScript, owns the external-navigation policy.
            // `create: false` in tauri.conf.json prevents Tauri from building
            // this same config before setup runs.
            let main_config = app
                .config()
                .app
                .windows
                .iter()
                .find(|config| config.label == "main")
                .ok_or("main window configuration is missing")?
                .clone();
            let navigation_app = app.handle().clone();
            let popup_app = app.handle().clone();
            tauri::WebviewWindowBuilder::from_config(app.handle(), &main_config)?
                .on_navigation(move |url| {
                    navigation::handle_navigation(&navigation_app, url)
                })
                .on_new_window(move |url, _features| {
                    navigation::handle_new_window(&popup_app, &url);
                    NewWindowResponse::Deny
                })
                .build()?;

            // Read the user's launch-time config (~/.vireo/config.json).
            // Failures fall back to defaults — see config::load_launch_config.
            let launch_cfg = config::load_launch_config();
            let browser_mode = launch_cfg.open_in_browser();

            if cfg!(debug_assertions) {
                // In dev mode, don't spawn sidecar — developer runs Flask
                // manually. Logging is initialized unconditionally in the
                // builder chain (see build_log_plugin), so it's already active.
                // Use a placeholder state pointing to the dev server
                app.manage(SidecarState::unmanaged(8080));
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
                    Err(SidecarStartError::IncompatibleDatabase { db_path, reason }) => {
                        // The Python sidecar refused to open the DB because
                        // its schema predates a non-migratable change. Surface
                        // an actionable dialog before exiting — without this
                        // the user just sees the WKWebView fail to load and
                        // has no way to tell whether to delete the DB, file a
                        // bug, or reinstall.
                        log::error!(
                            "Incompatible database at {}: {}. Back up this file and relaunch.",
                            db_path, reason
                        );
                        eprintln!(
                            "Vireo: Incompatible database at {}: {}",
                            db_path, reason
                        );
                        app.handle()
                            .dialog()
                            .message(format!(
                                "Vireo can't open the database at:\n\n{}\n\nIt's from an incompatible older version of Vireo. To start fresh, move this file aside (for example, rename it with a `.bak` suffix) and relaunch.\n\nDetails: {}",
                                db_path, reason
                            ))
                            .title("Incompatible Vireo Database")
                            .kind(MessageDialogKind::Error)
                            .blocking_show();
                        std::process::exit(3);
                    }
                    Err(e) => {
                        // Any other startup failure (corrupt DB, locked file,
                        // port conflict, health timeout, unexpected crash).
                        // Always surface a dialog rather than exiting silently
                        // into a blank window — the user otherwise has no idea
                        // why Vireo didn't open.
                        let reason = e.to_string();
                        log::error!("Failed to start sidecar: {}", reason);
                        eprintln!("Vireo: Failed to start Python backend: {}", reason);
                        app.handle()
                            .dialog()
                            .message(format!(
                                "Vireo couldn't start its backend.\n\nDetails: {}\n\nTry relaunching. If the problem persists, check Vireo's log file for more information.",
                                reason
                            ))
                            .title("Vireo Couldn't Start")
                            .kind(MessageDialogKind::Error)
                            .blocking_show();
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
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                if window.label() == "main" {
                    // Don't close — just hide the window (minimize to tray)
                    api.prevent_close();
                    let _ = window.hide();
                }
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

            if id == menu::ids::VIEW_RELOAD {
                reload_main_window(app);
                return;
            }

            if let Some(command) = menu::command_for_id(id) {
                dispatch_menu_command(app, command);
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
            open_external_url,
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
