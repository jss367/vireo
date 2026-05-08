use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tauri::{
    AppHandle, Manager,
    menu::{Menu, MenuItem, PredefinedMenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
};
use tauri_plugin_opener::OpenerExt;

#[derive(Deserialize)]
struct JobsResponse {
    active: Vec<JobInfo>,
}

#[derive(Deserialize)]
struct JobInfo {
    status: String,
    #[serde(rename = "type")]
    _job_type: String,
}

/// State for the tray polling thread's stop signal.
pub struct TrayPollState {
    pub stop: Arc<AtomicBool>,
}

/// Whether the app is currently running in browser mode. Stored in Tauri
/// state so the menu-event handler (which only gets an `AppHandle`) can
/// branch correctly without re-reading the config file.
///
/// Mutable because the user can flip from window mode to browser mode at
/// runtime via View → Open in Browser; we don't currently flip the other way.
pub struct TrayMode {
    pub browser_mode: AtomicBool,
    /// Sidecar port — used to construct the URL we hand to the browser.
    pub port: u16,
    /// Latest job-status text shown in the tray menu, kept here so we can
    /// rebuild the menu on a runtime mode flip without losing the label.
    pub job_status: Mutex<String>,
}

/// Menu item IDs
const SHOW_WINDOW: &str = "show_window";
const HIDE_WINDOW: &str = "hide_window";
const OPEN_IN_BROWSER: &str = "open_in_browser";
const JOB_STATUS: &str = "job_status";
const QUIT: &str = "quit";

/// Build the tray icon with its context menu and start job polling.
///
/// `browser_mode` swaps the "Show / Hide Window" pair for a single
/// "Open in browser" item, since the WKWebView window is intentionally
/// hidden in that mode and showing it would be confusing.
pub fn create_tray(app: &AppHandle, port: u16, browser_mode: bool) -> tauri::Result<()> {
    let initial_status = "No active jobs";
    app.manage(TrayMode {
        browser_mode: AtomicBool::new(browser_mode),
        port,
        job_status: Mutex::new(initial_status.to_string()),
    });

    let menu = build_menu(app, initial_status, browser_mode)?;

    TrayIconBuilder::with_id("main-tray")
        .icon(app.default_window_icon().unwrap().clone())
        .tooltip("Vireo")
        .menu(&menu)
        .show_menu_on_left_click(false)
        .on_menu_event(|app, event| {
            handle_menu_event(app, event.id().as_ref());
        })
        .on_tray_icon_event(|tray, event| {
            // Left-click on the tray icon: show and focus the window, or
            // re-open the browser if we're in browser mode.
            if let TrayIconEvent::Click {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                ..
            } = event
            {
                let app = tray.app_handle();
                activate_ui(app);
            }
        })
        .build(app)?;

    // Start the background polling thread
    let stop = Arc::new(AtomicBool::new(false));
    app.manage(TrayPollState { stop: stop.clone() });
    start_job_polling(app.clone(), port, stop);

    Ok(())
}

/// Build (or rebuild) the tray context menu with a given job status string.
pub fn build_menu(
    app: &AppHandle,
    job_status: &str,
    browser_mode: bool,
) -> tauri::Result<Menu<tauri::Wry>> {
    let sep1 = PredefinedMenuItem::separator(app)?;
    let jobs = MenuItem::with_id(app, JOB_STATUS, job_status, false, None::<&str>)?;
    let sep2 = PredefinedMenuItem::separator(app)?;
    let quit = MenuItem::with_id(app, QUIT, "Quit Vireo", true, None::<&str>)?;

    if browser_mode {
        // Browser-launch mode: there is no app window to show or hide, so
        // we offer a single "Open in browser" item that re-opens the URL
        // (handy if the user closed the tab).
        let open = MenuItem::with_id(
            app,
            OPEN_IN_BROWSER,
            "Open in browser",
            true,
            None::<&str>,
        )?;
        Menu::with_items(app, &[&open, &sep1, &jobs, &sep2, &quit])
    } else {
        let show = MenuItem::with_id(app, SHOW_WINDOW, "Show Window", true, None::<&str>)?;
        let hide = MenuItem::with_id(app, HIDE_WINDOW, "Hide Window", true, None::<&str>)?;
        let open = MenuItem::with_id(
            app,
            OPEN_IN_BROWSER,
            "Open in Browser",
            true,
            None::<&str>,
        )?;
        Menu::with_items(app, &[&show, &hide, &open, &sep1, &jobs, &sep2, &quit])
    }
}

/// Handle a menu item click.
fn handle_menu_event(app: &AppHandle, id: &str) {
    match id {
        SHOW_WINDOW => show_main_window(app),
        HIDE_WINDOW => hide_main_window(app),
        OPEN_IN_BROWSER => open_ui_in_browser(app),
        QUIT => {
            // Stop the polling thread
            if let Some(poll_state) = app.try_state::<TrayPollState>() {
                poll_state.stop.store(true, Ordering::Relaxed);
            }
            // Clean up sidecar before quitting
            if let Some(state) = app.try_state::<crate::sidecar::SidecarState>() {
                crate::sidecar::stop_sidecar(&state);
            }
            app.exit(0);
        }
        _ => {}
    }
}

/// Show and focus the main window.
fn show_main_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
    }
}

/// Hide the main window.
fn hide_main_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.hide();
    }
}

/// Open (or re-open) the UI in the user's default browser, hide the app
/// window if shown, and flip the runtime `browser_mode` flag on so all
/// later menu navigation and tray clicks route to the browser too.
///
/// In browser mode this is what the tray's left-click and "Open in browser"
/// menu item do (the window is already hidden and the flag already true,
/// so this is idempotent). In window mode it acts as a one-shot runtime
/// "flip to browser" for the rest of the session — most browsers focus an
/// existing tab on the same origin rather than opening a duplicate.
pub fn open_ui_in_browser(app: &AppHandle) {
    let (port, was_window_mode) = match app.try_state::<TrayMode>() {
        Some(mode) => (
            mode.port,
            !mode.browser_mode.swap(true, Ordering::Relaxed),
        ),
        None => {
            log::error!("TrayMode not initialised; cannot open browser");
            return;
        }
    };
    let url = format!("http://127.0.0.1:{}", port);
    if let Err(e) = app.opener().open_url(&url, None::<&str>) {
        log::error!("Failed to open browser at {}: {}", url, e);
    }
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.hide();
    }
    // First time we flip from window → browser, rebuild the tray menu so
    // the Show/Hide Window items are replaced by the browser-mode layout.
    if was_window_mode {
        let status = app
            .try_state::<TrayMode>()
            .and_then(|m| m.job_status.lock().ok().map(|s| s.clone()))
            .unwrap_or_else(|| "No active jobs".to_string());
        update_tray_menu(app, &status);
    }
}

/// Activate the UI: show the main window, or open the browser, depending
/// on which mode we're currently in.
fn activate_ui(app: &AppHandle) {
    if is_browser_mode(app) {
        open_ui_in_browser(app);
    } else {
        show_main_window(app);
    }
}

/// Read the current runtime mode.
pub fn is_browser_mode(app: &AppHandle) -> bool {
    app.try_state::<TrayMode>()
        .map(|m| m.browser_mode.load(Ordering::Relaxed))
        .unwrap_or(false)
}

/// Query the Flask backend for running jobs.
fn fetch_running_job_count(port: u16) -> usize {
    let url = format!("http://127.0.0.1:{}/api/jobs", port);
    match ureq::get(&url).call() {
        Ok(resp) => match resp.into_string() {
            Ok(body) => match serde_json::from_str::<JobsResponse>(&body) {
                Ok(data) => data.active.iter().filter(|j| j.status == "running").count(),
                Err(e) => {
                    log::warn!("Failed to parse /api/jobs response: {}", e);
                    0
                }
            },
            Err(e) => {
                log::warn!("Failed to read /api/jobs body: {}", e);
                0
            }
        },
        Err(e) => {
            log::debug!("Failed to reach /api/jobs: {}", e);
            0
        }
    }
}

/// Rebuild the tray menu with updated job status text.
fn update_tray_menu(app: &AppHandle, job_status: &str) {
    let browser_mode = is_browser_mode(app);
    if let Some(mode) = app.try_state::<TrayMode>() {
        if let Ok(mut s) = mode.job_status.lock() {
            *s = job_status.to_string();
        }
    }
    if let Some(tray) = app.tray_by_id("main-tray") {
        match build_menu(app, job_status, browser_mode) {
            Ok(menu) => {
                let _ = tray.set_menu(Some(menu));
            }
            Err(e) => {
                log::warn!("Failed to rebuild tray menu: {}", e);
            }
        }
    }
}

/// Start a background thread that polls /api/jobs every 5 seconds
/// and updates the tray menu with the current job count.
pub fn start_job_polling(app: AppHandle, port: u16, stop: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        let mut last_count: Option<usize> = None;
        while !stop.load(Ordering::Relaxed) {
            let count = fetch_running_job_count(port);

            // Only update the menu if the count changed
            if last_count != Some(count) {
                let status = if count == 0 {
                    "No active jobs".to_string()
                } else if count == 1 {
                    "1 job running".to_string()
                } else {
                    format!("{} jobs running", count)
                };
                update_tray_menu(&app, &status);

                // Update tooltip
                let tooltip = if count == 0 {
                    "Vireo".to_string()
                } else {
                    format!("Vireo - {}", status)
                };
                if let Some(tray) = app.tray_by_id("main-tray") {
                    let _ = tray.set_tooltip(Some(&tooltip));
                }

                last_count = Some(count);
            }

            // Sleep in 500ms increments so we can check the stop flag
            for _ in 0..10 {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        }
        log::info!("Tray job polling thread stopped");
    });
}
