use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tauri::{
    image::Image,
    AppHandle, Manager,
    menu::{Menu, MenuItem, PredefinedMenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
};

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

/// Menu item IDs
const SHOW_WINDOW: &str = "show_window";
const HIDE_WINDOW: &str = "hide_window";
const JOB_STATUS: &str = "job_status";
const QUIT: &str = "quit";

/// Build the tray icon with its context menu and start job polling.
pub fn create_tray(app: &AppHandle, port: u16) -> tauri::Result<()> {
    let menu = build_menu(app, "No active jobs")?;

    TrayIconBuilder::with_id("main-tray")
        .icon(app.default_window_icon().unwrap().clone())
        .tooltip("Vireo")
        .menu(&menu)
        .show_menu_on_left_click(false)
        .on_menu_event(|app, event| {
            handle_menu_event(app, event.id().as_ref());
        })
        .on_tray_icon_event(|tray, event| {
            // Left-click on the tray icon: show and focus the window
            if let TrayIconEvent::Click {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                ..
            } = event
            {
                let app = tray.app_handle();
                show_main_window(app);
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
pub fn build_menu(app: &AppHandle, job_status: &str) -> tauri::Result<Menu<tauri::Wry>> {
    let show = MenuItem::with_id(app, SHOW_WINDOW, "Show Window", true, None::<&str>)?;
    let hide = MenuItem::with_id(app, HIDE_WINDOW, "Hide Window", true, None::<&str>)?;
    let sep1 = PredefinedMenuItem::separator(app)?;
    let jobs = MenuItem::with_id(app, JOB_STATUS, job_status, false, None::<&str>)?;
    let sep2 = PredefinedMenuItem::separator(app)?;
    let quit = MenuItem::with_id(app, QUIT, "Quit Vireo", true, None::<&str>)?;

    Menu::with_items(app, &[&show, &hide, &sep1, &jobs, &sep2, &quit])
}

/// Handle a menu item click.
fn handle_menu_event(app: &AppHandle, id: &str) {
    match id {
        SHOW_WINDOW => show_main_window(app),
        HIDE_WINDOW => hide_main_window(app),
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
    if let Some(tray) = app.tray_by_id("main-tray") {
        match build_menu(app, job_status) {
            Ok(menu) => {
                let _ = tray.set_menu(Some(menu));
            }
            Err(e) => {
                log::warn!("Failed to rebuild tray menu: {}", e);
            }
        }
    }
}

/// Load the default tray icon (no badge).
fn load_icon_idle(app: &AppHandle) -> Image<'static> {
    let icon = app.default_window_icon().unwrap();
    Image::new_owned(icon.rgba().to_vec(), icon.width(), icon.height())
}

/// Create a "busy" tray icon by loading the badge variant.
/// Falls back to the default icon if the badge icon is missing.
fn load_icon_busy(app: &AppHandle) -> Image<'static> {
    match Image::from_path("icons/tray-busy.png") {
        Ok(img) => img,
        Err(_) => load_icon_idle(app),
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

                // Switch tray icon based on job activity
                if let Some(tray) = app.tray_by_id("main-tray") {
                    let icon = if count > 0 {
                        load_icon_busy(&app)
                    } else {
                        load_icon_idle(&app)
                    };
                    let _ = tray.set_icon(Some(icon));
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
