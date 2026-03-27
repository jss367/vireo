use tauri::{
    AppHandle, Manager,
    menu::{Menu, MenuItem, PredefinedMenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
};

/// Menu item IDs
const SHOW_WINDOW: &str = "show_window";
const HIDE_WINDOW: &str = "hide_window";
const JOB_STATUS: &str = "job_status";
const QUIT: &str = "quit";

/// Build the tray icon with its context menu.
pub fn create_tray(app: &AppHandle) -> tauri::Result<()> {
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
