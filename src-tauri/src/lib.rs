mod menu;
mod sidecar;
mod tray;
use sidecar::SidecarState;
use tauri::{Manager, RunEvent};

#[tauri::command]
fn get_server_port(state: tauri::State<'_, SidecarState>) -> u16 {
    state.port
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
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
                        if let Some(window) = app.get_webview_window("main") {
                            let url = format!("http://127.0.0.1:{}", port);
                            let _ = window.navigate(url.parse().unwrap());
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
            tray::create_tray(app.handle(), port)?;
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

            // "Report an Issue" — open GitHub issues in the default browser
            if id == menu::ids::REPORT_ISSUE {
                use tauri_plugin_opener::OpenerExt;
                let _ = app.opener().open_url("https://github.com/jss367/vireo/issues", None::<&str>);
                return;
            }

            // Navigation items — evaluate JS in the main webview
            if let Some(route) = menu::route_for_id(id) {
                if let Some(window) = app.get_webview_window("main") {
                    let js = format!("window.location.href = '{}'", route);
                    if let Err(e) = window.eval(&js) {
                        log::error!("Failed to navigate to {}: {}", route, e);
                    }
                }
            }
        })
        .invoke_handler(tauri::generate_handler![
            get_server_port,
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app_handle, event| {
            if let RunEvent::Exit = event {
                if let Some(state) = app_handle.try_state::<SidecarState>() {
                    sidecar::stop_sidecar(&state);
                }
            }
        });
}
