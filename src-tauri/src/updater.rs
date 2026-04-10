use std::sync::atomic::{AtomicBool, Ordering};

use tauri::AppHandle;
use tauri_plugin_dialog::{DialogExt, MessageDialogButtons, MessageDialogKind};
use tauri_plugin_updater::UpdaterExt;

/// Prevents overlapping update checks from running simultaneously.
static CHECKING: AtomicBool = AtomicBool::new(false);

/// Spawn an update check on a background async task.
///
/// When `user_initiated` is true, a dialog is shown even when no update
/// is available or when the check fails. Background (automatic) checks
/// stay silent on "no update" and log errors without bothering the user.
///
/// If a check is already in progress the call is silently ignored.
pub fn spawn_update_check(app: &AppHandle, user_initiated: bool) {
    // Atomically set the flag; if it was already true another check is running.
    if CHECKING.swap(true, Ordering::SeqCst) {
        log::debug!("Update check already in progress, skipping");
        return;
    }

    let handle = app.clone();
    tauri::async_runtime::spawn(async move {
        match do_update_check(&handle, user_initiated).await {
            Ok(()) => {}
            Err(e) => {
                log::error!("Update check failed: {e}");
                if user_initiated {
                    handle
                        .dialog()
                        .message(format!("Could not check for updates:\n{e}"))
                        .title("Update Error")
                        .kind(MessageDialogKind::Error)
                        .show(|_| {});
                }
            }
        }
        CHECKING.store(false, Ordering::SeqCst);
    });
}

async fn do_update_check(
    app: &AppHandle,
    user_initiated: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let update = app.updater()?.check().await?;

    match update {
        Some(update) => {
            let version = update.version.clone();
            log::info!("Update available: v{version}");

            update
                .download_and_install(|_chunk_len, _content_len| {}, || {})
                .await?;

            let handle = app.clone();
            app.dialog()
                .message(format!(
                    "Vireo v{version} has been downloaded.\n\nRestart now to update?"
                ))
                .title("Update Ready")
                .kind(MessageDialogKind::Info)
                .buttons(MessageDialogButtons::OkCancel)
                .show(move |restart| {
                    if restart {
                        handle.restart();
                    }
                });

            Ok(())
        }
        None => {
            log::info!("No update available");
            if user_initiated {
                app.dialog()
                    .message(format!(
                        "You're running the latest version of Vireo (v{}).",
                        env!("CARGO_PKG_VERSION")
                    ))
                    .title("Up to Date")
                    .kind(MessageDialogKind::Info)
                    .show(|_| {});
            }
            Ok(())
        }
    }
}
