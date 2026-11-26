use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tauri::window::{ProgressBarState, ProgressBarStatus};
use tauri::{AppHandle, Manager};
use tauri_plugin_dialog::{
    DialogExt, MessageDialogButtons, MessageDialogKind, MessageDialogResult,
};
use tauri_plugin_updater::{Update, UpdaterExt};

use crate::sidecar::SidecarState;

const CHECK_TEXT: &str = "Check for Updates...";
const READY_TEXT: &str = "Restart to Update…";

/// What choosing Later means; see `install_pending_on_exit`.
#[cfg(not(windows))]
const LATER_NOTE: &str = " If you choose Later, the update installs when you quit Vireo.";
#[cfg(windows)]
const LATER_NOTE: &str = "";

const AFTER_JOBS_BUTTON: &str = "Restart After Jobs Finish";
const RESTART_NOW_BUTTON: &str = "Restart Now";
const RESTART_BUTTON: &str = "Restart to Update";
const LATER_BUTTON: &str = "Later";

/// How often the waiter asks the backend whether the user's jobs are done.
const READINESS_POLL: Duration = Duration::from_secs(5);
const READINESS_TIMEOUT: Duration = Duration::from_secs(10);
/// Consecutive failed readiness checks after which the waiter stops waiting.
/// A backend that cannot answer for this long has no job worth protecting,
/// and the countdown still gives the user a chance to cancel.
const MAX_READINESS_FAILURES: u32 = 12;
/// The warning shown before an automatic restart.
const COUNTDOWN: Duration = Duration::from_secs(30);
/// How long the page has to show the countdown before the native dialog
/// asks instead. An automatic restart never happens without a visible warning.
const COUNTDOWN_SHOWN_TIMEOUT: Duration = Duration::from_secs(3);
const COUNTDOWN_TICK: Duration = Duration::from_millis(250);

struct PendingUpdate {
    update: Update,
    bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CountdownResponse {
    Now,
    Cancel,
}

struct Countdown {
    version: String,
    deadline: Instant,
    shown: bool,
    response: Option<CountdownResponse>,
}

/// Keep deferred downloads for this session, and serialize checks through the
/// restart decision so another check cannot download or prompt a second time.
///
/// `after_jobs` is set while the user waits for their jobs to finish before
/// restarting. `waiter` is a generation number: whoever takes the restart
/// decision away from a waiting thread bumps it, and the thread exits when it
/// sees a generation that is not its own.
struct UpdateState<T> {
    running: bool,
    user_visible: bool,
    pending: Option<T>,
    after_jobs: bool,
    waiter: u64,
    countdown: Option<Countdown>,
}

impl<T> UpdateState<T> {
    const fn new() -> Self {
        Self {
            running: false,
            user_visible: false,
            pending: None,
            after_jobs: false,
            waiter: 0,
            countdown: None,
        }
    }

    fn begin(&mut self, user_initiated: bool) -> bool {
        if self.running {
            self.user_visible |= user_initiated;
            return false;
        }
        // Once the user chooses Later or to wait for their jobs, only an
        // explicit menu request should offer that download again. Background
        // checks leave it ready.
        if self.pending.is_some() && !user_initiated {
            return false;
        }
        self.running = true;
        self.user_visible = user_initiated;
        true
    }

    fn finish(&mut self) -> bool {
        self.running = false;
        std::mem::take(&mut self.user_visible)
    }

    /// Take the restart decision away from any waiting thread. Returns
    /// whether the user had been waiting for their jobs.
    fn stop_waiting(&mut self) -> bool {
        self.waiter += 1;
        self.countdown = None;
        std::mem::take(&mut self.after_jobs)
    }

    /// Start waiting for the user's jobs with `pending`; returns the
    /// generation the new waiting thread owns.
    fn wait_for_jobs(&mut self, pending: T) -> u64 {
        self.stop_waiting();
        self.pending = Some(pending);
        self.after_jobs = true;
        self.waiter
    }

    fn is_waiter(&self, generation: u64) -> bool {
        self.after_jobs && self.waiter == generation && self.pending.is_some()
    }

    fn menu_text(&self) -> &'static str {
        if self.after_jobs {
            "Restart to Update Now…"
        } else if self.pending.is_some() {
            READY_TEXT
        } else {
            CHECK_TEXT
        }
    }
}

static STATE: Mutex<UpdateState<PendingUpdate>> = Mutex::new(UpdateState::new());

fn state() -> std::sync::MutexGuard<'static, UpdateState<PendingUpdate>> {
    STATE.lock().unwrap_or_else(|e| e.into_inner())
}

/// Download in the background and ask once, when the update is ready to install.
/// Manual checks also report no-update and error results. Repeated clicks promote
/// an existing check to user-visible without opening another progress dialog.
pub fn spawn_update_check(app: &AppHandle, user_initiated: bool) {
    let (pending, was_waiting, had_countdown) = {
        let mut state = state();
        if !state.begin(user_initiated) {
            return;
        }
        let had_countdown = state.countdown.is_some();
        let was_waiting = state.stop_waiting();
        (state.pending.take(), was_waiting, had_countdown)
    };
    if had_countdown {
        refresh_countdown_banner(app);
    }

    set_update_menu_text(app, "Checking for Updates…");
    let handle = app.clone();
    tauri::async_runtime::spawn(async move {
        let result = match pending {
            Some(pending) => Ok(Some(pending)),
            // The boxed error is not Send, and the prompt below is awaited.
            None => download_update(&handle).await.map_err(|e| e.to_string()),
        };
        let result = match result {
            Ok(Some(pending)) => {
                // The prompt asks the backend about jobs over blocking HTTP and
                // blocks on a native dialog, so keep both off the async workers.
                let prompt_handle = handle.clone();
                let _ = tauri::async_runtime::spawn_blocking(move || {
                    offer_restart(&prompt_handle, pending, was_waiting)
                })
                .await;
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(error) => Err(error),
        };
        // Restore the menu before releasing the check guard. Never hold the
        // mutex during a native UI call, which may dispatch to the main thread.
        // A waiting thread keeps the menu naming the job it waits for.
        let (waiting, text) = {
            let state = state();
            (state.after_jobs, state.menu_text())
        };
        if !waiting {
            set_update_menu_text(&handle, text);
        }
        let visible = state().finish();
        match result {
            Ok(false) if visible => {
                handle
                    .dialog()
                    .message(format!(
                        "You're running the latest version of Vireo (v{}).",
                        env!("CARGO_PKG_VERSION")
                    ))
                    .title("Up to Date")
                    .kind(MessageDialogKind::Info)
                    .show(|_| {});
            }
            Err(error) => {
                log::error!("Update check or download failed: {error}");
                if visible {
                    handle
                        .dialog()
                        .message(format!("Could not check for or download updates:\n{error}"))
                        .title("Update Error")
                        .kind(MessageDialogKind::Error)
                        .show(|_| {});
                }
            }
            _ => {}
        }
    });
}

async fn download_update(
    app: &AppHandle,
) -> Result<Option<PendingUpdate>, Box<dyn std::error::Error>> {
    let updater = app.updater()?;
    let update = match updater.check().await {
        Ok(u) => u,
        // A missing platform key in the manifest means the release pipeline
        // didn't publish an artifact for the current OS/arch (e.g. macOS
        // notarization failed mid-release). That's a broken release for the
        // developer to fix — not something to alarm end users about. Log
        // loudly so it shows up in vireo.log, then fall through to the
        // normal "no update available" path.
        Err(
            e @ (tauri_plugin_updater::Error::TargetNotFound(_)
            | tauri_plugin_updater::Error::TargetsNotFound(_)),
        ) => {
            log::error!("Update manifest missing platform entry: {e}");
            None
        }
        Err(e) => return Err(e.into()),
    };

    let cache_dir = package_cache_dir(app);
    let Some(update) = update else {
        log::info!("No update available");
        if let Some(dir) = &cache_dir {
            prune_packages(dir, None);
        }
        return Ok(None);
    };

    let version = update.version.clone();
    log::info!("Update available: v{version}");
    if let Some(dir) = &cache_dir {
        prune_packages(dir, Some(&version));
        if let Some(bytes) = load_package(dir, &version) {
            // install() verifies again; checking here discards a damaged
            // file before the user is asked to restart for it.
            match update.verify(&bytes) {
                Ok(()) => {
                    log::info!("Using update v{version} downloaded earlier");
                    return Ok(Some(PendingUpdate { update, bytes }));
                }
                Err(error) => {
                    log::warn!("Discarding saved update v{version}: {error}");
                    remove_package(dir, &version);
                }
            }
        }
    }

    log::info!("Downloading update v{version}");
    set_update_menu_text(app, "Downloading Update…");
    set_update_progress(app, ProgressBarStatus::Indeterminate, Some(10));

    let progress_handle = app.clone();
    let finish_handle = app.clone();
    let mut downloaded = 0_u64;
    let mut last_progress = 0_u64;
    let download_result = update
        .download(
            move |chunk_len, content_len| {
                downloaded = downloaded.saturating_add(chunk_len as u64);
                let Some(total) = content_len.filter(|total| *total > 0) else {
                    return;
                };
                let progress = downloaded.saturating_mul(100) / total;
                if progress != last_progress {
                    last_progress = progress;
                    set_update_menu_text(
                        &progress_handle,
                        &format!("Downloading Update… {}%", progress.min(100)),
                    );
                    set_update_progress(
                        &progress_handle,
                        ProgressBarStatus::Normal,
                        Some(progress.min(100)),
                    );
                }
            },
            move || {
                log::info!("Update download complete; verifying");
                set_update_menu_text(&finish_handle, "Verifying Update…");
                set_update_progress(&finish_handle, ProgressBarStatus::Indeterminate, Some(100));
            },
        )
        .await;
    clear_update_progress(app);

    let bytes = download_result?;
    if let Some(dir) = &cache_dir {
        // Saved so that choosing Later survives a quit or crash. A failure
        // only costs a second download.
        if let Err(error) = save_package(dir, &version, &bytes) {
            log::warn!("Could not save update v{version} for later: {error}");
        }
    }
    Ok(Some(PendingUpdate { update, bytes }))
}

/// Ask whether to restart for `pending`. When jobs the user started are still
/// running, the default answer waits for them to finish, and Restart Now names
/// what it would stop.
fn offer_restart(app: &AppHandle, pending: PendingUpdate, was_waiting: bool) {
    set_update_menu_text(app, READY_TEXT);
    let version = pending.update.version.clone();
    let readiness = fetch_readiness(app);
    if let Err(error) = &readiness {
        log::warn!("Could not check for running jobs before updating: {error}");
    }

    let busy = matches!(&readiness, Ok(r) if !r.idle);
    if !busy {
        let restart = app
            .dialog()
            .message(ready_message(&version, readiness.as_ref().ok()))
            .title("Update Ready")
            .kind(MessageDialogKind::Info)
            .buttons(MessageDialogButtons::OkCancelCustom(
                RESTART_BUTTON.into(),
                LATER_BUTTON.into(),
            ))
            .blocking_show();
        if restart {
            install_and_restart(app, pending);
        } else {
            state().pending = Some(pending);
        }
        return;
    }

    let readiness = readiness.expect("busy implies a readiness answer");
    let choice = app
        .dialog()
        .message(busy_message(&version, &readiness, was_waiting))
        .title("Update Ready")
        .kind(MessageDialogKind::Info)
        .buttons(MessageDialogButtons::YesNoCancelCustom(
            AFTER_JOBS_BUTTON.into(),
            RESTART_NOW_BUTTON.into(),
            LATER_BUTTON.into(),
        ))
        .blocking_show_with_result();

    match busy_choice(&choice) {
        BusyChoice::AfterJobs => {
            let generation = state().wait_for_jobs(pending);
            set_update_menu_text(app, &waiting_menu_text(&readiness.blocking));
            let handle = app.clone();
            std::thread::spawn(move || wait_for_jobs(&handle, generation));
        }
        BusyChoice::Now => install_and_restart(app, pending),
        BusyChoice::Later => state().pending = Some(pending),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum BusyChoice {
    AfterJobs,
    Now,
    Later,
}

fn busy_choice(result: &MessageDialogResult) -> BusyChoice {
    match result {
        MessageDialogResult::Yes => BusyChoice::AfterJobs,
        MessageDialogResult::No => BusyChoice::Now,
        MessageDialogResult::Custom(label) if label == AFTER_JOBS_BUTTON => BusyChoice::AfterJobs,
        MessageDialogResult::Custom(label) if label == RESTART_NOW_BUTTON => BusyChoice::Now,
        // Later, a closed dialog, or anything unexpected keeps the download
        // and changes nothing.
        _ => BusyChoice::Later,
    }
}

/// Install `pending` and restart, or put it back and report the failure.
fn install_and_restart(app: &AppHandle, pending: PendingUpdate) {
    // Windows installation exits the process, so consent must come before
    // install() on every platform, not just before app.restart().
    set_update_menu_text(app, "Installing Update…");
    set_update_progress(app, ProgressBarStatus::Indeterminate, Some(100));
    let result = pending.update.install(&pending.bytes);
    clear_update_progress(app);
    match result {
        Ok(()) => {
            if let Some(dir) = package_cache_dir(app) {
                prune_packages(&dir, None);
            }
            app.restart();
        }
        Err(error) => {
            log::error!("Update installation failed: {error}");
            state().pending = Some(pending);
            set_update_menu_text(app, READY_TEXT);
            app.dialog()
                .message(format!("Could not install the update:\n{error}"))
                .title("Update Error")
                .kind(MessageDialogKind::Error)
                .show(|_| {});
        }
    }
}

/// Install a downloaded update while Vireo quits, so choosing Later never
/// costs anything: the next launch runs the new version. Runs after the
/// backend has stopped, since installation replaces its files.
///
/// Not on Windows, where installing runs the installer, which shows its own
/// window and can relaunch Vireo right after the user quit it. There the
/// saved download is offered again at the next launch without downloading.
#[cfg(not(windows))]
pub fn install_pending_on_exit(app: &AppHandle) {
    let pending = {
        let mut state = state();
        // A running check or prompt owns its download; the saved copy covers
        // it at the next launch.
        if state.running {
            return;
        }
        state.stop_waiting();
        state.pending.take()
    };
    let Some(pending) = pending else {
        return;
    };
    log::info!("Installing update v{} on quit", pending.update.version);
    match pending.update.install(&pending.bytes) {
        Ok(()) => {
            if let Some(dir) = package_cache_dir(app) {
                prune_packages(&dir, None);
            }
        }
        Err(error) => log::error!("Could not install update on quit: {error}"),
    }
}

#[cfg(windows)]
pub fn install_pending_on_exit(_app: &AppHandle) {}

// ---------------------------------------------------------------------------
// Waiting for the user's jobs

#[derive(Debug, Deserialize)]
struct Readiness {
    idle: bool,
    #[serde(default)]
    blocking: Vec<ReadinessJob>,
    #[serde(default)]
    paused: Vec<ReadinessJob>,
}

#[derive(Debug, Deserialize)]
struct ReadinessJob {
    name: String,
    #[serde(default)]
    status: String,
}

/// Ask the backend whether restarting now would stop work the user started
/// (`/api/v1/update-readiness`; the rules live in `services/update_readiness.py`).
fn fetch_readiness(app: &AppHandle) -> Result<Readiness, String> {
    let sidecar = app
        .try_state::<SidecarState>()
        .ok_or_else(|| "the backend is not running".to_string())?;
    let url = format!("http://127.0.0.1:{}/api/v1/update-readiness", sidecar.port);
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_connect(Some(Duration::from_secs(2)))
        .timeout_recv_response(Some(READINESS_TIMEOUT))
        .build()
        .into();
    let mut request = agent.get(&url);
    if let Some(token) = &sidecar.token {
        request = request.header("X-Vireo-Token", token);
    }
    let mut response = request.call().map_err(|e| e.to_string())?;
    let body = response
        .body_mut()
        .read_to_string()
        .map_err(|e| e.to_string())?;
    serde_json::from_str(&body).map_err(|e| e.to_string())
}

/// Poll the backend until the user's jobs are done, warn with a countdown,
/// then install and restart. Exits as soon as the decision changes hands.
fn wait_for_jobs(app: &AppHandle, mut generation: u64) {
    let mut failures = 0_u32;
    loop {
        // The prompt that started this wait has just checked readiness.
        std::thread::sleep(READINESS_POLL);
        if !state().is_waiter(generation) {
            return;
        }
        let idle = match fetch_readiness(app) {
            Ok(readiness) if !readiness.idle => {
                failures = 0;
                set_update_menu_text(app, &waiting_menu_text(&readiness.blocking));
                false
            }
            Ok(_) => true,
            Err(error) => {
                failures += 1;
                log::warn!("Update readiness check failed ({failures}): {error}");
                if failures < MAX_READINESS_FAILURES {
                    set_update_menu_text(app, "Restart to Update Now (can't check jobs)…");
                    false
                } else {
                    log::warn!(
                        "Backend unreachable while waiting to update; starting the countdown"
                    );
                    true
                }
            }
        };
        if idle {
            match run_countdown(app, generation) {
                CountdownOutcome::Restart(pending) => {
                    install_and_restart(app, *pending);
                    state().finish();
                    return;
                }
                CountdownOutcome::Stopped => return,
                CountdownOutcome::Busy(next) => {
                    generation = next;
                    failures = 0;
                }
            }
        }
    }
}

enum CountdownOutcome {
    /// The countdown ended or the user chose Restart Now; the caller owns
    /// the check guard and the download.
    Restart(Box<PendingUpdate>),
    /// The user cancelled, or another prompt took over.
    Stopped,
    /// A job started during the countdown; keep waiting as this generation.
    Busy(u64),
}

/// Warn before an automatic restart. The page shows the countdown; when it
/// cannot (browser mode, a page without the navbar), a native dialog asks
/// instead and nothing restarts until the user answers.
fn run_countdown(app: &AppHandle, generation: u64) -> CountdownOutcome {
    let version = {
        let mut state = state();
        if !state.is_waiter(generation) {
            return CountdownOutcome::Stopped;
        }
        let version = state.pending.as_ref().map(|p| p.update.version.clone());
        state.countdown = Some(Countdown {
            version: version.clone().unwrap_or_default(),
            deadline: Instant::now() + COUNTDOWN,
            shown: false,
            response: None,
        });
        version.unwrap_or_default()
    };
    let started = Instant::now();
    let page_can_show = !crate::tray::is_browser_mode(app);
    if page_can_show {
        refresh_countdown_banner(app);
    }

    let mut last_seconds = None;
    let ask_natively = loop {
        if !page_can_show {
            break true;
        }
        std::thread::sleep(COUNTDOWN_TICK);
        let state = state();
        if !state.is_waiter(generation) {
            return CountdownOutcome::Stopped;
        }
        let Some(countdown) = state.countdown.as_ref() else {
            return CountdownOutcome::Stopped;
        };
        let now = Instant::now();
        match countdown.response {
            Some(CountdownResponse::Cancel) => {
                drop(state);
                stop_waiting_keep_download(app);
                return CountdownOutcome::Stopped;
            }
            Some(CountdownResponse::Now) => break false,
            None if now >= countdown.deadline => break false,
            None if !countdown.shown && now.duration_since(started) >= COUNTDOWN_SHOWN_TIMEOUT => {
                break true;
            }
            None => {}
        }
        let seconds = countdown.deadline.saturating_duration_since(now).as_secs() + 1;
        drop(state);
        if last_seconds != Some(seconds) {
            last_seconds = Some(seconds);
            set_update_menu_text(app, &format!("Restarting to Update in {seconds}s…"));
        }
    };

    // Take the check guard so a menu click during the last steps is folded
    // into this decision instead of opening a second prompt.
    let pending = {
        let mut state = state();
        if !state.is_waiter(generation) {
            return CountdownOutcome::Stopped;
        }
        if state.running {
            // A check that has not taken the decision yet; try again on the
            // next poll rather than leave nobody waiting.
            state.countdown = None;
            drop(state);
            refresh_countdown_banner(app);
            return CountdownOutcome::Busy(generation);
        }
        state.begin(true);
        state.stop_waiting();
        state.pending.take()
    };
    if page_can_show {
        refresh_countdown_banner(app);
    }
    let Some(pending) = pending else {
        state().finish();
        return CountdownOutcome::Stopped;
    };

    if ask_natively {
        set_update_menu_text(app, READY_TEXT);
        let restart = app
            .dialog()
            .message(format!(
                "Your jobs have finished, and Vireo v{version} is ready to install.\n\n\
                 Restart Vireo now to finish updating, or choose Later to keep working."
            ))
            .title("Update Ready")
            .kind(MessageDialogKind::Info)
            .buttons(MessageDialogButtons::OkCancelCustom(
                RESTART_BUTTON.into(),
                LATER_BUTTON.into(),
            ))
            .blocking_show();
        if !restart {
            state().pending = Some(pending);
            set_update_menu_text(app, READY_TEXT);
            state().finish();
            return CountdownOutcome::Stopped;
        }
    }

    // A follow-up job (the Process run an import queues) can start in the
    // moments after the last one ends. Check once more before stopping it.
    if let Ok(readiness) = fetch_readiness(app) {
        if !readiness.idle {
            let next = {
                let mut state = state();
                state.finish();
                state.wait_for_jobs(pending)
            };
            set_update_menu_text(app, &waiting_menu_text(&readiness.blocking));
            return CountdownOutcome::Busy(next);
        }
    }
    CountdownOutcome::Restart(Box::new(pending))
}

fn stop_waiting_keep_download(app: &AppHandle) {
    let text = {
        let mut state = state();
        state.stop_waiting();
        state.menu_text()
    };
    set_update_menu_text(app, text);
    refresh_countdown_banner(app);
}

/// Ask the page to read the countdown again (`refreshUpdateRestartCountdown`
/// in tauri-bridge.js), which shows, updates or removes its banner.
fn refresh_countdown_banner(app: &AppHandle) {
    if crate::tray::is_browser_mode(app) {
        return;
    }
    if let Some(window) = app.get_webview_window("main") {
        let js = "if (typeof window.refreshUpdateRestartCountdown === 'function') { window.refreshUpdateRestartCountdown(); }";
        if let Err(error) = window.eval(js) {
            log::debug!("Could not refresh the update countdown banner: {error}");
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct CountdownView {
    version: String,
    seconds_left: u64,
}

/// The countdown the page should show, if one is running. Calling it marks
/// the countdown as shown, which is what lets the restart happen on its own.
#[tauri::command]
pub fn update_restart_countdown() -> Option<CountdownView> {
    let mut state = state();
    let countdown = state.countdown.as_mut()?;
    countdown.shown = true;
    Some(CountdownView {
        version: countdown.version.clone(),
        seconds_left: countdown
            .deadline
            .saturating_duration_since(Instant::now())
            .as_secs(),
    })
}

/// The page's answer to the countdown: `"now"` restarts at once, `"cancel"`
/// stops waiting and keeps the download for later.
#[tauri::command]
pub fn respond_update_restart(action: String) -> Result<(), String> {
    let response = match action.as_str() {
        "now" => CountdownResponse::Now,
        "cancel" => CountdownResponse::Cancel,
        other => return Err(format!("unknown update response {other:?}")),
    };
    let mut state = state();
    let countdown = state
        .countdown
        .as_mut()
        .ok_or_else(|| "no update restart is pending".to_string())?;
    countdown.response = Some(response);
    Ok(())
}

// ---------------------------------------------------------------------------
// Prompt and menu text

fn describe_jobs(jobs: &[ReadinessJob]) -> String {
    jobs.iter()
        .map(|job| match job.status.as_str() {
            "queued" => format!("{} (waiting to start)", job.name),
            "pausing" => format!("{} (pausing)", job.name),
            _ => job.name.clone(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn paused_note(paused: &[ReadinessJob]) -> String {
    if paused.is_empty() {
        return String::new();
    }
    let names = paused
        .iter()
        .map(|j| j.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    format!("\n\nPaused jobs stop when Vireo restarts: {names}.")
}

fn ready_message(version: &str, readiness: Option<&Readiness>) -> String {
    let note = match readiness {
        Some(readiness) => paused_note(&readiness.paused),
        None => "\n\nVireo couldn't check for running jobs. A restart stops any that are running."
            .to_string(),
    };
    format!(
        "Vireo v{version} has been downloaded and is ready to install.\n\n\
         Restart Vireo to finish updating, or choose Later to keep working.{LATER_NOTE}{note}"
    )
}

fn busy_message(version: &str, readiness: &Readiness, was_waiting: bool) -> String {
    let lead = if was_waiting {
        format!(
            "Vireo is waiting for these jobs to finish before it restarts to install v{version}:"
        )
    } else {
        format!("Vireo v{version} is ready to install, but these jobs haven't finished:")
    };
    format!(
        "{lead}\n\n{}\n\n\
         Restart After Jobs Finish waits for them, then restarts after a 30-second warning. \
         Restart Now stops them.{}",
        describe_jobs(&readiness.blocking),
        paused_note(&readiness.paused),
    )
}

fn waiting_menu_text(blocking: &[ReadinessJob]) -> String {
    match blocking {
        [] => "Restart to Update Now…".to_string(),
        [job] => format!("Restart to Update Now (waiting for {})…", job.name),
        [job, rest @ ..] => format!(
            "Restart to Update Now (waiting for {} and {} more)…",
            job.name,
            rest.len()
        ),
    }
}

// ---------------------------------------------------------------------------
// The downloaded package on disk

fn package_cache_dir(app: &AppHandle) -> Option<PathBuf> {
    match app.path().app_cache_dir() {
        Ok(dir) => Some(dir.join("updates")),
        Err(error) => {
            log::warn!("No cache folder for downloaded updates: {error}");
            None
        }
    }
}

fn package_path(dir: &Path, version: &str) -> PathBuf {
    // Versions come from the signed manifest; keep the name to safe characters
    // anyway so a malformed one cannot point outside the folder.
    let safe: String = version
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    dir.join(format!("vireo-{safe}.update"))
}

fn save_package(dir: &Path, version: &str, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let path = package_path(dir, version);
    let partial = path.with_extension("partial");
    std::fs::write(&partial, bytes)?;
    std::fs::rename(&partial, &path)
}

fn load_package(dir: &Path, version: &str) -> Option<Vec<u8>> {
    std::fs::read(package_path(dir, version)).ok()
}

fn remove_package(dir: &Path, version: &str) {
    let _ = std::fs::remove_file(package_path(dir, version));
}

/// Remove every saved package except `keep`'s, including interrupted writes.
fn prune_packages(dir: &Path, keep: Option<&str>) {
    let keep = keep.map(|version| package_path(dir, version));
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if Some(&path) == keep.as_ref() {
            continue;
        }
        if let Err(error) = std::fs::remove_file(&path) {
            log::debug!("Could not remove old update {}: {error}", path.display());
        }
    }
}

// ---------------------------------------------------------------------------
// Native indicators

fn set_update_menu_text(app: &AppHandle, text: &str) {
    // Menu::get only searches direct children; the updater lives in Help.
    if let Some(item) = app
        .menu()
        .and_then(|menu| menu.items().ok())
        .and_then(|items| {
            items.into_iter().find_map(|item| {
                item.as_submenu()
                    .and_then(|submenu| submenu.get(crate::menu::ids::CHECK_FOR_UPDATES))
            })
        })
    {
        if let Some(item) = item.as_menuitem() {
            if let Err(error) = item.set_text(text) {
                log::debug!("Could not update updater menu status: {error}");
            }
        }
    }
}

fn set_update_progress(app: &AppHandle, status: ProgressBarStatus, progress: Option<u64>) {
    if let Some(window) = app.get_webview_window("main") {
        if let Err(e) = window.set_progress_bar(ProgressBarState {
            status: Some(status),
            progress,
        }) {
            log::debug!("Could not update updater progress indicator: {e}");
        }
    }
}

fn clear_update_progress(app: &AppHandle) {
    set_update_progress(app, ProgressBarStatus::None, None);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(name: &str, status: &str) -> ReadinessJob {
        ReadinessJob {
            name: name.into(),
            status: status.into(),
        }
    }

    #[test]
    fn manual_request_promotes_background_check_without_starting_another() {
        let mut state = UpdateState::<()>::new();
        assert!(state.begin(false));
        assert!(!state.begin(true));
        assert!(!state.begin(false));
        assert!(state.finish());

        // Visibility belongs to this check, not the next background check.
        assert!(state.begin(false));
        assert!(!state.finish());
    }

    #[test]
    fn deferred_download_waits_for_manual_request_and_is_reused() {
        let mut state = UpdateState::new();
        assert!(state.begin(false));
        state.pending = Some(vec![1, 2, 3]);
        state.finish();

        // Scheduled checks must neither prompt again nor redownload.
        assert!(!state.begin(false));
        assert_eq!(state.pending.as_deref(), Some([1, 2, 3].as_slice()));

        assert!(state.begin(true));
        assert_eq!(state.pending.take(), Some(vec![1, 2, 3]));
        // The guard also covers the restart prompt and installation.
        assert!(!state.begin(true));
        assert!(!state.begin(false));
        assert!(state.finish());
    }

    #[test]
    fn completed_check_allows_retry() {
        let mut state = UpdateState::<()>::new();
        assert!(state.begin(true));
        assert!(state.finish());
        assert!(state.begin(true));
        assert!(state.finish());
    }

    #[test]
    fn background_checks_leave_a_waiting_update_alone() {
        let mut state = UpdateState::new();
        let generation = state.wait_for_jobs(7);
        assert!(state.is_waiter(generation));
        assert!(!state.begin(false));
        assert!(state.is_waiter(generation));
        assert_eq!(state.menu_text(), "Restart to Update Now…");
    }

    #[test]
    fn menu_request_takes_the_decision_from_the_waiter() {
        let mut state = UpdateState::new();
        let generation = state.wait_for_jobs(7);
        assert!(state.begin(true));
        assert!(state.stop_waiting());
        assert!(!state.is_waiter(generation));
        assert_eq!(state.pending, Some(7));
        assert_eq!(state.menu_text(), READY_TEXT);

        // Choosing to wait again starts a new generation; the old thread
        // still sees it is no longer the waiter.
        let pending = state.pending.take().unwrap();
        let next = state.wait_for_jobs(pending);
        assert_ne!(next, generation);
        assert!(state.is_waiter(next));
        assert!(!state.is_waiter(generation));
    }

    #[test]
    fn stop_waiting_clears_the_countdown() {
        let mut state = UpdateState::new();
        state.wait_for_jobs(1);
        state.countdown = Some(Countdown {
            version: "1.0.0".into(),
            deadline: Instant::now(),
            shown: true,
            response: None,
        });
        assert!(state.stop_waiting());
        assert!(state.countdown.is_none());
        assert!(!state.stop_waiting());
    }

    #[test]
    fn busy_dialog_answers() {
        assert_eq!(
            busy_choice(&MessageDialogResult::Yes),
            BusyChoice::AfterJobs
        );
        assert_eq!(busy_choice(&MessageDialogResult::No), BusyChoice::Now);
        assert_eq!(
            busy_choice(&MessageDialogResult::Custom(AFTER_JOBS_BUTTON.into())),
            BusyChoice::AfterJobs
        );
        assert_eq!(
            busy_choice(&MessageDialogResult::Custom(RESTART_NOW_BUTTON.into())),
            BusyChoice::Now
        );
        assert_eq!(
            busy_choice(&MessageDialogResult::Custom(LATER_BUTTON.into())),
            BusyChoice::Later
        );
        // Closing the dialog must never restart.
        assert_eq!(busy_choice(&MessageDialogResult::Cancel), BusyChoice::Later);
    }

    #[test]
    fn waiting_menu_names_the_job() {
        assert_eq!(
            waiting_menu_text(&[job("Import", "running")]),
            "Restart to Update Now (waiting for Import)…"
        );
        assert_eq!(
            waiting_menu_text(&[job("Import", "running"), job("Process run", "queued")]),
            "Restart to Update Now (waiting for Import and 1 more)…"
        );
    }

    #[test]
    fn busy_message_names_jobs_and_paused_ones() {
        let readiness = Readiness {
            idle: false,
            blocking: vec![job("Import", "running"), job("Process run", "queued")],
            paused: vec![job("Move folder", "paused")],
        };
        let message = busy_message("1.2.3", &readiness, false);
        assert!(message.contains("Import, Process run (waiting to start)"));
        assert!(message.contains("Paused jobs stop when Vireo restarts: Move folder."));
        assert!(busy_message("1.2.3", &readiness, true).starts_with("Vireo is waiting"));
    }

    #[test]
    fn ready_message_says_when_jobs_could_not_be_checked() {
        assert!(ready_message("1.2.3", None).contains("couldn't check for running jobs"));
        let idle = Readiness {
            idle: true,
            blocking: vec![],
            paused: vec![],
        };
        assert!(!ready_message("1.2.3", Some(&idle)).contains("couldn't check"));
    }

    #[test]
    fn readiness_parses_the_endpoint_body() {
        let body = r#"{"idle": false, "blocking": [{"id": "a", "type": "import-full",
            "name": "Import", "status": "running"}], "paused": []}"#;
        let readiness: Readiness = serde_json::from_str(body).unwrap();
        assert!(!readiness.idle);
        assert_eq!(readiness.blocking[0].name, "Import");
    }

    #[test]
    fn saved_package_round_trips_and_old_versions_are_pruned() {
        let dir = std::env::temp_dir().join(format!("vireo-update-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        save_package(&dir, "1.0.0", b"old").unwrap();
        save_package(&dir, "1.1.0", b"new").unwrap();
        std::fs::write(dir.join("vireo-1.2.0.partial"), b"interrupted").unwrap();

        prune_packages(&dir, Some("1.1.0"));
        assert_eq!(
            load_package(&dir, "1.1.0").as_deref(),
            Some(b"new".as_slice())
        );
        assert!(load_package(&dir, "1.0.0").is_none());
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);

        prune_packages(&dir, None);
        assert!(load_package(&dir, "1.1.0").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn package_names_stay_inside_the_folder() {
        let dir = Path::new("/tmp/updates");
        assert_eq!(
            package_path(dir, "../../x"),
            dir.join("vireo-.._.._x.update")
        );
    }
}
