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
const RESTART_TEXT: &str = "Restart to Update";
const WAITING_TEXT: &str = "Restart to Update Now…";

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
    deadline: Instant,
    shown: bool,
    response: Option<CountdownResponse>,
}

/// Keep deferred downloads for this session, and serialize checks through the
/// restart decision so another check cannot download or prompt a second time.
///
/// `after_jobs` is set while the user waits for their jobs to finish before
/// restarting, and `waiting_for` names those jobs. `waiter` is a generation
/// number: whoever takes the restart decision away from a waiting thread bumps
/// it, and the thread exits when it sees a generation that is not its own.
struct UpdateState<T> {
    running: bool,
    user_visible: bool,
    pending: Option<T>,
    after_jobs: bool,
    waiting_for: Option<String>,
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
            waiting_for: None,
            waiter: 0,
            countdown: None,
        }
    }

    fn begin(&mut self, user_initiated: bool) -> bool {
        if self.running {
            self.user_visible |= user_initiated;
            return false;
        }
        // A downloaded update stays ready until the user asks to restart;
        // background checks neither download it again nor announce it again.
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
        self.waiting_for = None;
        std::mem::take(&mut self.after_jobs)
    }

    /// Start waiting for the user's jobs with `pending`; returns the
    /// generation the new waiting thread owns.
    fn wait_for_jobs(&mut self, pending: T, waiting_for: String) -> u64 {
        self.stop_waiting();
        self.pending = Some(pending);
        self.after_jobs = true;
        self.waiting_for = Some(waiting_for);
        self.waiter
    }

    fn is_waiter(&self, generation: u64) -> bool {
        self.after_jobs && self.waiter == generation && self.pending.is_some()
    }

    fn menu_text(&self) -> &'static str {
        if self.after_jobs {
            WAITING_TEXT
        } else if self.pending.is_some() {
            RESTART_TEXT
        } else {
            CHECK_TEXT
        }
    }
}

static STATE: Mutex<UpdateState<PendingUpdate>> = Mutex::new(UpdateState::new());

fn state() -> std::sync::MutexGuard<'static, UpdateState<PendingUpdate>> {
    STATE.lock().unwrap_or_else(|e| e.into_inner())
}

/// The version of the downloaded update waiting for a restart, if any. The
/// tray menu reads it.
pub fn ready_version() -> Option<String> {
    state()
        .pending
        .as_ref()
        .map(|pending| pending.update.version.clone())
}

/// What the web UI shows about a downloaded update (`update_status`).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct UpdateStatus {
    version: String,
    /// The jobs Vireo is waiting for before it restarts, when the user chose
    /// to restart after they finish.
    waiting_for: Option<String>,
    /// Seconds until the automatic restart, while the countdown runs.
    restart_in: Option<u64>,
}

/// The downloaded update and what Vireo is doing about it. Reading a running
/// countdown marks it shown, which is what lets the restart happen on its own.
/// A webview out of sight (the main window closed to the tray or minimized)
/// still runs JS, so a read from one leaves it unmarked and the native dialog
/// asks instead: nothing restarts until a window the user can see showed the
/// warning.
pub fn status(app: &AppHandle) -> Option<UpdateStatus> {
    let visible = main_window_on_screen(app);
    let mut state = state();
    let version = state.pending.as_ref()?.update.version.clone();
    let waiting_for = state.waiting_for.clone();
    let restart_in = state.countdown.as_mut().map(|countdown| {
        countdown.shown |= visible;
        countdown
            .deadline
            .saturating_duration_since(Instant::now())
            .as_secs()
    });
    Some(UpdateStatus {
        version,
        waiting_for,
        restart_in,
    })
}

/// Download in the background without interrupting. A background download
/// only marks the update ready: the app menu item becomes "Restart to Update",
/// the tray menu gains the same item, and the web UI shows a notice under the
/// bell. Nothing is installed until the user asks. A manual check that finds
/// an update asks once, because the user is waiting for the answer, and also
/// reports no-update and error results. Asking for an update that is already
/// ready (the menu item, tray item or notice) restarts for it, after asking
/// when jobs the user started are still running (`decide_restart`).
/// Repeated clicks promote an existing check to user-visible without opening
/// another progress dialog.
pub fn spawn_update_check(app: &AppHandle, user_initiated: bool) {
    let (pending, was_waiting) = {
        let mut state = state();
        if !state.begin(user_initiated) {
            return;
        }
        let was_waiting = state.stop_waiting();
        (state.pending.take(), was_waiting)
    };
    if was_waiting {
        notify_page(app);
    }

    if pending.is_none() {
        set_update_menu_text(app, "Checking for Updates…");
    }
    let handle = app.clone();
    tauri::async_runtime::spawn(async move {
        let result = match pending {
            Some(pending) => {
                prompt_off_async(&handle, move |app| {
                    if let Err(error) = decide_restart(app, pending, false, was_waiting) {
                        show_install_error(app, &error);
                    }
                })
                .await;
                Ok(true)
            }
            // The boxed error is not Send, and the prompt below is awaited.
            None => match download_update(&handle).await.map_err(|e| e.to_string()) {
                Ok(Some(pending)) => {
                    let user_visible = state().user_visible;
                    if user_visible {
                        prompt_off_async(&handle, move |app| {
                            if let Err(error) = decide_restart(app, pending, true, false) {
                                show_install_error(app, &error);
                            }
                        })
                        .await;
                    } else {
                        log::info!(
                            "Update v{} is ready; waiting for the user to restart",
                            pending.update.version
                        );
                        keep_ready(&handle, pending);
                    }
                    Ok(true)
                }
                Ok(None) => Ok(false),
                Err(error) => Err(error),
            },
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

/// Run a restart decision, which asks the backend about jobs over blocking
/// HTTP and blocks on native dialogs, off the async workers.
async fn prompt_off_async(app: &AppHandle, decide: impl FnOnce(&AppHandle) + Send + 'static) {
    let handle = app.clone();
    if let Err(error) = tauri::async_runtime::spawn_blocking(move || decide(&handle)).await {
        log::error!("Update restart prompt failed: {error}");
    }
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
            // file before the update is shown as ready.
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
        // Saved so that a ready update survives a quit or crash. A failure
        // only costs a second download.
        if let Err(error) = save_package(dir, &version, &bytes) {
            log::warn!("Could not save update v{version} for later: {error}");
        }
    }
    Ok(Some(PendingUpdate { update, bytes }))
}

/// Restart for `pending`, which the caller has taken out of the state while
/// holding the check guard.
///
/// When jobs the user started are queued or running, ask first: the default
/// waits for them to finish, Restart Now names what it would stop, and Later
/// keeps the download. A request to restart (the menu item, tray item or
/// notice) installs at once only when `restart_without_asking` holds; anything
/// else it would stop or replace (a paused job, a failed check, another Vireo
/// window on the backend) asks Restart or Later and says so. A manual check
/// that just found the update (`ask_when_idle`) always asks. Returns an
/// install failure for the caller to show; the update is then ready again.
fn decide_restart(
    app: &AppHandle,
    pending: PendingUpdate,
    ask_when_idle: bool,
    was_waiting: bool,
) -> Result<(), String> {
    let version = pending.update.version.clone();
    let readiness = fetch_readiness(app);
    if let Err(error) = &readiness {
        log::warn!("Could not check for running jobs before updating: {error}");
    }

    let shared = crate::sidecar::other_gui_clients_attached();

    let readiness = match readiness {
        Ok(readiness) if !readiness.idle => readiness,
        readiness => {
            if !ask_when_idle && restart_without_asking(&readiness, shared) {
                return install(app, pending);
            }
            set_update_menu_text(app, RESTART_TEXT);
            let restart = app
                .dialog()
                .message(ready_message(&version, readiness.as_ref().ok(), shared))
                .title("Update Ready")
                .kind(MessageDialogKind::Info)
                .buttons(MessageDialogButtons::OkCancelCustom(
                    RESTART_BUTTON.into(),
                    LATER_BUTTON.into(),
                ))
                .blocking_show();
            if restart {
                let asked = Asked::new(readiness.as_ref().ok(), shared);
                return install_confirmed(app, pending, &asked);
            }
            keep_ready(app, pending);
            return Ok(());
        }
    };

    let choice = app
        .dialog()
        .message(busy_message(&version, &readiness, was_waiting, shared))
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
            start_waiting(app, pending, &readiness);
            Ok(())
        }
        BusyChoice::Now => install_confirmed(app, pending, &Asked::new(Some(&readiness), shared)),
        BusyChoice::Later => {
            keep_ready(app, pending);
            Ok(())
        }
    }
}

/// What a Restart dialog told the user a restart would stop or replace.
#[derive(Debug, Default)]
struct Asked {
    /// Identifiers of the jobs the dialog named. `name` is a generic label
    /// (several import types all show as "Import", every pipeline as "Process
    /// run"), so identity is the backend's unique job id; a new job that
    /// happens to share a generic name is not treated as previously disclosed.
    /// An empty id (an older backend that omitted it) falls back to name so
    /// the dialog still recognizes the same job.
    jobs: Vec<JobIdentity>,
    shared: bool,
    /// Whether the dialog was based on a successful readiness response. When
    /// true, the user answered for a specific picture: a recheck that fails
    /// can no longer confirm that picture holds, so the answer must be
    /// re-asked. When false, the dialog already warned that readiness was
    /// unknown and a continuing failure adds nothing new.
    readiness_known: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct JobIdentity {
    id: String,
    name: String,
}

impl JobIdentity {
    fn of(job: &ReadinessJob) -> Self {
        Self {
            id: job.id.clone(),
            name: job.name.clone(),
        }
    }

    /// Whether two entries name the same live job. Prefer the unique id;
    /// fall back to name only when both sides lack an id (older backend).
    fn same(&self, other: &ReadinessJob) -> bool {
        if !self.id.is_empty() || !other.id.is_empty() {
            return self.id == other.id;
        }
        self.name == other.name
    }
}

impl Asked {
    fn new(readiness: Option<&Readiness>, shared: bool) -> Self {
        let (jobs, readiness_known) = match readiness {
            Some(r) => (
                r.blocking
                    .iter()
                    .chain(&r.paused)
                    .map(JobIdentity::of)
                    .collect(),
                true,
            ),
            None => (Vec::new(), false),
        };
        Self {
            jobs,
            shared,
            readiness_known,
        }
    }

    /// Whether `readiness` and `shared`, read after the answer, include
    /// something the dialog did not name: a job, or another Vireo window. A
    /// failed recheck after a dialog that named specific jobs is also a miss,
    /// since the picture the user answered for can no longer be confirmed. A
    /// failed recheck after a dialog that already warned that readiness was
    /// unknown adds nothing new, so the answer still holds.
    fn missed(&self, readiness: &Result<Readiness, String>, shared: bool) -> bool {
        if shared && !self.shared {
            return true;
        }
        match readiness {
            Ok(r) => r
                .blocking
                .iter()
                .chain(&r.paused)
                .any(|job| !self.jobs.iter().any(|known| known.same(job))),
            Err(_) => self.readiness_known,
        }
    }
}

/// Install after the user answered Restart in a dialog. A dialog stays open
/// while Vireo keeps working (fully so in browser mode), so read the jobs and
/// GUI clients again first: if something appeared that the dialog did not
/// name, ask again naming it, rather than install for a state the user never
/// saw. Each further check waits on another answer, so this ends.
fn install_confirmed(app: &AppHandle, pending: PendingUpdate, asked: &Asked) -> Result<(), String> {
    let readiness = fetch_readiness(app);
    let shared = crate::sidecar::other_gui_clients_attached();
    if asked.missed(&readiness, shared) {
        log::info!("Work changed while the restart dialog was open; asking again");
        return decide_restart(app, pending, false, false);
    }
    install(app, pending)
}

/// Whether a click on Restart to Update may restart without a dialog: the job
/// check answered, nothing the user started is running or paused, and no
/// other Vireo window runs on this backend's application files.
fn restart_without_asking(readiness: &Result<Readiness, String>, shared: bool) -> bool {
    matches!(readiness, Ok(r) if r.idle && r.paused.is_empty()) && !shared
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

/// Keep a downloaded update for this session and show it as ready.
fn keep_ready(app: &AppHandle, pending: PendingUpdate) {
    state().pending = Some(pending);
    set_update_menu_text(app, RESTART_TEXT);
    crate::tray::refresh_tray_menu(app);
    notify_page(app);
}

fn show_install_error(app: &AppHandle, error: &str) {
    app.dialog()
        .message(format!("Could not install the update:\n{error}"))
        .title("Update Error")
        .kind(MessageDialogKind::Error)
        .show(|_| {});
}

/// Restart for the ready update, from the web UI's notice, which shows an
/// install failure itself instead of a native dialog. Runs under the same
/// guard as a check, so it cannot overlap one. Asks first when the user's
/// jobs are running, like every other way to restart.
pub fn restart_to_update(app: &AppHandle) -> Result<(), String> {
    let (pending, was_waiting) = {
        let mut state = state();
        if state.running {
            return Err("Vireo is already checking for or installing an update.".into());
        }
        if state.pending.is_none() {
            return Err("No downloaded update is waiting to be installed.".into());
        }
        state.running = true;
        let was_waiting = state.stop_waiting();
        (state.pending.take().expect("checked above"), was_waiting)
    };
    if was_waiting {
        notify_page(app);
    }
    let result = decide_restart(app, pending, false, was_waiting);
    let (waiting, text) = {
        let state = state();
        (state.after_jobs, state.menu_text())
    };
    if !waiting {
        set_update_menu_text(app, text);
    }
    state().finish();
    result
}

/// Install a downloaded update and restart. On failure the update stays ready
/// so the user can try again, and the error is returned for the caller to show.
fn install(app: &AppHandle, pending: PendingUpdate) -> Result<(), String> {
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
            app.restart()
        }
        Err(error) => {
            log::error!("Update installation failed: {error}");
            keep_ready(app, pending);
            Err(error.to_string())
        }
    }
}

/// Install a downloaded update while Vireo quits, so a ready update never
/// waits past the session: the next launch runs the new version. Runs after
/// the backend has stopped, since installation replaces its files.
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
    #[serde(default)]
    id: String,
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
        // Bounds the body read too, so a stalled response fails like any
        // other check instead of holding the restart decision.
        .timeout_global(Some(READINESS_TIMEOUT))
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

/// Hand `pending` to a new waiting thread and show what it waits for.
fn start_waiting(app: &AppHandle, pending: PendingUpdate, readiness: &Readiness) {
    let generation = state().wait_for_jobs(pending, waiting_for_text(&readiness.blocking));
    set_update_menu_text(app, &waiting_menu_text(&readiness.blocking));
    crate::tray::refresh_tray_menu(app);
    notify_page(app);
    let handle = app.clone();
    std::thread::spawn(move || wait_for_jobs(&handle, generation));
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
                let text = waiting_for_text(&readiness.blocking);
                let (current, changed) = {
                    let mut state = state();
                    let current = state.is_waiter(generation);
                    let changed = current && state.waiting_for.as_deref() != Some(text.as_str());
                    if changed {
                        state.waiting_for = Some(text);
                    }
                    (current, changed)
                };
                // A request that outlived its wait must not relabel the menu.
                if current {
                    set_update_menu_text(app, &waiting_menu_text(&readiness.blocking));
                }
                if changed {
                    notify_page(app);
                }
                false
            }
            Ok(_) => true,
            Err(error) => {
                failures += 1;
                log::warn!("Update readiness check failed ({failures}): {error}");
                if failures < MAX_READINESS_FAILURES {
                    if state().is_waiter(generation) {
                        set_update_menu_text(app, "Restart to Update Now (can't check jobs)…");
                    }
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
                    if let Err(error) = install(app, *pending) {
                        show_install_error(app, &error);
                    }
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
    /// Jobs are running again; keep waiting as this generation.
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
        state.countdown = Some(Countdown {
            deadline: Instant::now() + COUNTDOWN,
            shown: false,
            response: None,
        });
        state
            .pending
            .as_ref()
            .map(|p| p.update.version.clone())
            .unwrap_or_default()
    };
    let started = Instant::now();
    let page_can_show = unattended_restart_check(app).allowed();
    if page_can_show {
        notify_page(app);
    }

    let mut last_seconds = None;
    let mut end = loop {
        if !page_can_show {
            break CountdownEnd::AskNatively;
        }
        std::thread::sleep(COUNTDOWN_TICK);
        let mut state = state();
        if !state.is_waiter(generation) {
            return CountdownOutcome::Stopped;
        }
        let Some(countdown) = state.countdown.as_ref() else {
            return CountdownOutcome::Stopped;
        };
        let now = Instant::now();
        match countdown.response {
            Some(CountdownResponse::Cancel) => {
                // Stopped under the lock that read the answer, so a newer
                // wait started after it cannot be the one cleared.
                state.stop_waiting();
                drop(state);
                publish_stopped_waiting(app);
                return CountdownOutcome::Stopped;
            }
            Some(CountdownResponse::Now) => break CountdownEnd::UserChoseNow,
            // Expiry restarts on its own only after a page showed the warning.
            None if !countdown.shown && now.duration_since(started) >= COUNTDOWN_SHOWN_TIMEOUT => {
                break CountdownEnd::AskNatively;
            }
            None if countdown.shown && now >= countdown.deadline => break CountdownEnd::Expired,
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
    // into this decision instead of opening a second prompt. Closing the
    // countdown here, in the same lock that reads its last answer, means a
    // Not Now that arrived after the loop let go still cancels, and a later
    // one is refused.
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
            notify_page(app);
            return CountdownOutcome::Busy(generation);
        }
        let response = state.countdown.as_ref().and_then(|c| c.response);
        state.stop_waiting();
        if response == Some(CountdownResponse::Cancel) {
            drop(state);
            publish_stopped_waiting(app);
            return CountdownOutcome::Stopped;
        }
        state.begin(true);
        // A Restart Now that arrived after the loop let go is the user's
        // answer, not an expiry nobody answered.
        if response == Some(CountdownResponse::Now) {
            end = CountdownEnd::UserChoseNow;
        }
        state.pending.take()
    };
    notify_page(app);
    let Some(pending) = pending else {
        state().finish();
        return CountdownOutcome::Stopped;
    };

    // Everything the last step depends on is read now, after the countdown:
    // the jobs (a follow-up such as the Process run an import queues can
    // start in the last moments), the window and the other GUI clients.
    let readiness = fetch_readiness(app);
    if let Err(error) = &readiness {
        log::warn!("Final job check before updating failed: {error}");
    }
    let check = unattended_restart_check(app);
    match final_step(&end, &readiness, &check) {
        FinalStep::Restart => CountdownOutcome::Restart(Box::new(pending)),
        FinalStep::KeepWaiting => {
            let blocking = readiness.map(|r| r.blocking).unwrap_or_default();
            let next = {
                let mut state = state();
                state.finish();
                state.wait_for_jobs(pending, waiting_for_text(&blocking))
            };
            set_update_menu_text(app, &waiting_menu_text(&blocking));
            notify_page(app);
            CountdownOutcome::Busy(next)
        }
        FinalStep::AskAboutNewJobs => {
            // The user chose Restart Now, but new work appeared: ask again,
            // naming it, rather than overriding or ignoring that choice.
            if let Err(error) = decide_restart(app, pending, false, false) {
                show_install_error(app, &error);
            }
            state().finish();
            CountdownOutcome::Stopped
        }
        FinalStep::Ask => {
            let message = match &readiness {
                Ok(readiness) => jobs_finished_message(&version, &readiness.paused, check.shared),
                Err(_) => ready_message(&version, None, check.shared),
            };
            set_update_menu_text(app, RESTART_TEXT);
            let restart = app
                .dialog()
                .message(message)
                .title("Update Ready")
                .kind(MessageDialogKind::Info)
                .buttons(MessageDialogButtons::OkCancelCustom(
                    RESTART_BUTTON.into(),
                    LATER_BUTTON.into(),
                ))
                .blocking_show();
            if restart {
                let asked = Asked::new(readiness.as_ref().ok(), check.shared);
                if let Err(error) = install_confirmed(app, pending, &asked) {
                    show_install_error(app, &error);
                }
            } else {
                keep_ready(app, pending);
            }
            state().finish();
            CountdownOutcome::Stopped
        }
    }
}

/// What happens once the countdown is over.
#[derive(Debug, PartialEq, Eq)]
enum FinalStep {
    Restart,
    /// New jobs started and nobody has answered: wait for them too.
    KeepWaiting,
    /// New jobs started after the user chose Restart Now: ask again.
    AskAboutNewJobs,
    /// Ask Restart or Later in a native dialog.
    Ask,
}

/// Decide the last step from how the countdown ended and what holds now. A
/// restart nobody answered needs all of it: a page showed the warning, the
/// job check answered with nothing running or paused, the window is still on
/// screen and no other Vireo window shares the backend. The user's own
/// Restart Now is honored unless new jobs appeared, a paused job or another
/// window would be affected that the banner did not name, or the check
/// failed and can no longer confirm the jobs finished.
fn final_step(
    end: &CountdownEnd,
    readiness: &Result<Readiness, String>,
    check: &UnattendedCheck,
) -> FinalStep {
    let busy = matches!(readiness, Ok(r) if !r.idle);
    // A paused job or another Vireo window is something a restart would stop
    // or replace that the banner never mentioned, so it always asks.
    let paused = matches!(readiness, Ok(r) if !r.paused.is_empty());
    match end {
        CountdownEnd::UserChoseNow if busy => FinalStep::AskAboutNewJobs,
        // The banner said the jobs had finished; a check that now fails can
        // no longer confirm it, so ask, as every Restart answer does.
        CountdownEnd::UserChoseNow if paused || check.shared || readiness.is_err() => {
            FinalStep::Ask
        }
        CountdownEnd::UserChoseNow => FinalStep::Restart,
        _ if busy => FinalStep::KeepWaiting,
        CountdownEnd::Expired if readiness.is_ok() && !paused && check.allowed() => {
            FinalStep::Restart
        }
        CountdownEnd::Expired | CountdownEnd::AskNatively => FinalStep::Ask,
    }
}

/// How the countdown loop ended.
enum CountdownEnd {
    /// The deadline passed with no answer.
    Expired,
    /// The user clicked Restart Now on the banner.
    UserChoseNow,
    /// No visible page could show the countdown.
    AskNatively,
}

/// What an automatic restart, one nobody answered, depends on.
struct UnattendedCheck {
    /// Another Vireo window shares this backend; installing replaces the
    /// application it runs.
    shared: bool,
    /// A page the user can see can show the countdown banner.
    page_on_screen: bool,
}

impl UnattendedCheck {
    fn allowed(&self) -> bool {
        !self.shared && self.page_on_screen
    }
}

fn unattended_restart_check(app: &AppHandle) -> UnattendedCheck {
    UnattendedCheck {
        shared: crate::sidecar::other_gui_clients_attached(),
        page_on_screen: !crate::tray::is_browser_mode(app) && main_window_on_screen(app),
    }
}

/// Show that the wait was cancelled and the download kept, after the caller
/// stopped waiting under the state lock.
fn publish_stopped_waiting(app: &AppHandle) {
    let text = state().menu_text();
    set_update_menu_text(app, text);
    crate::tray::refresh_tray_menu(app);
    notify_page(app);
}

/// Whether the main window is on screen. A window closed to the tray or
/// minimized keeps its webview running, out of sight, where a banner warns
/// nobody; some platforms report a minimized window as visible.
fn main_window_on_screen(app: &AppHandle) -> bool {
    app.get_webview_window("main").is_some_and(|window| {
        window.is_visible().unwrap_or(false) && !window.is_minimized().unwrap_or(true)
    })
}

/// Tell the page the update status changed, so it reads `update_status` again
/// now instead of at its next poll (navbar-update-notice.js listens).
fn notify_page(app: &AppHandle) {
    if crate::tray::is_browser_mode(app) {
        return;
    }
    if let Some(window) = app.get_webview_window("main") {
        let js = "window.dispatchEvent(new Event('vireo:update-status'));";
        if let Err(error) = window.eval(js) {
            log::debug!("Could not notify the page of the update status: {error}");
        }
    }
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

fn shared_note(shared: bool) -> &'static str {
    if shared {
        "\n\nAnother Vireo window shares this one's backend. Installing replaces the \
         application it is running, so quit that window before you restart."
    } else {
        ""
    }
}

fn ready_message(version: &str, readiness: Option<&Readiness>, shared: bool) -> String {
    let note = match readiness {
        Some(readiness) => paused_note(&readiness.paused),
        None => "\n\nVireo couldn't check for running jobs. A restart stops any that are running."
            .to_string(),
    };
    format!(
        "Vireo v{version} has been downloaded and is ready to install.\n\n\
         Restart Vireo to finish updating, or choose Later to keep working.{LATER_NOTE}{note}{}",
        shared_note(shared)
    )
}

fn jobs_finished_message(version: &str, paused: &[ReadinessJob], shared: bool) -> String {
    format!(
        "Your jobs have finished, and Vireo v{version} is ready to install.\n\n\
         Restart Vireo now to finish updating, or choose Later to keep working.{LATER_NOTE}{}{}",
        paused_note(paused),
        shared_note(shared)
    )
}

fn busy_message(version: &str, readiness: &Readiness, was_waiting: bool, shared: bool) -> String {
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
         Restart Now stops them.{}{}",
        describe_jobs(&readiness.blocking),
        paused_note(&readiness.paused),
        shared_note(shared),
    )
}

/// The jobs a waiting restart is held by, as the page's notice names them.
fn waiting_for_text(blocking: &[ReadinessJob]) -> String {
    match blocking {
        [] => "your jobs".to_string(),
        [job] => job.name.clone(),
        [job, rest @ ..] => format!("{} and {} more", job.name, rest.len()),
    }
}

fn waiting_menu_text(blocking: &[ReadinessJob]) -> String {
    if blocking.is_empty() {
        return WAITING_TEXT.to_string();
    }
    format!(
        "Restart to Update Now (waiting for {})…",
        waiting_for_text(blocking)
    )
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

/// A write in progress is named for its process, so two Vireo processes that
/// share the cache never write or rename each other's file.
const PARTIAL_PREFIX: &str = "partial-";
/// How old a partial write must be before pruning treats it as abandoned.
const ABANDONED_PARTIAL_AGE: Duration = Duration::from_secs(60 * 60);

fn save_package(dir: &Path, version: &str, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let path = package_path(dir, version);
    let partial = path.with_extension(format!("{PARTIAL_PREFIX}{}", std::process::id()));
    std::fs::write(&partial, bytes)?;
    std::fs::rename(&partial, &path)
}

/// Whether `path` is another process's write that may still be in progress.
fn is_recent_partial(path: &Path) -> bool {
    let is_partial = path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.starts_with(PARTIAL_PREFIX));
    is_partial
        && std::fs::metadata(path)
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|modified| modified.elapsed().ok())
            .map_or(true, |age| age < ABANDONED_PARTIAL_AGE)
}

fn load_package(dir: &Path, version: &str) -> Option<Vec<u8>> {
    std::fs::read(package_path(dir, version)).ok()
}

fn remove_package(dir: &Path, version: &str) {
    let _ = std::fs::remove_file(package_path(dir, version));
}

/// Remove every saved package except `keep`'s, and writes abandoned over an
/// hour ago. A recent write may be another Vireo process's download.
fn prune_packages(dir: &Path, keep: Option<&str>) {
    let keep = keep.map(|version| package_path(dir, version));
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if Some(&path) == keep.as_ref() || is_recent_partial(&path) {
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
            id: format!("{}-id", name.to_lowercase().replace(' ', "-")),
            name: name.into(),
            status: status.into(),
        }
    }

    fn job_with_id(id: &str, name: &str, status: &str) -> ReadinessJob {
        ReadinessJob {
            id: id.into(),
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
        let generation = state.wait_for_jobs(7, "Import".into());
        assert!(state.is_waiter(generation));
        assert!(!state.begin(false));
        assert!(state.is_waiter(generation));
        assert_eq!(state.menu_text(), WAITING_TEXT);
        assert_eq!(state.waiting_for.as_deref(), Some("Import"));
    }

    #[test]
    fn menu_request_takes_the_decision_from_the_waiter() {
        let mut state = UpdateState::new();
        let generation = state.wait_for_jobs(7, "Import".into());
        assert!(state.begin(true));
        assert!(state.stop_waiting());
        assert!(!state.is_waiter(generation));
        assert_eq!(state.waiting_for, None);
        assert_eq!(state.pending, Some(7));
        assert_eq!(state.menu_text(), RESTART_TEXT);

        // Choosing to wait again starts a new generation; the old thread
        // still sees it is no longer the waiter.
        let pending = state.pending.take().unwrap();
        let next = state.wait_for_jobs(pending, "Import".into());
        assert_ne!(next, generation);
        assert!(state.is_waiter(next));
        assert!(!state.is_waiter(generation));
    }

    #[test]
    fn stop_waiting_clears_the_countdown() {
        let mut state = UpdateState::new();
        state.wait_for_jobs(1, "Import".into());
        state.countdown = Some(Countdown {
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
    fn waiting_text_names_the_job() {
        assert_eq!(
            waiting_menu_text(&[job("Import", "running")]),
            "Restart to Update Now (waiting for Import)…"
        );
        assert_eq!(
            waiting_menu_text(&[job("Import", "running"), job("Process run", "queued")]),
            "Restart to Update Now (waiting for Import and 1 more)…"
        );
        assert_eq!(waiting_for_text(&[]), "your jobs");
    }

    #[test]
    fn busy_message_names_jobs_and_paused_ones() {
        let readiness = Readiness {
            idle: false,
            blocking: vec![job("Import", "running"), job("Process run", "queued")],
            paused: vec![job("Move folder", "paused")],
        };
        let message = busy_message("1.2.3", &readiness, false, false);
        assert!(message.contains("Import, Process run (waiting to start)"));
        assert!(message.contains("Paused jobs stop when Vireo restarts: Move folder."));
        assert!(busy_message("1.2.3", &readiness, true, false).starts_with("Vireo is waiting"));
    }

    #[test]
    fn ready_message_says_when_jobs_could_not_be_checked() {
        assert!(ready_message("1.2.3", None, false).contains("couldn't check for running jobs"));
        let idle = Readiness {
            idle: true,
            blocking: vec![],
            paused: vec![],
        };
        assert!(!ready_message("1.2.3", Some(&idle), false).contains("couldn't check"));
    }

    #[test]
    fn unattended_restart_needs_an_unshared_backend_and_a_page_on_screen() {
        let check = |shared, page_on_screen| {
            UnattendedCheck {
                shared,
                page_on_screen,
            }
            .allowed()
        };
        assert!(check(false, true));
        assert!(!check(true, true));
        assert!(!check(false, false));
    }

    #[test]
    fn a_click_restarts_without_asking_only_when_nothing_would_be_lost() {
        let readiness = |idle: bool, paused: Vec<ReadinessJob>| -> Result<Readiness, String> {
            Ok(Readiness {
                idle,
                blocking: vec![],
                paused,
            })
        };
        assert!(restart_without_asking(&readiness(true, vec![]), false));
        // A paused job would stop, another window would lose its files, and a
        // failed check says nothing about running jobs.
        assert!(!restart_without_asking(
            &readiness(true, vec![job("Process run", "paused")]),
            false
        ));
        assert!(!restart_without_asking(&readiness(true, vec![]), true));
        assert!(!restart_without_asking(&Err("timed out".into()), false));
        assert!(!restart_without_asking(&readiness(false, vec![]), false));
        assert!(ready_message("1.2.3", None, true).contains("Another Vireo window"));
    }

    #[test]
    fn final_step_restarts_unattended_only_when_everything_holds() {
        let idle = || {
            Ok(Readiness {
                idle: true,
                blocking: vec![],
                paused: vec![],
            })
        };
        let busy = || {
            Ok(Readiness {
                idle: false,
                blocking: vec![job("Process run", "queued")],
                paused: vec![],
            })
        };
        let failed = || Err::<Readiness, String>("timed out".into());
        let ok = UnattendedCheck {
            shared: false,
            page_on_screen: true,
        };
        let hidden = UnattendedCheck {
            shared: false,
            page_on_screen: false,
        };
        let shared = UnattendedCheck {
            shared: true,
            page_on_screen: true,
        };

        assert_eq!(
            final_step(&CountdownEnd::Expired, &idle(), &ok),
            FinalStep::Restart
        );
        assert_eq!(
            final_step(&CountdownEnd::Expired, &busy(), &ok),
            FinalStep::KeepWaiting
        );
        assert_eq!(
            final_step(&CountdownEnd::Expired, &failed(), &ok),
            FinalStep::Ask
        );
        assert_eq!(
            final_step(&CountdownEnd::Expired, &idle(), &hidden),
            FinalStep::Ask
        );
        assert_eq!(
            final_step(&CountdownEnd::Expired, &idle(), &shared),
            FinalStep::Ask
        );
        assert_eq!(
            final_step(&CountdownEnd::AskNatively, &idle(), &ok),
            FinalStep::Ask
        );
        assert_eq!(
            final_step(&CountdownEnd::AskNatively, &busy(), &ok),
            FinalStep::KeepWaiting
        );
        // A paused job stops on restart, so an unanswered expiry asks.
        let paused = || {
            Ok(Readiness {
                idle: true,
                blocking: vec![],
                paused: vec![job("Process run", "paused")],
            })
        };
        assert_eq!(
            final_step(&CountdownEnd::Expired, &paused(), &ok),
            FinalStep::Ask
        );
        // Restart Now asks again for what the banner never named.
        assert_eq!(
            final_step(&CountdownEnd::UserChoseNow, &paused(), &ok),
            FinalStep::Ask
        );
        assert_eq!(
            final_step(&CountdownEnd::UserChoseNow, &idle(), &shared),
            FinalStep::Ask
        );
        // The user's Restart Now stands, unless new work appeared.
        assert_eq!(
            final_step(&CountdownEnd::UserChoseNow, &failed(), &hidden),
            FinalStep::Ask
        );
        assert_eq!(
            final_step(&CountdownEnd::UserChoseNow, &idle(), &hidden),
            FinalStep::Restart
        );
        assert_eq!(
            final_step(&CountdownEnd::UserChoseNow, &busy(), &ok),
            FinalStep::AskAboutNewJobs
        );
    }

    #[test]
    fn a_confirmed_restart_asks_again_only_for_what_the_dialog_did_not_name() {
        let readiness = |blocking: Vec<ReadinessJob>, paused: Vec<ReadinessJob>| {
            Ok::<Readiness, String>(Readiness {
                idle: blocking.is_empty(),
                blocking,
                paused,
            })
        };
        let asked = Asked::new(
            Some(&Readiness {
                idle: false,
                blocking: vec![job("Import", "running")],
                paused: vec![job("Move folder", "paused")],
            }),
            false,
        );
        // Nothing new, or less than before: install.
        assert!(!asked.missed(&readiness(vec![job("Import", "running")], vec![]), false));
        assert!(!asked.missed(
            &readiness(vec![], vec![job("Move folder", "paused")]),
            false
        ));
        // A job or another window the dialog never named, or a check that
        // can no longer confirm what it named: ask again.
        assert!(asked.missed(&Err("timed out".into()), false));
        assert!(asked.missed(
            &readiness(vec![job("Process run", "queued")], vec![]),
            false
        ));
        assert!(asked.missed(&readiness(vec![], vec![]), true));
        assert!(!Asked::new(None, true).missed(&readiness(vec![], vec![]), true));
    }

    #[test]
    fn jobs_finished_message_warns_about_a_shared_backend() {
        assert!(jobs_finished_message("1.2.3", &[], true).contains("Another Vireo window"));
        assert!(!jobs_finished_message("1.2.3", &[], false).contains("Another Vireo window"));
        assert!(
            jobs_finished_message("1.2.3", &[job("Process run", "paused")], false)
                .contains("Paused jobs stop when Vireo restarts: Process run.")
        );
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
    fn status_serializes_for_the_page() {
        let status = UpdateStatus {
            version: "1.2.3".into(),
            waiting_for: Some("Import".into()),
            restart_in: None,
        };
        assert_eq!(
            serde_json::to_value(&status).unwrap(),
            serde_json::json!({"version": "1.2.3", "waiting_for": "Import", "restart_in": null})
        );
    }

    #[test]
    fn saved_package_round_trips_and_old_versions_are_pruned() {
        let dir = std::env::temp_dir().join(format!("vireo-update-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        save_package(&dir, "1.0.0", b"old").unwrap();
        save_package(&dir, "1.1.0", b"new").unwrap();
        // Another process's download in progress survives pruning.
        std::fs::write(dir.join("vireo-1.2.0.partial-999"), b"writing").unwrap();

        prune_packages(&dir, Some("1.1.0"));
        assert_eq!(
            load_package(&dir, "1.1.0").as_deref(),
            Some(b"new".as_slice())
        );
        assert!(load_package(&dir, "1.0.0").is_none());
        assert!(dir.join("vireo-1.2.0.partial-999").exists());
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 2);

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

    #[test]
    fn asked_matches_jobs_by_id_not_generic_name() {
        let readiness = Readiness {
            idle: false,
            blocking: vec![job_with_id("import-1", "Import", "running")],
            paused: vec![],
        };
        let asked = Asked::new(Some(&readiness), false);
        // A second Import with a different id started while the dialog was
        // open; the generic name matches but the identity does not.
        let after = Ok(Readiness {
            idle: false,
            blocking: vec![
                job_with_id("import-1", "Import", "running"),
                job_with_id("import-2", "Import", "queued"),
            ],
            paused: vec![],
        });
        assert!(asked.missed(&after, false));

        // The same id with the same state is not missed.
        let unchanged = Ok(Readiness {
            idle: false,
            blocking: vec![job_with_id("import-1", "Import", "running")],
            paused: vec![],
        });
        assert!(!asked.missed(&unchanged, false));
    }

    #[test]
    fn asked_falls_back_to_name_when_both_sides_lack_an_id() {
        let readiness = Readiness {
            idle: false,
            blocking: vec![job_with_id("", "Import", "running")],
            paused: vec![],
        };
        let asked = Asked::new(Some(&readiness), false);
        let unchanged = Ok(Readiness {
            idle: false,
            blocking: vec![job_with_id("", "Import", "running")],
            paused: vec![],
        });
        assert!(!asked.missed(&unchanged, false));
    }

    #[test]
    fn asked_re_prompts_when_a_confirmed_recheck_now_fails() {
        // Dialog named a specific job; a later recheck times out. The user
        // answered for a confirmed picture, so re-ask rather than install.
        let readiness = Readiness {
            idle: false,
            blocking: vec![job("Import", "running")],
            paused: vec![],
        };
        let asked = Asked::new(Some(&readiness), false);
        assert!(asked.missed(&Err::<Readiness, String>("timed out".into()), false));

        // A dialog that already warned readiness was unknown treats a
        // continuing failure as the same unknown state the user accepted.
        let asked_unknown = Asked::new(None, false);
        assert!(!asked_unknown.missed(&Err::<Readiness, String>("timed out".into()), false));
    }

    #[test]
    fn asked_notices_a_new_shared_backend_window() {
        let asked = Asked::new(None, false);
        let readiness: Result<Readiness, String> = Err("timed out".into());
        assert!(asked.missed(&readiness, true));
    }
}
