//! Read a small subset of Vireo's user config (`~/.vireo/config.json`).
//!
//! The Python side owns the full schema and writes the file. Rust only needs
//! to read a couple of launch-time booleans, so we deserialize defensively
//! into a struct with all-optional fields and fall back to defaults if the
//! file is missing, unreadable, or malformed.

use serde::Deserialize;
use std::path::PathBuf;

/// Subset of `~/.vireo/config.json` the Tauri wrapper cares about.
///
/// Everything is `Option<T>` so a missing key, an extra key, or a totally
/// empty file all parse cleanly and let us apply our own defaults.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct LaunchConfig {
    /// If true, the desktop wrapper opens the UI in the user's default
    /// browser on launch instead of creating a WKWebView window.
    pub open_in_browser: Option<bool>,
}

impl LaunchConfig {
    /// True if browser-launch mode is enabled (defaults to false).
    pub fn open_in_browser(&self) -> bool {
        self.open_in_browser.unwrap_or(false)
    }
}

/// Resolve the path to `~/.vireo/config.json`.
pub fn config_path() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".vireo").join("config.json"))
}

/// Read and parse the config file, returning defaults on any failure.
///
/// Failure modes (file missing, unreadable, invalid JSON, unexpected shape)
/// all silently fall back to defaults so a corrupt file never blocks app
/// launch — the worst it does is skip the user's preference for one run.
pub fn load_launch_config() -> LaunchConfig {
    let Some(path) = config_path() else {
        return LaunchConfig::default();
    };
    let Ok(bytes) = std::fs::read(&path) else {
        return LaunchConfig::default();
    };
    match serde_json::from_slice::<LaunchConfig>(&bytes) {
        Ok(cfg) => cfg,
        Err(e) => {
            log::warn!(
                "Failed to parse {} for launch config ({e}); using defaults",
                path.display()
            );
            LaunchConfig::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_yields_defaults() {
        // We can't easily redirect the home dir in unit tests, so just
        // verify the parser is defensive on empty / missing input.
        let cfg: LaunchConfig = serde_json::from_str("{}").unwrap();
        assert!(!cfg.open_in_browser());
    }

    #[test]
    fn explicit_true_round_trips() {
        let cfg: LaunchConfig =
            serde_json::from_str(r#"{"open_in_browser": true}"#).unwrap();
        assert!(cfg.open_in_browser());
    }

    #[test]
    fn explicit_false_round_trips() {
        let cfg: LaunchConfig =
            serde_json::from_str(r#"{"open_in_browser": false}"#).unwrap();
        assert!(!cfg.open_in_browser());
    }

    #[test]
    fn unknown_keys_are_tolerated() {
        // The Python side has dozens of keys we don't care about — they
        // must not break parsing.
        let json = r#"{
            "open_in_browser": true,
            "classification_threshold": 0.4,
            "pipeline": {"w_focus": 0.45},
            "scan_roots": ["/tmp/photos"]
        }"#;
        let cfg: LaunchConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.open_in_browser());
    }

    #[test]
    fn malformed_json_is_handled_by_caller() {
        // load_launch_config swallows parse errors; here we just confirm
        // that serde itself reports the error so the caller's match arm
        // is exercised in real use.
        let result = serde_json::from_str::<LaunchConfig>("not json {{");
        assert!(result.is_err());
    }
}
