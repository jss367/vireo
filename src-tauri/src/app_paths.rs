//! Home-relative defaults shared with the Python backend. No filesystem writes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const LAYOUT: &str = include_str!("../../vireo/data/app_paths.json");

pub fn path_in(home: &Path, name: &str) -> PathBuf {
    static PATHS: OnceLock<HashMap<String, String>> = OnceLock::new();
    let paths = PATHS.get_or_init(|| serde_json::from_str(LAYOUT).expect("valid storage layout"));
    home.join(paths.get(name).expect("known storage path"))
}

pub fn default_path(name: &str) -> Option<PathBuf> {
    dirs::home_dir().map(|home| path_in(&home, name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launcher_keeps_existing_backend_locations() {
        let home = Path::new("test home");
        for (name, relative) in [
            ("config", ".vireo/config.json"),
            ("database", ".vireo/vireo.db"),
            ("runtime", ".vireo/runtime.json"),
            ("runtime_lock", ".vireo/runtime.lock"),
            ("gui_clients", ".vireo/gui-clients"),
            ("gui_clients_lock", ".vireo/gui-clients.lock"),
        ] {
            assert_eq!(path_in(home, name), home.join(relative));
        }
    }
}
