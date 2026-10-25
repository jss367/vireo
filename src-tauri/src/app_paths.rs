//! Defaults shared with Python. Legacy installations move only via offline opt-in migration.

use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

#[derive(Deserialize)]
struct Root {
    env: Option<String>,
    fallback: String,
    suffix: String,
}

#[derive(Deserialize)]
struct Location {
    root: String,
    path: String,
}

#[derive(Deserialize)]
struct Layout {
    legacy: HashMap<String, String>,
    roots: HashMap<String, HashMap<String, Root>>,
    native: HashMap<String, Location>,
    native_marker: String,
    migration_journal: String,
}

fn layout() -> &'static Layout {
    static LAYOUT: OnceLock<Layout> = OnceLock::new();
    LAYOUT.get_or_init(|| {
        serde_json::from_str(include_str!("../../vireo/data/app_paths.json"))
            .expect("valid storage layout")
    })
}

fn entry_exists(path: &Path) -> bool {
    match std::fs::symlink_metadata(path) {
        Ok(_) => true,
        Err(error) => error.kind() != std::io::ErrorKind::NotFound,
    }
}

fn resolve(
    home: &Path,
    platform: &str,
    name: &str,
    env: impl Fn(&str) -> Option<PathBuf>,
) -> PathBuf {
    let layout = layout();
    let legacy = layout.legacy.get(name).expect("known storage path");
    let data = native_root(home, platform, "data", &env);
    if name == "desktop_logs_macos"
        || (!entry_exists(&data.join(&layout.native_marker)) && entry_exists(&home.join(".vireo")))
    {
        return home.join(legacy);
    }
    let location = &layout.native[name];
    native_root(home, platform, &location.root, &env).join(&location.path)
}

fn native_root(
    home: &Path,
    platform: &str,
    kind: &str,
    env: &impl Fn(&str) -> Option<PathBuf>,
) -> PathBuf {
    let root = &layout().roots[platform][kind];
    let base = root
        .env
        .as_deref()
        .and_then(env)
        .filter(|path| path.is_absolute())
        .unwrap_or_else(|| home.join(&root.fallback));
    base.join(&root.suffix)
}

fn ensure_no_pending_migration(home: &Path) -> std::io::Result<()> {
    if entry_exists(&home.join(&layout().migration_journal)) {
        return Err(std::io::Error::other(
            "Storage migration is unfinished. Quit Vireo and run the sidecar with \
             --migrate-storage apply to resume or --migrate-storage rollback to restore.",
        ));
    }
    Ok(())
}

pub fn initialize_layout() -> std::io::Result<()> {
    let Some(home) = dirs::home_dir() else {
        return Ok(());
    };
    ensure_no_pending_migration(&home)?;
    let data = path_in(&home, "data");
    if data == home.join(".vireo") {
        return Ok(());
    }
    std::fs::create_dir_all(&data)?;
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(data.join(&layout().native_marker))
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

pub fn path_in(home: &Path, name: &str) -> PathBuf {
    let platform = if cfg!(target_os = "macos") {
        "darwin"
    } else if cfg!(target_os = "windows") {
        "win32"
    } else {
        "linux"
    };
    resolve(home, platform, name, |key| {
        std::env::var_os(key).map(PathBuf::from)
    })
}

pub fn default_path(name: &str) -> Option<PathBuf> {
    dirs::home_dir().map(|home| path_in(&home, name))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_home() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "vireo-paths-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn unfinished_storage_migration_blocks_launcher_startup() {
        let home = test_home();
        assert!(ensure_no_pending_migration(&home).is_ok());
        std::fs::write(home.join(&layout().migration_journal), "{}").unwrap();
        let error = ensure_no_pending_migration(&home).unwrap_err();
        assert!(error.to_string().contains("--migrate-storage rollback"));
        assert!(!home.join("Library/Application Support/Vireo").exists());
        std::fs::remove_dir_all(home).unwrap();
    }

    #[test]
    fn fresh_install_roots_on_every_platform() {
        let home = test_home();
        for (platform, data, cache) in [
            (
                "darwin",
                "Library/Application Support/Vireo",
                "Library/Caches/Vireo",
            ),
            ("win32", "AppData/Local/Vireo", "AppData/Local/Vireo/Cache"),
            ("linux", ".local/share/Vireo", ".cache/Vireo"),
        ] {
            for (name, suffix) in [
                ("database", "vireo.db"),
                ("config", "config.json"),
                ("runtime", "runtime.json"),
                ("runtime_lock", "runtime.lock"),
                ("gui_clients", "gui-clients"),
                ("gui_clients_lock", "gui-clients.lock"),
            ] {
                assert_eq!(
                    resolve(&home, platform, name, |_| None),
                    home.join(data).join(suffix)
                );
            }
            assert_eq!(
                resolve(&home, platform, "models", |_| None),
                home.join(cache).join("models")
            );
        }
        assert_eq!(std::fs::read_dir(&home).unwrap().count(), 0);
        std::fs::remove_dir(&home).unwrap();
    }

    #[test]
    fn legacy_wins_even_when_native_catalog_exists() {
        let home = test_home();
        std::fs::create_dir(home.join(".vireo")).unwrap();
        let native = home.join("Library/Application Support/Vireo");
        std::fs::create_dir_all(&native).unwrap();
        std::fs::write(native.join("vireo.db"), "native").unwrap();
        for platform in ["darwin", "win32", "linux"] {
            for (name, relative) in &layout().legacy {
                assert_eq!(
                    resolve(&home, platform, name, |_| Some(home.join("override"))),
                    home.join(relative)
                );
            }
        }
        std::fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn native_marker_keeps_selection_after_older_app_creates_legacy_root() {
        let home = test_home();
        let data = home.join("Library/Application Support/Vireo");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join(&layout().native_marker), "").unwrap();
        std::fs::create_dir(home.join(".vireo")).unwrap();
        assert_eq!(
            resolve(&home, "darwin", "database", |_| None),
            data.join("vireo.db")
        );
        std::fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn absolute_environment_overrides_only() {
        let home = test_home();
        for platform in ["linux", "win32"] {
            let root = home.join("redirected");
            assert_eq!(
                resolve(&home, platform, "database", |_| Some(root.clone())),
                root.join("Vireo/vireo.db")
            );
            assert_eq!(
                resolve(&home, platform, "database", |_| Some("relative".into())),
                resolve(&home, platform, "database", |_| None)
            );
        }
        std::fs::remove_dir(&home).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn dangling_legacy_symlink_is_not_a_fresh_install() {
        let home = test_home();
        std::os::unix::fs::symlink(home.join("missing-volume"), home.join(".vireo")).unwrap();
        assert_eq!(
            resolve(&home, "darwin", "database", |_| None),
            home.join(".vireo/vireo.db")
        );
        std::fs::remove_dir_all(&home).unwrap();
    }
}
