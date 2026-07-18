use tauri::{Manager, Runtime, Url};
use tauri_plugin_opener::OpenerExt;

use crate::sidecar::SidecarState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NavigationAction {
    AllowInternal,
    OpenExternal,
    Block,
}

fn is_loopback_backend(url: &Url, backend_port: Option<u16>) -> bool {
    if url.scheme() != "http" || !url.username().is_empty() || url.password().is_some() {
        return false;
    }

    let host_is_vireo = matches!(url.host_str(), Some("localhost") | Some("127.0.0.1"));
    if !host_is_vireo {
        return false;
    }

    let expected_port = backend_port.or_else(|| cfg!(debug_assertions).then_some(8080));
    url.port_or_known_default() == expected_port
}

fn classify(url: &Url, backend_port: Option<u16>) -> NavigationAction {
    match url.scheme() {
        // Tauri's packaged frontend is served from this private scheme. The
        // production window uses it briefly before navigating to the sidecar.
        "tauri"
            if url.host_str() == Some("localhost")
                && url.username().is_empty()
                && url.password().is_none() =>
        {
            NavigationAction::AllowInternal
        }
        "http" if is_loopback_backend(url, backend_port) => NavigationAction::AllowInternal,
        "http" | "https" => NavigationAction::OpenExternal,
        _ => NavigationAction::Block,
    }
}

fn backend_port<R: Runtime>(app: &tauri::AppHandle<R>) -> Option<u16> {
    app.try_state::<SidecarState>().map(|state| state.port)
}

fn open_external<R: Runtime>(
    app: &tauri::AppHandle<R>,
    url: &Url,
    source: &str,
) -> Result<(), String> {
    app.opener()
        .open_url(url.as_str(), None::<&str>)
        .map_err(|error| {
            let message = error.to_string();
            log::error!(
                "External navigation from {} failed for {}: {}",
                source,
                url,
                message
            );
            message
        })?;
    log::info!(
        "External navigation from {} opened in browser: {}",
        source,
        url
    );
    Ok(())
}

fn surface_open_failure<R: Runtime>(app: &tauri::AppHandle<R>, url: &Url, error: &str) {
    let Some(window) = app.get_webview_window("main") else {
        return;
    };
    let url_json = serde_json::to_string(url.as_str()).unwrap_or_else(|_| "\"\"".to_string());
    let message = format!("Vireo could not open the external URL: {error}");
    let message_json = serde_json::to_string(&message)
        .unwrap_or_else(|_| "\"Vireo could not open the external URL.\"".to_string());
    let script = format!(
        "if (window.showExternalOpenFailure) {{ window.showExternalOpenFailure({url_json}, {message_json}); }}"
    );
    if let Err(eval_error) = window.eval(&script) {
        log::error!("Could not show external-open recovery UI: {}", eval_error);
    }
}

/// Apply the native-shell navigation invariant to a main-frame navigation.
/// Returning false prevents WKWebView/WebView2 from loading the URL.
pub fn handle_navigation<R: Runtime>(app: &tauri::AppHandle<R>, url: &Url) -> bool {
    match classify(url, backend_port(app)) {
        NavigationAction::AllowInternal => {
            log::info!("Allowed internal Vireo navigation: {}", url);
            true
        }
        NavigationAction::OpenExternal => {
            if let Err(error) = open_external(app, url, "main webview") {
                surface_open_failure(app, url, &error);
            }
            false
        }
        NavigationAction::Block => {
            log::warn!("Blocked unsupported webview navigation: {}", url);
            false
        }
    }
}

/// Allow downloads only from Vireo's own packaged frontend or loopback
/// backend. Download navigations can bypass the ordinary navigation callback
/// on some webview engines, so they need the same origin check here.
///
/// Same-origin `blob:` URLs are also allowed so the frontend's internal
/// fallbacks (e.g. the issue-report modal saves diagnostics via
/// `URL.createObjectURL` when the API can't email them) still work in the
/// desktop shell. Blob URLs can only be created by a same-origin page, so
/// gating on the inner origin keeps the trust boundary intact.
pub fn allow_download<R: Runtime>(app: &tauri::AppHandle<R>, url: &Url) -> bool {
    let port = backend_port(app);
    if matches!(classify(url, port), NavigationAction::AllowInternal)
        || is_same_origin_blob(url, port)
    {
        true
    } else {
        log::warn!("Blocked download from non-Vireo URL: {}", url);
        false
    }
}

fn is_same_origin_blob(url: &Url, backend_port: Option<u16>) -> bool {
    if url.scheme() != "blob" {
        return false;
    }
    let Ok(inner) = Url::parse(url.path()) else {
        return false;
    };
    matches!(
        classify(&inner, backend_port),
        NavigationAction::AllowInternal
    )
}

/// Handle a `window.open` request. Vireo is intentionally single-webview:
/// external pages go to the OS browser and no child webview is ever created.
pub fn handle_new_window<R: Runtime>(app: &tauri::AppHandle<R>, url: &Url) {
    match classify(url, backend_port(app)) {
        NavigationAction::OpenExternal => {
            if let Err(error) = open_external(app, url, "window.open") {
                surface_open_failure(app, url, &error);
            }
        }
        NavigationAction::AllowInternal => {
            log::warn!("Blocked child Vireo webview request: {}", url);
        }
        NavigationAction::Block => {
            log::warn!("Blocked unsupported child webview request: {}", url);
        }
    }
}

/// Canonical command implementation used by frontend-initiated external opens.
pub fn open_external_url<R: Runtime>(
    app: &tauri::AppHandle<R>,
    raw_url: &str,
) -> Result<(), String> {
    let url = Url::parse(raw_url.trim()).map_err(|error| format!("Invalid URL: {error}"))?;
    if !matches!(
        classify(&url, backend_port(app)),
        NavigationAction::OpenExternal
    ) {
        return Err("Only external http and https URLs can be opened".to_string());
    }
    open_external(app, &url, "frontend command")
}

#[cfg(test)]
mod tests {
    use super::{classify, is_same_origin_blob, NavigationAction};
    use tauri::Url;

    fn action(raw: &str, port: Option<u16>) -> NavigationAction {
        classify(&Url::parse(raw).unwrap(), port)
    }

    fn blob_allowed(raw: &str, port: Option<u16>) -> bool {
        is_same_origin_blob(&Url::parse(raw).unwrap(), port)
    }

    #[test]
    fn allows_packaged_assets_and_exact_backend_origin() {
        assert_eq!(
            action("tauri://localhost/index.html", None),
            NavigationAction::AllowInternal
        );
        assert_eq!(
            action("http://127.0.0.1:43127/browse?photo_id=1", Some(43127)),
            NavigationAction::AllowInternal
        );
        assert_eq!(
            action("http://localhost:43127/settings", Some(43127)),
            NavigationAction::AllowInternal
        );
    }

    #[test]
    fn blocks_packaged_scheme_lookalikes() {
        assert_eq!(
            action("tauri://elsewhere/index.html", None),
            NavigationAction::Block
        );
        assert_eq!(
            action("tauri://someone@localhost/index.html", None),
            NavigationAction::Block
        );
    }

    #[test]
    fn externalizes_http_and_https_websites() {
        assert_eq!(
            action(
                "https://www.inaturalist.org/observations/upload",
                Some(43127)
            ),
            NavigationAction::OpenExternal
        );
        assert_eq!(
            action("http://example.com/help", Some(43127)),
            NavigationAction::OpenExternal
        );
    }

    #[test]
    fn externalizes_wrong_ports_and_loopback_lookalikes() {
        assert_eq!(
            action("http://127.0.0.1:43128/browse", Some(43127)),
            NavigationAction::OpenExternal
        );
        assert_eq!(
            action("http://localhost.example.com:43127/browse", Some(43127)),
            NavigationAction::OpenExternal
        );
        assert_eq!(
            action("http://127.0.0.1.example.com:43127/browse", Some(43127)),
            NavigationAction::OpenExternal
        );
    }

    #[test]
    fn rejects_user_info_on_otherwise_internal_urls() {
        assert_eq!(
            action("http://someone@localhost:43127/browse", Some(43127)),
            NavigationAction::OpenExternal
        );
        assert_eq!(
            action("http://someone:secret@127.0.0.1:43127/browse", Some(43127)),
            NavigationAction::OpenExternal
        );
    }

    #[test]
    fn allows_same_origin_blob_downloads() {
        assert!(blob_allowed(
            "blob:http://localhost:43127/1e2b-c",
            Some(43127)
        ));
        assert!(blob_allowed(
            "blob:http://127.0.0.1:43127/9f-08",
            Some(43127)
        ));
        assert!(blob_allowed("blob:tauri://localhost/abc-1", None));
    }

    #[test]
    fn blocks_cross_origin_and_malformed_blob_downloads() {
        assert!(!blob_allowed(
            "blob:https://evil.example.com/1e2b-c",
            Some(43127)
        ));
        assert!(!blob_allowed(
            "blob:http://localhost:43128/wrong-port",
            Some(43127)
        ));
        assert!(!blob_allowed(
            "blob:http://someone@localhost:43127/user-info",
            Some(43127)
        ));
        assert!(!blob_allowed("blob:null/opaque", Some(43127)));
        assert!(!blob_allowed("http://localhost:43127/not-a-blob", Some(43127)));
    }

    #[test]
    fn blocks_unsupported_schemes() {
        assert_eq!(
            action("mailto:help@example.com", Some(43127)),
            NavigationAction::Block
        );
        assert_eq!(
            action("file:///tmp/index.html", Some(43127)),
            NavigationAction::Block
        );
        assert_eq!(
            action("javascript:alert(1)", Some(43127)),
            NavigationAction::Block
        );
    }
}
