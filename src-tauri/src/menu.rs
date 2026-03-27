use tauri::{
    menu::{AboutMetadataBuilder, MenuBuilder, MenuItemBuilder, PredefinedMenuItem, SubmenuBuilder},
    AppHandle,
};

/// Menu item IDs used to match events in `on_menu_event`.
pub mod ids {
    // View menu — navigation
    pub const NAV_BROWSE: &str = "nav_browse";
    pub const NAV_IMPORT: &str = "nav_import";
    pub const NAV_PIPELINE: &str = "nav_pipeline";
    pub const NAV_PIPELINE_REVIEW: &str = "nav_pipeline_review";
    pub const NAV_REVIEW: &str = "nav_review";
    pub const NAV_CULL: &str = "nav_cull";
    pub const NAV_MAP: &str = "nav_map";
    pub const NAV_VARIANTS: &str = "nav_variants";
    pub const NAV_AUDIT: &str = "nav_audit";
    pub const NAV_COMPARE: &str = "nav_compare";
    pub const NAV_DASHBOARD: &str = "nav_dashboard";
    pub const NAV_WORKSPACE: &str = "nav_workspace";
    pub const NAV_SETTINGS: &str = "nav_settings";
    pub const NAV_LOGS: &str = "nav_logs";
}

/// Map a menu item ID to its Flask route path.
pub fn route_for_id(id: &str) -> Option<&'static str> {
    match id {
        ids::NAV_BROWSE => Some("/browse"),
        ids::NAV_IMPORT => Some("/import"),
        ids::NAV_PIPELINE => Some("/pipeline"),
        ids::NAV_PIPELINE_REVIEW => Some("/pipeline/review"),
        ids::NAV_REVIEW => Some("/review"),
        ids::NAV_CULL => Some("/cull"),
        ids::NAV_MAP => Some("/map"),
        ids::NAV_VARIANTS => Some("/variants"),
        ids::NAV_AUDIT => Some("/audit"),
        ids::NAV_COMPARE => Some("/compare"),
        ids::NAV_DASHBOARD => Some("/dashboard"),
        ids::NAV_WORKSPACE => Some("/workspace"),
        ids::NAV_SETTINGS => Some("/settings"),
        ids::NAV_LOGS => Some("/logs"),
        _ => None,
    }
}

/// Build the application menu bar.
///
/// macOS: appears in the system menu bar (app name menu is automatic).
/// Windows/Linux: attached to the window chrome.
pub fn build_menu(app: &AppHandle) -> tauri::Result<tauri::menu::Menu<tauri::Wry>> {
    // -- File menu --
    let file_menu = SubmenuBuilder::new(app, "File")
        .close_window()
        .separator()
        .quit()
        .build()?;

    // -- View menu (page navigation) --
    let view_menu = SubmenuBuilder::new(app, "View")
        .item(
            &MenuItemBuilder::with_id(ids::NAV_BROWSE, "Browse")
                .accelerator("CmdOrCtrl+1")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_IMPORT, "Import")
                .accelerator("CmdOrCtrl+2")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_PIPELINE, "Pipeline")
                .accelerator("CmdOrCtrl+3")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_PIPELINE_REVIEW, "Pipeline Review")
                .accelerator("CmdOrCtrl+4")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_REVIEW, "Review")
                .accelerator("CmdOrCtrl+5")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_CULL, "Cull")
                .accelerator("CmdOrCtrl+6")
                .build(app)?,
        )
        .separator()
        .item(
            &MenuItemBuilder::with_id(ids::NAV_MAP, "Map")
                .accelerator("CmdOrCtrl+7")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_VARIANTS, "Variants")
                .accelerator("CmdOrCtrl+8")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_AUDIT, "Audit")
                .accelerator("CmdOrCtrl+9")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_COMPARE, "Compare")
                .accelerator("CmdOrCtrl+0")
                .build(app)?,
        )
        .separator()
        .item(
            &MenuItemBuilder::with_id(ids::NAV_DASHBOARD, "Dashboard")
                .accelerator("CmdOrCtrl+Shift+D")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_WORKSPACE, "Workspace")
                .accelerator("CmdOrCtrl+Shift+W")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_SETTINGS, "Settings")
                .accelerator("CmdOrCtrl+,")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_LOGS, "Logs")
                .accelerator("CmdOrCtrl+Shift+L")
                .build(app)?,
        )
        .build()?;

    // -- Window menu --
    let window_menu = SubmenuBuilder::new(app, "Window")
        .minimize()
        .maximize()
        .separator()
        .close_window()
        .build()?;

    // -- Help menu --
    let about = PredefinedMenuItem::about(
        app,
        Some("About Vireo"),
        Some(
            AboutMetadataBuilder::new()
                .name(Some("Vireo"))
                .version(Some(env!("CARGO_PKG_VERSION")))
                .comments(Some("AI-powered wildlife photo organizer"))
                .license(Some("MIT"))
                .build(),
        ),
    )?;

    let help_menu = SubmenuBuilder::new(app, "Help").item(&about).build()?;

    // -- Assemble the full menu bar --
    MenuBuilder::new(app)
        .item(&file_menu)
        .item(&view_menu)
        .item(&window_menu)
        .item(&help_menu)
        .build()
}
