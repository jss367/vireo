use tauri::{
    menu::{AboutMetadataBuilder, MenuBuilder, MenuItemBuilder, PredefinedMenuItem, SubmenuBuilder},
    AppHandle,
};

/// Menu item IDs used to match events in `on_menu_event`.
pub mod ids {
    pub const NAV_BROWSE: &str = "nav_browse";
    pub const NAV_IMPORT: &str = "nav_import";
    pub const NAV_PIPELINE: &str = "nav_pipeline";
    pub const NAV_PIPELINE_REVIEW: &str = "nav_pipeline_review";
    pub const NAV_PIPELINE_RAPID_REVIEW: &str = "nav_pipeline_rapid_review";
    pub const NAV_REVIEW: &str = "nav_review";
    pub const NAV_CULL: &str = "nav_cull";
    pub const NAV_MISSES: &str = "nav_misses";
    pub const NAV_HIGHLIGHTS: &str = "nav_highlights";
    pub const NAV_MAP: &str = "nav_map";
    pub const NAV_VARIANTS: &str = "nav_variants";
    pub const NAV_AUDIT: &str = "nav_audit";
    pub const NAV_COMPARE: &str = "nav_compare";
    pub const NAV_JOBS: &str = "nav_jobs";
    pub const NAV_DUPLICATES: &str = "nav_duplicates";
    pub const NAV_LIGHTROOM: &str = "nav_lightroom";
    pub const NAV_SHORTCUTS: &str = "nav_shortcuts";
    pub const NAV_DASHBOARD: &str = "nav_dashboard";
    pub const NAV_WORKSPACE: &str = "nav_workspace";
    pub const NAV_SETTINGS: &str = "nav_settings";
    pub const NAV_LOGS: &str = "nav_logs";
    pub const REPORT_ISSUE: &str = "report_issue";
    pub const CHECK_FOR_UPDATES: &str = "check_for_updates";
    pub const OPEN_IN_BROWSER: &str = "open_in_browser_now";

    pub const FILE_NEW_WORKSPACE: &str = "file_new_workspace";
    pub const FILE_OPEN_WORKSPACE: &str = "file_open_workspace";
    pub const FILE_IMPORT_PHOTOS: &str = "file_import_photos";
    pub const FILE_IMPORT_FOLDER: &str = "file_import_folder";
    pub const FILE_EXPORT_SELECTED: &str = "file_export_selected";

    pub const PHOTO_OPEN_LIGHTBOX: &str = "photo_open_lightbox";
    pub const PHOTO_REVEAL: &str = "photo_reveal";
    pub const PHOTO_OPEN_EDITOR: &str = "photo_open_editor";
    pub const PHOTO_COPY_PATHS: &str = "photo_copy_paths";
    pub const PHOTO_FIND_SIMILAR: &str = "photo_find_similar";
    pub const PHOTO_COMPARE: &str = "photo_compare";
    pub const PHOTO_ADD_KEYWORD: &str = "photo_add_keyword";
    pub const PHOTO_ADD_COLLECTION: &str = "photo_add_collection";
    pub const PHOTO_DELETE: &str = "photo_delete";
    pub const PHOTO_RATE_0: &str = "photo_rate_0";
    pub const PHOTO_RATE_1: &str = "photo_rate_1";
    pub const PHOTO_RATE_2: &str = "photo_rate_2";
    pub const PHOTO_RATE_3: &str = "photo_rate_3";
    pub const PHOTO_RATE_4: &str = "photo_rate_4";
    pub const PHOTO_RATE_5: &str = "photo_rate_5";
    pub const PHOTO_FLAG_PICK: &str = "photo_flag_pick";
    pub const PHOTO_FLAG_REJECT: &str = "photo_flag_reject";
    pub const PHOTO_FLAG_CLEAR: &str = "photo_flag_clear";

    pub const REVIEW_ACCEPT: &str = "review_accept";
    pub const REVIEW_REJECT: &str = "review_reject";
    pub const REVIEW_ACCEPT_ALL: &str = "review_accept_all";
    pub const REVIEW_PREVIOUS: &str = "review_previous";
    pub const REVIEW_NEXT: &str = "review_next";
    pub const REVIEW_MARK_WILDLIFE: &str = "review_mark_wildlife";
    pub const REVIEW_EXCLUDE_WILDLIFE: &str = "review_exclude_wildlife";

    pub const TOOLS_RUN_PIPELINE: &str = "tools_run_pipeline";
    pub const TOOLS_SCAN_LIBRARY: &str = "tools_scan_library";
    pub const TOOLS_FIND_DUPLICATES: &str = "tools_find_duplicates";
    pub const TOOLS_BUILD_PREVIEWS: &str = "tools_build_previews";
    pub const TOOLS_SYNC_METADATA: &str = "tools_sync_metadata";
    pub const TOOLS_VERIFY_MODELS: &str = "tools_verify_models";
    pub const TOOLS_CANCEL_JOB: &str = "tools_cancel_job";
    pub const TOOLS_REVIEW_PIPELINE: &str = "tools_review_pipeline";
    pub const HELP_KEYBOARD_SHORTCUTS: &str = "help_keyboard_shortcuts";
    pub const HELP_OPEN_HELP: &str = "help_open_help";
    pub const HELP_OPEN_LOGS: &str = "help_open_logs";
    pub const HELP_COPY_DIAGNOSTICS: &str = "help_copy_diagnostics";
}

/// Map a menu item ID to its Flask route path.
pub fn route_for_id(id: &str) -> Option<&'static str> {
    match id {
        ids::NAV_BROWSE => Some("/browse"),
        ids::NAV_IMPORT => Some("/lightroom"),
        ids::NAV_PIPELINE => Some("/pipeline"),
        ids::NAV_PIPELINE_REVIEW => Some("/pipeline/review"),
        ids::NAV_PIPELINE_RAPID_REVIEW => Some("/pipeline/rapid-review"),
        ids::NAV_REVIEW => Some("/review"),
        ids::NAV_CULL => Some("/cull"),
        ids::NAV_MISSES => Some("/misses"),
        ids::NAV_HIGHLIGHTS => Some("/highlights"),
        ids::NAV_MAP => Some("/map"),
        ids::NAV_VARIANTS => Some("/variants"),
        ids::NAV_AUDIT => Some("/audit"),
        ids::NAV_COMPARE => Some("/compare"),
        ids::NAV_JOBS => Some("/jobs"),
        ids::NAV_DUPLICATES => Some("/duplicates"),
        ids::NAV_LIGHTROOM => Some("/lightroom"),
        ids::NAV_SHORTCUTS => Some("/shortcuts"),
        ids::NAV_DASHBOARD => Some("/dashboard"),
        ids::NAV_WORKSPACE => Some("/workspace"),
        ids::NAV_SETTINGS => Some("/settings"),
        ids::NAV_LOGS => Some("/logs"),
        ids::TOOLS_REVIEW_PIPELINE => Some("/pipeline/review"),
        ids::HELP_KEYBOARD_SHORTCUTS => Some("/shortcuts"),
        ids::HELP_OPEN_LOGS => Some("/logs"),
        _ => None,
    }
}

/// Map a menu item ID to a shared browser-side command.
pub fn command_for_id(id: &str) -> Option<&'static str> {
    match id {
        ids::FILE_NEW_WORKSPACE => Some("new_workspace"),
        ids::FILE_OPEN_WORKSPACE => Some("open_workspace"),
        ids::FILE_IMPORT_PHOTOS => Some("import_photos"),
        ids::FILE_IMPORT_FOLDER => Some("import_folder"),
        ids::FILE_EXPORT_SELECTED => Some("export_selected"),
        ids::PHOTO_OPEN_LIGHTBOX => Some("photo_open_lightbox"),
        ids::PHOTO_REVEAL => Some("photo_reveal"),
        ids::PHOTO_OPEN_EDITOR => Some("photo_open_editor"),
        ids::PHOTO_COPY_PATHS => Some("photo_copy_paths"),
        ids::PHOTO_FIND_SIMILAR => Some("photo_find_similar"),
        ids::PHOTO_COMPARE => Some("photo_compare"),
        ids::PHOTO_ADD_KEYWORD => Some("photo_add_keyword"),
        ids::PHOTO_ADD_COLLECTION => Some("photo_add_collection"),
        ids::PHOTO_DELETE => Some("photo_delete"),
        ids::PHOTO_RATE_0 => Some("photo_rate_0"),
        ids::PHOTO_RATE_1 => Some("photo_rate_1"),
        ids::PHOTO_RATE_2 => Some("photo_rate_2"),
        ids::PHOTO_RATE_3 => Some("photo_rate_3"),
        ids::PHOTO_RATE_4 => Some("photo_rate_4"),
        ids::PHOTO_RATE_5 => Some("photo_rate_5"),
        ids::PHOTO_FLAG_PICK => Some("photo_flag_pick"),
        ids::PHOTO_FLAG_REJECT => Some("photo_flag_reject"),
        ids::PHOTO_FLAG_CLEAR => Some("photo_flag_clear"),
        ids::REVIEW_ACCEPT => Some("review_accept"),
        ids::REVIEW_REJECT => Some("review_reject"),
        ids::REVIEW_ACCEPT_ALL => Some("review_accept_all"),
        ids::REVIEW_PREVIOUS => Some("review_previous"),
        ids::REVIEW_NEXT => Some("review_next"),
        ids::REVIEW_MARK_WILDLIFE => Some("review_mark_wildlife"),
        ids::REVIEW_EXCLUDE_WILDLIFE => Some("review_exclude_wildlife"),
        ids::TOOLS_RUN_PIPELINE => Some("tools_run_pipeline"),
        ids::TOOLS_SCAN_LIBRARY => Some("tools_scan_library"),
        ids::TOOLS_FIND_DUPLICATES => Some("tools_find_duplicates"),
        ids::TOOLS_BUILD_PREVIEWS => Some("tools_build_previews"),
        ids::TOOLS_SYNC_METADATA => Some("tools_sync_metadata"),
        ids::TOOLS_VERIFY_MODELS => Some("tools_verify_models"),
        ids::TOOLS_CANCEL_JOB => Some("tools_cancel_job"),
        ids::HELP_OPEN_HELP => Some("help_open_help"),
        ids::HELP_COPY_DIAGNOSTICS => Some("help_copy_diagnostics"),
        _ => None,
    }
}

fn build_about_metadata() -> tauri::menu::AboutMetadata<'static> {
    AboutMetadataBuilder::new()
        .name(Some("Vireo"))
        .version(Some(env!("CARGO_PKG_VERSION")))
        .comments(Some("AI-powered wildlife photo organizer"))
        .website(Some("https://github.com/jss367/vireo"))
        .website_label(Some("GitHub"))
        .license(Some("MIT"))
        .build()
}

/// Build the application menu bar.
pub fn build_menu(app: &AppHandle) -> tauri::Result<tauri::menu::Menu<tauri::Wry>> {
    // -- macOS app submenu --
    #[cfg(target_os = "macos")]
    let app_menu = {
        let about = PredefinedMenuItem::about(app, Some("About Vireo"), Some(build_about_metadata()))?;
        let settings_item = MenuItemBuilder::with_id(ids::NAV_SETTINGS, "Settings...")
            .accelerator("CmdOrCtrl+,")
            .build(app)?;

        SubmenuBuilder::new(app, "Vireo")
            .item(&about)
            .separator()
            .item(&settings_item)
            .separator()
            .services()
            .separator()
            .hide()
            .hide_others()
            .show_all()
            .separator()
            .quit()
            .build()?
    };

    // -- File menu --
    let file_menu = SubmenuBuilder::new(app, "File")
        .item(
            &MenuItemBuilder::with_id(ids::FILE_NEW_WORKSPACE, "New Workspace...")
                .accelerator("CmdOrCtrl+Shift+N")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::FILE_OPEN_WORKSPACE, "Open Workspace...")
                .build(app)?,
        )
        .separator()
        .item(
            &MenuItemBuilder::with_id(ids::FILE_IMPORT_PHOTOS, "Import Photos...")
                .accelerator("CmdOrCtrl+I")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::FILE_IMPORT_FOLDER, "Import Folder...")
                .accelerator("CmdOrCtrl+Shift+I")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::FILE_EXPORT_SELECTED, "Export Selected Photos...")
                .accelerator("CmdOrCtrl+E")
                .build(app)?,
        )
        .separator()
        .close_window()
        .separator()
        .quit()
        .build()?;

    // -- Edit menu (required for Cmd+C/V/X/A/Z on macOS) --
    let edit_menu = SubmenuBuilder::new(app, "Edit")
        .undo()
        .redo()
        .separator()
        .cut()
        .copy()
        .paste()
        .select_all()
        .build()?;

    // -- View menu (page navigation) --
    let mut view_builder = SubmenuBuilder::new(app, "View");
    view_builder = view_builder
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
            &MenuItemBuilder::with_id(ids::NAV_PIPELINE_RAPID_REVIEW, "Rapid Review")
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
        .item(
            &MenuItemBuilder::with_id(ids::NAV_MISSES, "Misses")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_HIGHLIGHTS, "Highlights")
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
        .item(
            &MenuItemBuilder::with_id(ids::NAV_DUPLICATES, "Duplicates")
                .build(app)?,
        )
        .separator()
        .item(
            &MenuItemBuilder::with_id(ids::NAV_JOBS, "Jobs")
                .accelerator("CmdOrCtrl+Shift+J")
                .build(app)?,
        )
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
            &MenuItemBuilder::with_id(ids::NAV_LOGS, "Logs")
                .accelerator("CmdOrCtrl+Shift+L")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_SHORTCUTS, "Shortcuts")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::NAV_LIGHTROOM, "Lightroom")
                .build(app)?,
        );

    // Settings in View menu only on non-macOS (it is in the app submenu on macOS)
    #[cfg(not(target_os = "macos"))]
    {
        view_builder = view_builder.separator().item(
            &MenuItemBuilder::with_id(ids::NAV_SETTINGS, "Settings")
                .accelerator("CmdOrCtrl+,")
                .build(app)?,
        );
    }

    view_builder = view_builder.separator().item(
        &MenuItemBuilder::with_id(ids::OPEN_IN_BROWSER, "Open in Browser")
            .accelerator("CmdOrCtrl+Shift+B")
            .build(app)?,
    );

    let view_menu = view_builder.build()?;

    // -- Photo menu --
    let photo_menu = SubmenuBuilder::new(app, "Photo")
        .item(
            &MenuItemBuilder::with_id(ids::PHOTO_OPEN_LIGHTBOX, "Open in Lightbox")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::PHOTO_REVEAL, "Reveal in Finder")
                .accelerator("CmdOrCtrl+R")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::PHOTO_OPEN_EDITOR, "Open in External Editor")
                .accelerator("CmdOrCtrl+Shift+E")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::PHOTO_COPY_PATHS, "Copy Path")
                .accelerator("CmdOrCtrl+Option+C")
                .build(app)?,
        )
        .separator()
        .item(
            &MenuItemBuilder::with_id(ids::PHOTO_FIND_SIMILAR, "Find Similar")
                .accelerator("CmdOrCtrl+F")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::PHOTO_COMPARE, "Compare Selected")
                .build(app)?,
        )
        .separator()
        .item(
            &MenuItemBuilder::with_id(ids::PHOTO_ADD_KEYWORD, "Add Keyword...")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::PHOTO_ADD_COLLECTION, "Add to Collection...")
                .build(app)?,
        )
        .separator()
        .item(&MenuItemBuilder::with_id(ids::PHOTO_RATE_0, "Rate 0").build(app)?)
        .item(&MenuItemBuilder::with_id(ids::PHOTO_RATE_1, "Rate 1").build(app)?)
        .item(&MenuItemBuilder::with_id(ids::PHOTO_RATE_2, "Rate 2").build(app)?)
        .item(&MenuItemBuilder::with_id(ids::PHOTO_RATE_3, "Rate 3").build(app)?)
        .item(&MenuItemBuilder::with_id(ids::PHOTO_RATE_4, "Rate 4").build(app)?)
        .item(&MenuItemBuilder::with_id(ids::PHOTO_RATE_5, "Rate 5").build(app)?)
        .separator()
        .item(&MenuItemBuilder::with_id(ids::PHOTO_FLAG_PICK, "Flag as Pick").build(app)?)
        .item(&MenuItemBuilder::with_id(ids::PHOTO_FLAG_REJECT, "Reject Photo").build(app)?)
        .item(&MenuItemBuilder::with_id(ids::PHOTO_FLAG_CLEAR, "Clear Flag").build(app)?)
        .separator()
        .item(
            &MenuItemBuilder::with_id(ids::PHOTO_DELETE, "Delete Selected")
                .build(app)?,
        )
        .build()?;

    // -- Review menu --
    let review_menu = SubmenuBuilder::new(app, "Review")
        .item(
            &MenuItemBuilder::with_id(ids::REVIEW_ACCEPT, "Accept Prediction")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::REVIEW_REJECT, "Reject Prediction")
                .build(app)?,
        )
        .item(&MenuItemBuilder::with_id(ids::REVIEW_ACCEPT_ALL, "Accept All Pending").build(app)?)
        .separator()
        .item(
            &MenuItemBuilder::with_id(ids::REVIEW_PREVIOUS, "Previous Photo")
                .build(app)?,
        )
        .item(
            &MenuItemBuilder::with_id(ids::REVIEW_NEXT, "Next Photo")
                .build(app)?,
        )
        .separator()
        .item(&MenuItemBuilder::with_id(ids::REVIEW_MARK_WILDLIFE, "Mark as Wildlife").build(app)?)
        .item(
            &MenuItemBuilder::with_id(ids::REVIEW_EXCLUDE_WILDLIFE, "Exclude from Wildlife Classification")
                .build(app)?,
        )
        .build()?;

    // -- Tools menu --
    let tools_menu = SubmenuBuilder::new(app, "Tools")
        .item(
            &MenuItemBuilder::with_id(ids::TOOLS_RUN_PIPELINE, "Run Pipeline")
                .accelerator("CmdOrCtrl+Return")
                .build(app)?,
        )
        .item(&MenuItemBuilder::with_id(ids::TOOLS_REVIEW_PIPELINE, "Review Pipeline Results").build(app)?)
        .separator()
        .item(&MenuItemBuilder::with_id(ids::TOOLS_SCAN_LIBRARY, "Scan Library...").build(app)?)
        .item(&MenuItemBuilder::with_id(ids::TOOLS_FIND_DUPLICATES, "Find Duplicates").build(app)?)
        .item(&MenuItemBuilder::with_id(ids::TOOLS_BUILD_PREVIEWS, "Build/Refresh Previews").build(app)?)
        .separator()
        .item(&MenuItemBuilder::with_id(ids::TOOLS_SYNC_METADATA, "Write XMP Metadata").build(app)?)
        .item(&MenuItemBuilder::with_id(ids::TOOLS_VERIFY_MODELS, "Verify Models").build(app)?)
        .separator()
        .item(&MenuItemBuilder::with_id(ids::TOOLS_CANCEL_JOB, "Cancel Current Job").build(app)?)
        .build()?;

    // -- Window menu --
    let mut window_builder = SubmenuBuilder::new(app, "Window")
        .minimize()
        .maximize();

    #[cfg(target_os = "macos")]
    {
        window_builder = window_builder.fullscreen();
    }

    let window_menu = window_builder.separator().close_window().build()?;

    // -- Help menu --
    #[allow(unused_mut)]
    let mut help_builder = SubmenuBuilder::new(app, "Help");

    let keyboard_shortcuts = MenuItemBuilder::with_id(ids::HELP_KEYBOARD_SHORTCUTS, "Keyboard Shortcuts")
        .build(app)?;
    let open_help = MenuItemBuilder::with_id(ids::HELP_OPEN_HELP, "Vireo Help")
        .accelerator("F1")
        .build(app)?;
    let open_logs = MenuItemBuilder::with_id(ids::HELP_OPEN_LOGS, "Open Logs")
        .build(app)?;
    let copy_diagnostics = MenuItemBuilder::with_id(ids::HELP_COPY_DIAGNOSTICS, "Copy Diagnostics")
        .build(app)?;
    let check_updates = MenuItemBuilder::with_id(ids::CHECK_FOR_UPDATES, "Check for Updates...")
        .build(app)?;
    let report_issue = MenuItemBuilder::with_id(ids::REPORT_ISSUE, "Report an Issue...")
        .build(app)?;
    help_builder = help_builder
        .item(&keyboard_shortcuts)
        .item(&open_help)
        .separator()
        .item(&open_logs)
        .item(&copy_diagnostics)
        .separator()
        .item(&check_updates)
        .separator()
        .item(&report_issue);

    #[cfg(not(target_os = "macos"))]
    {
        let about = PredefinedMenuItem::about(app, Some("About Vireo"), Some(build_about_metadata()))?;
        help_builder = help_builder.separator().item(&about);
    }

    let help_menu = help_builder.build()?;

    // -- Assemble --
    let mut builder = MenuBuilder::new(app);

    #[cfg(target_os = "macos")]
    {
        builder = builder.item(&app_menu);
    }

    builder
        .item(&file_menu)
        .item(&edit_menu)
        .item(&view_menu)
        .item(&photo_menu)
        .item(&review_menu)
        .item(&tools_menu)
        .item(&window_menu)
        .item(&help_menu)
        .build()
}
