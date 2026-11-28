# Finding and changing settings in Vireo

**Proposed design · November 25, 2026**

Open Settings and the first control you see should help you change how you browse photos. If you came to connect an editor, check an identification model, or adjust a workspace override, the same screen should show you where to go.

## Why change the page?

At a desktop viewport of 1280 × 800, the live Settings page opened with Storage, an experimental computation cache, and model downloads. Display began about 2,800 pixels down. Appearance began about 9,650 pixels down. The full page was roughly 20,400 pixels tall. These are observations from one populated installation. Model availability, saved locations, and platform-specific sections affect the height.

Once you reach “Default browse thumbnail,” the existing slider and pixel value make the decision easy. The work is locating that row. Cmd/Ctrl+F searches text across the page, while a second search in All settings filters the configuration editor. Even someone who knows the word “thumbnail” has to understand which search they found.

## Page layout

Keep Vireo’s app navigation. Below it, place a Settings heading and an always-visible **Search settings** field. On desktop, put a narrow, persistent category list on the left and a single column of settings on the right. Highlight the current category and repeat its name as the content heading. Keep the compact rows, aligned controls, units beside values, and existing theme colors.

A plain visit to Settings opens **Browsing & Appearance**, with thumbnail size and photos per page first. In a narrow window, replace the left column with a labeled category selector beneath search. Keep controls and their values together when a row wraps.

| Category | What belongs here |
| --- | --- |
| Browsing & Appearance | Thumbnail size, photos per page, browse grid options, quick filters, theme, time format, navigation, and desktop launch behavior. |
| Identification & Models | Active models and their roles, species lists, taxonomy, classification defaults (confidence, grouping window, similarity, keyword casing), detector confidence, detection box padding, and the number of predictions kept per photo. |
| Processing & Culling | The seven Pipeline Tuning groups: quality scoring weights, rejection thresholds, miss detection, eye-focus detection, burst detection, selection, and encounter segmentation. Also the redundancy threshold, cull time window, and cull pHash threshold, which sit in Detection & Classification today. |
| Integrations | Hugging Face, iNaturalist, Google Maps, location metadata in photo sidecars, AI assistants, external editors, and raw photo development. Keep service credentials alongside their connection status. |
| Storage & Maintenance | Link to the Storage page for disk usage and preview caches. Place Catalog backups here, including **Back up now**, backup history, and the restore notice. Keep remote destinations and their setup wizard, saved scan folders, computation-cache transfers, the edit history limit, and keyword cleanup here. |
| Advanced | Full configuration editor, global and workspace scopes, reset, and settings import and export. Group system diagnostics and desktop version and update information separately from editable preferences. |

Processing & Culling is its own category because pipeline settings account for nearly half the configuration schema. Left inside Identification & Models, they would make that one category most of the old page.

### Addresses

The category goes in the query string: `/settings?category=integrations`. The names are `browsing`, `identification`, `processing`, `integrations`, `storage`, and `advanced`. Selecting a category pushes a history entry, so a bookmark, reload, or browser Back returns to the same place.

The hash keeps its current job of pointing at one section or action inside the page (`#google-maps`, `#nas-setup`). A category in the hash would collide with those links. An address with a hash and no `category` resolves the category from the hash, so every existing link keeps working unchanged.

## Three paths through the design

**Change thumbnail size.** Open Settings, adjust the slider in Browsing & Appearance, and read the new pixel value beside it. Use the existing saving, saved, and error feedback in a stable location near the heading. A pending save remains visible when the user switches categories. Show help text where it explains a consequence, such as a change taking effect after restart.

**Check which model identifies species.** Open Identification & Models. Lead with a short summary showing each role, the model currently used for it, and whether it is ready. If a required model is missing or needs repair, show that state and its relevant action here. **Manage models** opens the full list grouped by purpose. Keep variant comparisons, download sizes, local weights, verification, removal, and the setup wizard there. Distinguish a model selected for species identification from an installed model available to the culling pipeline.

**Find a setting by name.** Typing “thumbnail” or pressing Cmd/Ctrl+F and typing it opens a result list below the same search field. Each result shows the setting label and category. Search across categories and collapsed groups using labels, descriptions, common synonyms, and configuration keys.

Cmd/Ctrl+F is a literal text find over the whole page today, and in the desktop app it is the only find. The new search has to keep finding what that one finds. Index the names of the things the page lists as well as the settings: installed and downloadable models, species lists, remote destinations, saved scan folders, external editors, and quick filters. A result for one of these opens its category and scrolls to its row. Rebuild those entries when the list they come from reloads.

Credentials and the values typed into fields stay out of the index. The current find already skips field values.

Return one result per matching preference for a given query. For a label, description, or synonym, open its curated control if one exists and its Advanced row otherwise. An exact configuration key opens its Advanced row directly. Actions have their own results. Searching “backup” should find **Back up now** in Storage & Maintenance, and both **Daily catalog backups kept** and **Catalog backup folder** in Advanced. Enter opens the selected result, reveals its category and enclosing group, scrolls it into view, and focuses the control. Escape dismisses results and returns focus to search. For an empty result list, show the query and a clear action to reset it. Opening a result for a command such as keyword cleanup focuses its button and leaves execution to the user.

### Where the search index comes from

Each curated row declares itself in the markup: a stable id, its category, and the configuration keys its control reads and writes, in data attributes on the row. Build the index from those attributes and from the configuration schema. Today the mapping from a control to its keys exists only as hand-written load and save code in `config.js`, which search cannot read.

Test both routes independently. Enumerate schema keys and verify each exact-key query resolves to its Advanced row. Enumerate curated preference controls from the rendered form, independently of their search attributes, and verify each visible label finds its own control once. This catches a missing declaration even when the key already resolves in Advanced. Flag competing curated declarations for the same preference.

The four workspace override controls share one label, “Override for” followed by the workspace name. Leave them out of the label test. Each belongs to its setting’s row, so a search for “confidence” lists the confidence row once and the override control is beside the result.

## Scope and save behavior

Many global preferences can be overridden per workspace. Only four curated rows have an override control today: classification confidence, grouping window, similarity threshold, and detector confidence. Every other curated row shows the global value, because the curated form loads from `/api/config`, which returns the global preference. A workspace override set in the Advanced editor for a quality weight or for the number of predictions kept is in effect and invisible on its curated row.

Keep the global value in the editable global control. When an override exists, use `/api/settings/values` to show the workspace name and effective value beside it. For example, the prediction-count input might show **Global: 5**, with **Wetlands uses 2** beside it. The workspace value is display text outside the global form inputs. Saving an unrelated preference leaves the stored global value at 5 and the workspace override at 2. **Edit override** opens that key’s Advanced row with **Workspace** selected for the active workspace.

A curated row whose key can be overridden, and has no override yet, offers a quiet **Override for this workspace** link to the same destination. Without it, someone who searches for a quality weight lands on the global control with no sign that a per-workspace value is possible. Close to forty rows qualify, most of them in Processing & Culling, so the link appears only on the row that holds focus and on a row opened from a search result. The four rows with their own override control do not need the link.

Keep the four existing override controls, each with the active workspace name beside it. A photographer adjusting classification confidence should be able to tell whether the displayed value comes from the global preference or this workspace. In Advanced, retain the scope selector, the value’s source, and Reset. For a workspace override, explain that Reset restores inheritance from the global preference. Keep the curated row and the configuration editor synchronized after an edit, reset, or import.

For curated edits, work out the affected rows by comparing the form with what was last saved. A save cannot tell which row changed. The template has 60 inline `saveConfig()` calls that pass nothing, and the scripts call it from about 17 more places. Several of those follow no `input` or `change` event, so a listener for those events would miss them. **Reset pipeline defaults** sets its fields in code. The Browse buttons in RAW development fill a path from the file picker. Removing an external editor or a remote destination is a click.

After loading each scope, serialize its controls once to establish the baseline for detecting edits. A detector confidence stored as 0.405 reads 0.41 beside the slider. Treat 0.41 as the starting position for that control. When Photos per page changes, omit detector confidence from the request so the server retains 0.405. Moving the slider to 0.42 sends 0.42.

Keep each scope’s curated controls read-only until its first successful load establishes a baseline. If the global read fails, the page status says “Global settings couldn’t be loaded” and offers **Reload global settings**. Give a workspace read failure the corresponding workspace label and reload action. Reload reads only the affected scope. On success, populate its controls with stored values, establish its baseline, and enable editing. If the read fails again, keep the action available. A scope that has loaded successfully remains editable throughout.

Enforce the same rule where writes start, including direct calls to `_saveConfigNow()`. An explicit save without a baseline must reject before sending a request so the caller can report that nothing was saved. In the network-storage setup wizard, keep the dialog open with the entered destination details, roll back the temporary destination in the list, and offer **Reload global settings** inside the dialog. After recovery, the user can press **Save** again.

When a queued save starts, compare the current serialized values with that scope’s baseline and send only changed settings. Compare nested pipeline settings by individual key and send only the changed ones. The server already merges a partial `pipeline` block into the stored one (`_merge_config_body` in `vireo/web/settings.py`), so this needs no server change. Treat each list of external editors, remote destinations, or quick filters as one setting. When the external editors list is sent, send an empty legacy `external_editor` field with it. Today every save clears that field, and without the clear a removed editor comes back from the legacy value. Preserve explicit clears and workspace resets. Capture the scope, generation, affected rows, and exact outgoing payload before sending.

On success, update the baseline from the captured payload for each key the server accepted. An explicit workspace reset removes the override from the baseline. An omitted key keeps its previous baseline until a successful read from the server refreshes it. Edits made while a request is in flight belong to a later request, even when the user edits the same control again.

On failure, retain the baseline. In the page status, group failed rows under **Global** or the workspace name and link each row to its control. Each group has its own **Retry** action with an accessible name that includes the scope, such as “Retry Wetlands changes.” Retry runs the normal save for that group’s current edits. Keep the other group’s links and Retry available while the request runs and after it succeeds. Clear only failures resolved in the retried scope.

A save or reset in Advanced reloads the curated form for its scope from the server, and so does a settings import. If a curated failure is outstanding in that scope, the reload replaces the unsaved values on screen. Rebuild the baseline from the reloaded form and replace the failure message with one that says the changes were discarded, such as “2 unsaved changes were replaced by the stored values.” Keep the links to those rows so the user can make the changes again.

In Advanced, keep saving, saved, and failed feedback beside the affected row in the scope where the edit occurred. Returning to that row reveals the error.

## Implementation sequence

New code stays in classic scripts under `vireo/static/settings/`, like the rest of the page. Converting Settings to a module page is a separate change.

1. Group the existing template sections under category headings and add the `category` address handling. Keep the controls mounted when switching categories because the current save code reads fields across the form. Preserve the shared save queue and initial-load checks, and use the baseline for each scope to send changed settings and route failures to that scope’s Retry action. Resolve the address before choosing the initial category.

   Give every curated row its stable id, its category attribute, and the configuration keys it reads and writes in this step. The category containers need the first two. Failure reporting needs the keys to get from a changed key in a request to its row. The search terms on the same rows wait for step 3.

   Keep the network storage wizard’s overlay outside the category containers. It is a fixed overlay, and a hidden category would hide it.

   Teach the current find about categories in this step. `isSettingsFindTextHidden` skips any hidden text except inside collapsible groups and the Advanced block, and `revealSettingsFindMatch` opens only those two. Without this change Cmd/Ctrl+F searches only the category on screen until step 3 ships. Both functions treat a hidden category like a collapsed group, and revealing a match switches to its category.

   Point every link into Settings at its category:

   | Link | From | Opens |
   | --- | --- | --- |
   | `/settings#google-maps` | Location review (`location-review/map.js`, `candidates.js`) | Integrations, at Google Maps. Unchanged address. |
   | `/settings#nas-setup` | Import (`import/after-import.js`) | Storage & Maintenance, with the wizard open. Unchanged address. |
   | `/settings` “Update” | Update banner (`tauri-bridge.js`) | Advanced, at the version and update section. Give that section an anchor. |
   | `/settings` “Download a model in Settings” and “Download MegaDetector from the Settings page” | Dashboard (`dashboard/inventory.js`, `dashboard/charts.js`) | Identification & Models. |
   | `/settings` “Manage in Settings” | Storage page, Models card (`storage/boot.js`) | Identification & Models. |

2. Consolidate the three model sections around the role summary and management view. Reuse the model status and action handlers. Check the summary in ready, missing, and repair states.
3. Replace the two search entry points with one. Add search terms to the curated rows, beside the ids and configuration keys from step 1, and build the index from them and from the configuration schema. Respect platform availability when indexing controls. Add the override marker and the **Override for this workspace** link to curated rows in this step, since search is what makes the curated row the destination.

   The Catalog backups section tells the user to find “Daily catalog backups kept” in Settings. Make that sentence a link to the setting’s Advanced row.

   The shared page find hands Cmd/Ctrl+F to Settings by calling `openSettingsFind` (`vireo-page-find.js`). Keep that name or change the caller. Three browser tests hold the current find and change with it: `test_settings_cmd_f_opens_page_text_search` and `test_settings_text_search_ignores_hidden_update_section` in `tests/e2e/test_settings.py`, and `test_native_find_uses_existing_page_search` in `tests/e2e/test_page_find.py`.

Implementation references include the [settings template](../../../vireo/templates/settings.html), [frontend responsibilities and save behavior](../../../vireo/static/settings/README.md), and [existing settings browser tests](../../../tests/e2e/test_settings.py).

## Acceptance checks

- At 1280 × 800, a fresh visit shows thumbnail size, its current value, and every category without scrolling. In a 640-pixel-wide window, search, category selection, and the slider work without horizontal scrolling.
- A keyboard user can focus search, select a result in a collapsed group, change its value, and see or hear the save outcome. The selected category, focused control, and save errors are distinguishable without color alone.
- Searching “thumbnail” lists the default browse thumbnail preference once and opens its curated slider. Searching `browse_thumb_default` opens its Advanced row. An unmatched query can be cleared without losing the current category.
- Searching the name of an installed model, a species list, a remote destination, or a saved scan folder opens its row. No query matches a stored credential.
- Every exact schema key opens its Advanced row. Searching by each visible curated preference label finds the corresponding control once. Removing its search declaration fails the label test even if exact-key search still passes.
- Searching “backup” finds **Back up now**, **Daily catalog backups kept**, and **Catalog backup folder**. Opening the action result focuses its button. A backup starts only when the user activates it. The Catalog backups section’s mention of “Daily catalog backups kept” is a link that opens that Advanced row.
- From Identification & Models, a user can identify the active species model before expanding management, then download or verify a model through the existing workflow.
- Set the global prediction count to 5 and the active workspace override to 2 in Advanced. The curated global input shows 5 alongside the workspace name and its effective value of 2. Change photos per page, then reload. Verify both stored values are intact. Follow **Edit override**, confirm Workspace is selected, and reset it. Both views now show the inherited 5. Focus the curated input: its row offers **Override for this workspace**, which opens the same Advanced row with Workspace selected. Other overridable rows show no link until they hold focus or are opened from search.
- Load a global detector confidence of 0.405 and verify the slider reads 0.41. Change Photos per page. Verify its request omits detector confidence, then reload and read the stored global value through the settings API. It must still be 0.405. Move the slider to 0.42 and verify the stored value becomes 0.42.
- Store 80 photos per page. Fail the initial global read and let the workspace read succeed. Verify Photos per page is read-only, the global load error offers **Reload global settings**, and a workspace override still saves. Fail the first reload and succeed on the second. Recovery sends no global write. The control now shows 80. Change it to 90, reload the page, and verify both 90 and the workspace override persisted.
- Open the network-storage wizard through its Import link while the initial global read fails. Enter a destination and press **Save**. Verify no write is sent, the wizard reports the failure and retains the entered details, and the temporary destination is removed from the list. Use **Reload global settings** in the dialog, then press **Save**. The wizard closes after the successful write, and the destination appears exactly once after a page reload.
- With only the legacy single external editor stored, remove that editor and reload. No editor is listed.
- Fail a global Photos per page edit and a Wetlands workspace override edit. Both groups show their affected rows and a scope-labeled Retry. Retry Global and verify only a global request is sent. After it succeeds, Wetlands still shows its failed row and an enabled Retry. Retry Wetlands, reload, and verify both edits persisted.
- Start at 50 photos per page. Change it to 60 and hold the response after the request is sent. Change it to 70, let the request for 60 succeed, then fail the request for 70 before it commits. The saved baseline and stored value are 60. The input still shows 70, and the error links to Photos per page. Use **Retry** and reload. The input now shows the stored 70 and the error has cleared.
- Switch categories during a pending curated save and verify the edit persists. Then edit two global settings within one debounce window, delay and fail that request, and complete a separate workspace save. From another category, the page status links to both failed global edits. Then change a third global setting and let that save succeed: the links disappear, and a reload shows all three values stored.
- Use **Reset pipeline defaults** and fail that save: the page status links to the rows the reset changed. Remove an external editor and fail that save: the status links to the External Editors section.
- With a failed curated global edit outstanding, save a different global value in Advanced. The failure message is replaced by one saying the unsaved change was discarded, it still links to the row, and the row shows the stored value.
- A failed Advanced edit remains visible beside its row when returning to the same scope.
- Each link in the table under step 1 opens its category on first load, and the two that carry a hash reach their section or wizard. Browser Back restores the prior category after following a search result. Desktop-only actions appear on supported platforms.
- After step 1 alone, Cmd/Ctrl+F finds text in a category that is not on screen and switches to it.
