# Changelog

All notable changes to Vireo are documented in this file.

## Unreleased

### Changed
- **ID Conflicts loads a page at a time.** The page used to fetch every photo
  in the collection and render every matching row at once — on a large
  catalog that meant a 159 MB response and tens of thousands of table rows,
  which took minutes to appear. The comparison is now worked out on the
  server and the page asks for one page of rows plus the counts, so it opens
  in seconds and filtering, sorting, searching and paging are immediate. The
  filter chips, pager and batch actions now stay pinned to the top of the
  list instead of scrolling out of reach.

### Added
- **The Jobs page names the species list a classification ran against.** A
  classify step used to say only which model was running, so a run against
  your active regional lists looked identical to one falling back to the full
  Tree of Life. The step now carries a line naming the label space — the
  species count and which lists were merged, “Tree of Life” when no list is
  active, or the model’s own built-in classes for models that ignore lists —
  and it stays on the step in job history.
- **Described color labels.** Right-click any photo color label to give that
  color a short, workspace-specific meaning such as “Reptiles.” The meaning
  appears in color-label tooltips wherever that color is shown, including the
  lightbox context menu.
- **Automatic catalog backup before upgrades.** When a new Vireo version
  needs to migrate the database, a snapshot is saved next to it (e.g.
  `vireo.db.pre-v8.bak`) before any migration runs. Only the most recent
  pre-upgrade snapshot is kept.
- **Advanced color and tone editing.** The non-destructive photo editor now
  includes a five-point tone curve, an eight-color hue/saturation/luminance
  mixer, and independent shadow, midtone, and highlight color grading. These
  adjustments work in live previews, reusable presets, copied settings, edit
  history, and final exports.
- **Reliable photo metadata on macOS.** macOS releases now bundle a pinned,
  checksum-verified ExifTool. Import checks metadata readiness before starting,
  offers an explicit advanced metadata-free override, and can repair photos
  imported by an older or damaged installation.
- **Windows 11 public beta.** Windows releases now include ExifTool, report
  optional integration readiness, support long-path-aware packaging, require
  signed release installers, and run Windows browser, native-shell, installer,
  updater, and uninstall-preservation gates before publication.
- **"Check again" for offline folders.** When the new-images banner reports
  that a registered folder's volume is offline, it now offers a manual
  recheck and shows the time of the check behind the message. Clicking it
  forgets every cached volume verdict and re-walks straight away, so a share
  you have just remounted is picked up immediately instead of on the
  automatic 30-second retry. The notice also groups the offline folders under
  the path they share, so a set of sibling folders reads as the parts that
  differ rather than as several near-identical absolute paths, with the full
  paths on hover.

### Fixed
- **Offline NAS no longer breaks the new-images check.** When a registered
  folder's volume is unreachable (an SMB share that dropped mid-walk, or one
  that is not mounted), the new-images walk now skips that folder and keeps
  checking the others instead of failing with a traceback. The banner says
  which folder is offline and that it was not checked, so a count shown for
  the remaining folders is never mistaken for the whole library. A single
  bounded reachability check per volume is consulted before any walk and is
  shared across the app, so a dead share is never hammered on every poll.
- **Background polls no longer tie up the app.** The navbar's new-images
  poll used to block for half a second on every request while a long walk
  was already running, and the Work Locally status poll on Browse recounted
  every root's photos on each tick. Both now answer immediately; on an
  88,000-photo catalog this removes thousands of slow-request warnings per
  day from the log.
- Large existing photo catalogs no longer fail Vireo's startup check while a
  one-time Wildlife metadata migration scans tens of thousands of sidecar
  files. The app opens first and completes that migration in the background.
- Wildlife detection now retries weak full-frame MegaDetector results on
  overlapping higher-resolution crops. Small or unusually posed birds that
  were previously labeled "No subject" can clear the normal confidence floor
  without lowering it globally; cached detector results are versioned so the
  improved pass is not skipped. Every crop in the grid is scanned, so a
  second animal lying along the boundary between two crops is no longer
  missed. Miss cards now state the applicable threshold and show the best
  below-threshold candidate confidence, and threshold previews report the
  floor they were actually computed with rather than the last saved one.
- Freeing card space now verifies only archive copies that match the selected
  card instead of re-hashing the entire workspace, refreshes the preview
  automatically, and reads each card file only once at deletion.
- Opening a catalog that was already migrated by a newer Vireo (for example
  after reinstalling an older build) now shows clear "update Vireo to open
  this catalog" guidance instead of a crash — and no longer suggests moving
  the database aside, which would have orphaned the newer catalog.
- A corrupt `~/.vireo/config.json` is now preserved as `config.json.corrupt`
  before Vireo falls back to defaults, instead of being silently overwritten
  on the next settings change.

- Settings export no longer includes secret values (iNaturalist token,
  Hugging Face token, Google Maps API key); the exported file lists which
  keys were omitted. Importing a backup keeps the secrets already configured
  on the machine unless the file explicitly provides them.
- After-import classification now pauses with an actionable label-download
  message when the selected model cannot run without a species list, instead
  of enqueueing a pipeline job guaranteed to fail.
- Miss detection now aligns its default no-subject threshold with the default
  detector confidence floor, avoiding "no subject" misses for photos whose
  bird detection is already visible. Existing installs that had the previous
  defaults persisted in `~/.vireo/config.json` or in a workspace's saved
  overrides get a one-time migration to the new defaults on next startup;
  any user-customized thresholds are left untouched.

### Changed
- **Wildlife classification is now explicit workflow state.** Adding a species
  no longer creates a redundant `Wildlife` keyword. Existing generated terms
  are retired without disturbing real keyword hierarchies, and Browse now
  groups selected-photo metadata by meaning with dedicated bulk controls for
  including or excluding photos from wildlife processing. Keywords you add by
  hand now record that you added them, so a tag is never mistaken for a
  generated one once its edit history ages out — and that record follows the
  keyword through duplicate merges, keyword renames, and RAW/JPEG pairing.
- **Work Locally follows folders across workspaces.** Local copies are now
  managed per top-level folder. A folder shared by several workspaces uses one
  local copy in all of them, while workspace controls can stage or finish
  several folders together. Individual folders can also be staged, synced, or
  discarded from the Workspace page.
- **Global detection/classifier cache.** MegaDetector and classifier results
  are now cached per-photo instead of per-workspace. Switching to a new
  workspace or changing your detector confidence threshold no longer
  triggers a full reprocess.
- **Threshold is now a read-time filter.** Lowering `detector_confidence` in
  workspace config takes effect immediately; you no longer need to rerun
  detection to see previously-subthreshold boxes.
- Legacy detections from prior versions are preserved but pre-filtered. Run
  "Reclassify" once per folder to regenerate them with the new raw storage
  if you want to take full advantage of low-threshold browsing.
