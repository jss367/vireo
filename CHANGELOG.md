# Changelog

All notable changes to Vireo are documented in this file.

## Unreleased

### Added
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

### Fixed
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
