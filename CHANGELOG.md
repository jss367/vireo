# Changelog

All notable changes to Vireo are documented in this file.

## Unreleased

### Added
- **Windows 11 public beta.** Windows releases now include ExifTool, report
  optional integration readiness, support long-path-aware packaging, require
  signed release installers, and run Windows browser, native-shell, installer,
  updater, and uninstall-preservation gates before publication.

### Fixed
- Miss detection now aligns its default no-subject threshold with the default
  detector confidence floor, avoiding "no subject" misses for photos whose
  bird detection is already visible. Existing installs that had the previous
  defaults persisted in `~/.vireo/config.json` or in a workspace's saved
  overrides get a one-time migration to the new defaults on next startup;
  any user-customized thresholds are left untouched.

### Changed
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
