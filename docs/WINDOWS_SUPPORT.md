# Windows 11 Public Beta

Vireo supports 64-bit Windows 11 as a public beta. The Windows build runs the
same local application and machine-learning models as the macOS and Linux builds.
CPU inference is the guaranteed configuration; CUDA and DirectML are not part
of the support commitment yet.

## Included components

- The installer includes the 64-bit ExifTool distribution used for photo
  dates, GPS, camera data, and metadata repair.
- The installer checks for and can bootstrap the Microsoft Edge WebView2
  runtime used by the desktop interface.
- Vireo enables long-path awareness. Windows must also have **Enable Win32
  long paths** turned on for deeply nested libraries; Settings reports when
  that system policy is disabled.
- Uninstalling Vireo removes the application but preserves `.vireo` under the
  user's home directory, including the catalog, settings, downloaded models,
  and logs. XMP files and photos are never application-owned and are not
  removed.

## Optional integrations

Vireo detects standard Windows installation locations and also accepts an
explicit executable path under Settings → Paths.

- Darktable and `darktable-cli` are required only for Darktable development.
- Adobe DNG Converter is required only for Nikon High Efficiency NEF
  conversion.
- Remote import, archive, and move require both the Windows OpenSSH Client
  optional feature and a user-installed GNU rsync. Vireo tests both programs
  before enabling a transfer and preserves originals after any failed or
  unverified transfer.

## Supported storage

The beta covers local NTFS libraries, removable exFAT media, and mounted Server
Message Block (SMB) shares through drive-letter or Universal Naming Convention
(UNC) paths. Windows symlinks are processed when
the current account has permission, but enabling Developer Mode is not a
requirement. A disconnected drive, locked file, read-only destination, or
failed verification must produce an error and leave the original untouched.

## Reporting a Windows problem

Use Help → Report an Issue and mention that the problem occurred on Windows.
The diagnostic bundle includes Windows build, architecture, WebView2 version,
inference provider, dependency readiness, and filesystem type. Tokens,
configured executable paths, and catalog photo paths are redacted.

## Release certification

Every Windows beta release must pass the automated Windows Python, browser,
Rust, sidecar, installer, signature, updater, and uninstall-preservation
checks. Before publishing, certify the release on a clean 64-bit Windows 11
virtual machine and a physical Windows 11 machine with local and removable or
network storage.

The manual journey covers fresh install, JPEG and supported RAW imports, model
download, CPU classification, process/review/cull/browse/edit/export,
duplicates, map, iNaturalist, Darktable, DNG conversion,
local and remote moves, publishing, update from the previous beta, and
uninstall. Do not publish with a confirmed Windows startup, updater, data-loss,
or core-workflow blocker.
