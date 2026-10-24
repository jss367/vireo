# Uninstalling Vireo and reclaiming disk space

Removing the application does not remove Vireo's catalog, settings, downloaded
models, or support files. Keep these if you plan to reinstall and resume work.
You can also remove downloaded models and disposable caches while keeping the
catalog and your photo library.

## Clean up while Vireo is installed

1. Finish or cancel running jobs. Complete any pending photo transfers and
   metadata syncs you want to keep.
2. Open **Storage** and review the listed locations and sizes. Use **Open folder**
   to inspect a location. Record custom locations before uninstalling.
3. In **Settings → Models**, remove models you no longer need. Vireo-managed
   models are deleted, and Vireo also attempts to remove their Hugging Face
   cache entries. Custom models are unregistered without deleting their files.
4. In **Storage → Hugging Face Cache → Manage**, review remaining entries and
   remove unused ones. Check any cleanup errors or retained-file explanations;
   model removal does not guarantee that every cached file was deleted.
5. Use **Storage → Clear safe caches** for disposable generated files. This
   action does not remove models, originals, or the catalog. Review other
   storage categories separately.

Models are downloaded again when needed. Regenerating image caches requires
access to the source photos. When keeping Vireo, use its cleanup controls rather
than deleting its data folder by hand.

## Manual cleanup on macOS

Quit Vireo and any separately started Vireo server before moving or removing
files. To open a hidden folder in Finder, choose **Go → Go to Folder**
(`Shift-Command-G`) and paste its path.

| Default location | Contents and removal considerations |
| --- | --- |
| `/Applications/Vireo.app` | The application. Move it to the Trash to uninstall. |
| `~/.vireo/models` | Downloaded models. Vireo-managed downloads can be fetched again; preserve any custom weights you placed here. |
| `~/.cache/huggingface/hub/models--jss367--vireo-onnx-models` | Cached downloads from Vireo's main model repository. These may remain after removing the installed models or the application. |
| `~/.vireo` | The default catalog, settings, taxonomy downloads, generated files, and other working data. Review and back up this folder before removing it; it is not all disposable cache. |
| `~/Library/Logs/com.vireo.app` | Desktop application logs, if present. These can be removed after quitting. |

`~` means your home folder. The model directory is inside `~/.vireo`; it is not
an additional location to delete after removing that parent folder.

These are defaults, not a complete list for every installation. A custom
catalog, cache directory, or **Work Locally** destination can be elsewhere.
The **Storage** page reports the locations used by the running installation.
Hugging Face environment settings such as `HF_HUB_CACHE`, `HF_HOME`, or
`XDG_CACHE_HOME` can also change the download-cache location.

Other model repositories may have their own `models--…` folders in the shared
Hugging Face cache. Review the Vireo entries in Storage before uninstalling;
do not delete the entire `~/.cache/huggingface` folder as a Vireo cleanup step.
Other applications may use it. Removing a repository's cache can require those
applications to download its files again.

## Before removing all Vireo data

Back up the catalog and settings with Vireo stopped. Saved photo metadata lives
in your photos and XMP sidecars, but a fresh scan is not a backup of every
setting, workspace, pending operation, or editing state in the catalog.

Check `staging`, `offline`, `local-folders`, and `local-workspaces`, including
custom locations, for photo copies that have not reached their intended final
destination. External editor files can contain unique edits. Preserve those
files and confirm that your original photos and sidecars are accessible in
their permanent locations before discarding working data.

After this review, move the unwanted Vireo files to the Trash. Emptying the
Trash makes their removal permanent and reclaims space. Keep your photo-library
folders and XMP sidecars unless you separately intend to delete those photos.

## Windows and Linux

The default data folder is also `.vireo` in your home directory
(`%USERPROFILE%\.vireo` on Windows). Apply the same backup and cleanup steps.
On Windows, uninstall the application through **Settings → Apps → Installed
apps**; the installer preserves the data folder. On Linux, remove the application
using the method used to install it. In both cases, review the locations shown
by Vireo's Storage page before deleting leftover data or shared download caches.
