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
   remove unused ones. **Remove caches for existing model folders** clears old
   cache copies without deleting installed files. A model folder may be incomplete
   or hold another revision, so repair can require another download. Other apps
   sharing that repository cache may also need to download it again.
   Check any cleanup errors or retained-file explanations;
   model removal does not guarantee that every cached file was deleted.
5. Use **Storage → Clear safe caches** for disposable generated files. This
   action does not remove models, originals, or the catalog. Review other
   storage categories separately.

Models are downloaded again when needed. Regenerating image caches requires
access to the source photos. When keeping Vireo, use its cleanup controls rather
than deleting its data folder by hand.

## Default folders and upgrades

Fresh installations use the platform folders below. If `~/.vireo` already
exists on first launch, Vireo continues using it for all defaults, including
models and thumbnails. An empty directory or a symlink also counts as an
existing installation. Files only move when you explicitly run the offline
migration below.

A fresh installation records its choice with `storage-layout-v1` in its data
folder. Keep that file with your catalog and settings: it prevents a later
installation of an older Vireo version from switching the new installation
back to `~/.vireo`. If both layouts exist without that marker, the legacy
layout takes precedence. Explicit catalog, thumbnail, and model paths still
retain their existing behavior.

Models, thumbnails, embedding caches, and computation caches use the cache
folder for new installations. The catalog, settings, labels, taxonomy, SSH
keys, runtime files, and managed media tree use the data folder. That media
tree includes previews alongside working copies, offline originals, pending
imports, and external editor files; do not treat the data folder as disposable.
Custom thumbnail directories keep their existing sibling media layout.

## Opt-in migration of an existing installation

Back up your catalog and settings, finish pending transfers, and quit Vireo and
all separately started servers. Migration preserves all entries in `~/.vireo`,
including unknown files, working copies, offline originals, and external-editor
output. Only models, thumbnails, embeddings, and computation caches move into
the platform cache folder; everything else goes into platform app data.

From a source checkout, preview the exact locations without changing files:

```bash
python scripts/migrate_storage.py
```

Then explicitly apply the plan while Vireo is closed:

```bash
python scripts/migrate_storage.py --apply
```

Desktop installations can use the bundled server without installing Python.
On macOS, the equivalent commands are:

```bash
/Applications/Vireo.app/Contents/MacOS/vireo-server --migrate-storage preview
/Applications/Vireo.app/Contents/MacOS/vireo-server --migrate-storage apply
```

On Windows and Linux, invoke the installed `vireo-server` executable with the
same arguments. The command prints the actual platform/environment destinations.
It refuses an active server or model/cache operation, existing destination
contents, cross-volume moves, custom symlinked cache roots, and relative
symlinks that could change meaning after relocation. Windows requires directory
symlink support (for example, Developer Mode). These unsupported layouts need
manual review; the tool does not merge installations or follow links to move
external photo libraries.

Files move by same-volume rename, preserving their identity and contents without
allocating another model-sized copy. Compatibility links in `~/.vireo` keep old
catalog, configuration, thumbnail, model, and managed-media paths accessible.
**Keep `~/.vireo` after migration**: its links and runtime locks remain part of
the installation. Custom paths outside that folder are unchanged. This migration
does not remove any Hugging Face cache entries; use the Storage controls separately.

A journal at `~/.vireo-storage-migration-v1.json` records unfinished work.
Vireo's desktop launcher and server refuse startup while it exists. After an
interruption, keep Vireo closed and rerun the apply command to resume, or restore
the previous layout with:

```bash
python scripts/migrate_storage.py --rollback
# Desktop equivalent:
/Applications/Vireo.app/Contents/MacOS/vireo-server --migrate-storage rollback
```

Recovery verifies recorded file/directory identities and refuses to overwrite
changed paths. Keep the original environment overrides when resuming. Do not
delete the journal or compatibility links to bypass recovery. Once migration
completes, the journal is removed and Vireo can be restarted; rollback is for
unfinished migrations only. The migration never deletes catalog or photo data.

## Manual cleanup on macOS

Quit Vireo and any separately started Vireo server before moving or removing
files. To open a hidden folder in Finder, choose **Go → Go to Folder**
(`Shift-Command-G`) and paste its path.

| Default location | Contents and removal considerations |
| --- | --- |
| `/Applications/Vireo.app` | The application. Move it to the Trash to uninstall. |
| `~/Library/Caches/Vireo` | New installations: downloaded models, thumbnails, embeddings, and computation caches. Preserve any custom weights you placed here. |
| `~/Library/Application Support/Vireo` | New installations: catalog, settings, and persistent working data. Back up and review before removing. |
| `~/.vireo/models` | Existing installations: downloaded models. Preserve custom weights. |
| `~/.cache/huggingface/hub/models--jss367--vireo-onnx-models` | Cached downloads from Vireo's main model repository. These may remain after removing the installed models or the application. |
| `~/.vireo` | Existing installations: catalog, settings, taxonomy downloads, generated files, and other working data. After opt-in migration this holds compatibility links and locks, which must be kept while using Vireo. Review and back up before removal. |
| `~/Library/Logs/com.vireo.app` | Desktop application logs, if present. These can be removed after quitting. |

`~` means your home folder. Legacy models are inside `~/.vireo`; they are not
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

Fresh Windows installations keep data in `%LOCALAPPDATA%\Vireo` and disposable
caches in `%LOCALAPPDATA%\Vireo\Cache`. If `LOCALAPPDATA` is unset, the base is
`%USERPROFILE%\AppData\Local`. The cache folder is inside the data folder.
Uninstall through **Settings → Apps → Installed apps**; the data is preserved.

Fresh Linux installations keep data in `$XDG_DATA_HOME/Vireo` (default
`~/.local/share/Vireo`) and caches in `$XDG_CACHE_HOME/Vireo` (default
`~/.cache/Vireo`). Environment overrides must be absolute paths; empty or
relative values use the defaults. Remove the application using the method
used to install it.

Existing Windows and Linux installations continue using `.vireo` under the
home directory (`%USERPROFILE%\.vireo` on Windows) until explicitly migrated.
On every platform, apply the
same backup and cleanup steps and review the Storage page's actual locations
before removing data or shared download caches.
