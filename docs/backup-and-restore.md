# Backing up Vireo, restoring it, and moving to a new computer

Vireo automatically backs up its catalog. To recover from a disk failure,
keep a separate copy of both your photo folders and Vireo’s data folder.

- **Your photos** stay in the folders where you put them. Vireo reads them
  there and writes small `.xmp` sidecar files next to them when you sync.
- **The catalog** is what Vireo knows about those photos: workspaces, keywords,
  ratings, flags, species identifications, collections, edits, and history. It
  lives in Vireo's data folder, apart from the photos.

A copy of your photo folders does not include the catalog, and a copy of the
catalog does not include a single photo. You need a backup of both.

For Linux paths, see the [storage locations guide](uninstalling.md#windows-and-linux).

## Find where the catalog lives

Open **Storage** in Vireo and click **Open folder** next to **Catalog and masks**
to find the active catalog (normally `vireo.db`). The default locations are:

| Installation | Data folder | Cache folder |
| --- | --- | --- |
| macOS, fresh installation | `~/Library/Application Support/Vireo` | `~/Library/Caches/Vireo` |
| macOS, legacy layout | `~/.vireo` | Inside `~/.vireo` |
| Windows, fresh installation | `%LOCALAPPDATA%\Vireo` | `%LOCALAPPDATA%\Vireo\Cache` |
| Windows, legacy layout | `%USERPROFILE%\.vireo` | Inside `%USERPROFILE%\.vireo` |

In Finder, press **Shift-Command-G** and paste the path. In Windows File
Explorer, click the address bar, paste the path, and press **Enter**.

If you started Vireo with a custom `--db` path, the catalog is at that path
instead of the folder above, and it may be outside the data folder entirely.
Use the location **Storage** shows under **Catalog and masks**, not the
default path.

The data folder holds more than `vireo.db`:

| Item | What it is |
| --- | --- |
| `vireo.db` | The catalog. |
| `vireo.db-wal`, `vireo.db-shm` | Recent catalog changes that have not been folded into `vireo.db` yet. They appear while Vireo is running. A copy of `vireo.db` without them can be missing your latest work. |
| `config.json` | Your settings, including API tokens you entered. Treat a backup that contains it as private. |
| `masks` | The subject outlines made by Process. Making them again takes hours on a large library. |
| `pipeline_results_ws*.json`, `culling_results_ws*.json` | The encounter, burst, and culling results for each workspace. |
| `labels`, `labels_active.json`, `taxonomy`, `taxonomy.json`, `models.json` | Species lists, species reference data, and the list of models you registered. |
| `staging`, `offline`, `local-folders`, `local-workspaces` | Copies of original photos: imports that have not reached the archive, and photos kept on this computer by **Work Locally**. Some can be the only copy of a photo. |
| `external-edits`, `external-dng`, `edit-masks` | Files sent to and returned from an external editor, and the masks your edits use. |
| `working`, `previews`, `originals` | Images Vireo made from your photos. Vireo can make them again, which takes time. |
| `ssh` | The keys Vireo uses to reach a storage server. |
| `storage-layout-v1` | A marker in fresh installations that tells Vireo to use this folder. Keep it. |

A few things can live outside the data folder. If you started Vireo with a
custom `--thumb-dir`, the managed-media rows above (`staging`, `offline`,
`local-folders`, `local-workspaces`, `external-edits`, `external-dng`,
`edit-masks`) live next to that thumbnail folder instead. Separately, when
you use **Work Locally** you can pick any folder on disk as the destination,
and the local copies then live there regardless of `--thumb-dir`. Either
way, **Storage** lists every persistent location with its full path. Back
up each location **Storage** names, not just the data folder, or the backup
can miss the only copy of a photo. The alternative is to sync or discard
every **Work Locally** copy before you back up, so no photo exists only in
a local folder.

The cache folder holds downloaded models, thumbnails, and saved computation
results. Vireo downloads or rebuilds all of it, so it does not need a backup
— except for custom model weights you registered under the models folder.
Vireo cannot re-download those, so if you keep custom weights there, copy
them with your other files.

Back up the whole data folder. Backing up `vireo.db` alone leaves out your
settings, the masks, and the workspace results.

## Daily and on-demand catalog backups

While Vireo is running, it backs up the catalog about once a day. Open
**Settings → Storage & Maintenance → Catalog backups** to see the dates,
sizes, and location of available copies. Choose **Back up now** to make one
before a large import or edit. **Daily catalog backups kept** sets the total
number of daily and manual copies to keep (default 3, maximum 30). Set it to
0 to turn off daily backups. **Back up now** still keeps the newest copy.

Each dated folder under `backups` beside the active catalog contains a checked
copy of the database, matching workspace results, and available edit-mask
snapshots referenced by the catalog. Photos, global settings, and Process
subject masks still need a separate backup. A disk failure can take both the
catalog and these local copies.

## Snapshots before a catalog upgrade

Before an update changes the catalog format, Vireo saves a complete catalog
file beside the database. For example, `vireo.db.pre-v57.bak` holds the catalog
as it was **before** upgrading to format 57. It needs no `-wal` or `-shm`
file. Vireo keeps snapshots for the three most recent upgrade versions.
Snapshots from a newer Vireo are also preserved when running an older build.

If Vireo cannot write the snapshot, it stops before changing the catalog.
The startup message names the files and explains the failure. Free the
requested space on the catalog drive or fix the folder’s write permissions,
then relaunch Vireo.

To keep upgrade snapshots on another disk, set **Catalog backup folder** to
an existing folder on an external drive or network share. After startup,
Vireo verifies each copy before removing the local file. If the destination
is unavailable, snapshots wait beside the catalog. The daily and manual
backups stay in their own `backups` folder.

Two other leftover files have a similar purpose:

- `config.json.corrupt` is a copy of a settings file Vireo could not read. Vireo
  saved it before going back to default settings.
- `pipeline_results_ws<N>.json.corrupt-<number>` is a workspace results file
  Vireo could not read and moved aside.

## Back up Vireo

1. **Sync to XMP** to write pending keywords, ratings, flags, locations, and
   edits to the sidecars. Workspaces, collections, species review decisions,
   highlights, and history exist only in the catalog.
2. **Back up your photo folders**, including their `.xmp` sidecars, to another
   drive or storage server.
3. **Back up the data folder and other persistent locations listed in Storage.**
   On macOS, check that Time Machine includes those paths. It normally skips
   `~/Library/Caches`, so copy any custom model weights there separately.
   On Windows, quit Vireo, open the active data folder in File Explorer, and
   copy the whole folder and any additional locations to your backup drive.
   Include a custom catalog path if you use one.

On either platform, quit Vireo before copying its data folder by hand so the
catalog and its `-wal` file come from the same moment. For a catalog backup
while Vireo is open, use **Back up now** and wait for it to finish before
copying the completed backup folder to a separate disk.

If you need a single database file while Vireo is running, SQLite’s backup
command also works. For example, on macOS with a legacy catalog:

```bash
sqlite3 ~/.vireo/vireo.db ".backup '/Volumes/Backup/vireo-catalog.db'"
```

Substitute your active catalog path and backup drive.

**Settings → All settings → Export…** downloads your global settings as
`vireo-config-<date>.json`, and **Import…** loads such a file. The export
leaves out API tokens and per-workspace settings. It is useful for copying
settings to another installation, and it does not replace a backup of
`config.json`.

## Restore the data folder from a backup

Use this after a disk failure, or when the data folder is damaged or gone.

1. Quit Vireo.
2. If a data folder is still there, rename it (for example to
   `.vireo-damaged`) and keep it until the restore has worked.
3. Copy the backed-up folder to the same path the data folder had before.
   Restore the whole folder from one backup, so that `vireo.db` and its
   `-wal` file come from the same moment.
4. Start Vireo. Use the version that made the backup or a newer one. An older
   version refuses to open a newer catalog and asks you to update.

Restore any additional persistent locations to their recorded paths before
launching Vireo. On Windows, use the File Explorer address bar to reach the
parent folder (for example, `%LOCALAPPDATA%` for a fresh installation) and
copy the saved Vireo folder back there. If you already launched Vireo and
it created a fresh data folder, quit and move that folder aside first. A
fresh layout with a storage marker takes precedence over a legacy folder.

The restored catalog is as old as the backup. Photos you added since then are
not in it; add their folders again on the **Import** page. Photos you moved or
deleted since then show up as missing. Vireo reports them in a notice; click
**Review** on it to deal with them.

## Restore a daily or manual backup in Vireo

1. Open **Settings → Storage & Maintenance → Catalog backups**. Find the last
   copy made before the change you want to undo and click **Restore…**.
2. Check the date and click **Prepare restore**. Vireo copies and checks the
   backup while you work. Wait for **Restore waiting**. You can still choose
   **Cancel restore** at this point.
3. Quit and reopen Vireo to apply the restore. Check the outcome in **Catalog
   backups**. The restored catalog shows your work as of the selected date.
   Photos imported later are still on disk. Rescan their folders to add them.
4. To undo the restore, use **Restore…** on the **Before restore** copy. It
   includes catalog changes made up to the restart. Vireo keeps the two
   newest such copies separately from daily and manual backups.

## Go back to a `.bak` snapshot

Use this when an update left the catalog in a state you do not want and you
have no better backup.

1. Quit Vireo.
2. In the active catalog folder, rename `vireo.db` to `vireo.db.before-restore`.
3. Move `vireo.db-wal` and `vireo.db-shm` out of the folder if they are there.
   They belong to the catalog you just renamed and must not be combined with
   another one.
4. Move `pipeline_results_ws*.json` and `culling_results_ws*.json` out of the
   folder if they are there. These workspace result files refer to photos by
   catalog id, and the snapshot can hold a different photo under the same id,
   so leaving them in place would show the old result for the wrong image.
   Vireo makes them again the next time Process or Cull runs.
5. Copy the chosen `.bak` into place as `vireo.db` in the active catalog
   folder. Keep the original snapshot. In File Explorer, enable **File name
   extensions** so you can check the restored filename.
6. Start Vireo.

Before you do this, know what it costs:

- Every catalog change since that update is lost: imports, ratings, flags,
  species decisions, and edits. Changes you synced to XMP are still in the
  sidecars.
- The same version of Vireo updates the restored catalog again, in the same
  way. If the update itself caused the problem, restoring the snapshot and
  starting the same version repeats it. Keep the snapshot,
  [report the problem](https://github.com/jss367/vireo/issues), and wait for a
  fix, or open the snapshot with the earlier version of Vireo.
- Masks and settings are not in the snapshot. They stay as they are now.
  Workspace results are not in the snapshot either; step 4 moves them aside
  because they refer to photos by catalog id, and Vireo rebuilds them on the
  next Process or Cull run.

## Move Vireo to a new Mac or Windows PC

On a Mac, Migration Assistant can transfer your user account and Vireo data
together. Reconnect your photo drives and check the catalog as in step 6.
To move by hand on either platform:

1. On the old computer, finish running jobs, sync pending changes to XMP,
   and sync **Work Locally** copies back to their permanent folders. Record
   the paths on the **Storage** page and quit Vireo.
2. Copy the whole data folder and any additional persistent locations to
   an external drive. Bring custom model weights and any photo folders
   stored on the old computer.
3. Connect the photo drives or network shares on the new computer. Match the
   original drive letters or network paths on Windows, or volume names on
   macOS, so the catalog can find the photos.
4. Before opening Vireo, copy the data folder to the corresponding location
   in the table above, keeping the old installation’s fresh or legacy layout.
   Restore other persistent locations to their original paths where possible.
   If Vireo has already created an empty data folder, quit and move it aside
   first.
5. Install the same Vireo version or a newer one. Reinstall external programs
   you use, such as darktable, and update their paths in **Settings** if needed.
6. Open Vireo and verify the catalog path in **Storage**. Check familiar
   photos, ratings, and collections. If Vireo reports missing folders, click
   **Review → Relocate** for each moved parent folder. Its subfolders follow.
   Subject masks also use absolute paths. If the user profile or data path
   changed, you may need to run Process again to rebuild them.
7. Keep the old data folder until you have verified the move. Continue editing
   on one computer because Vireo cannot merge two catalogs that have both changed.
