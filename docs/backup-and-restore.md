# Backing up Vireo, restoring it, and moving to a new Mac

Vireo works with two separate things, and it backs up neither of them for you.

- **Your photos** stay in the folders where you put them. Vireo reads them
  there and writes small `.xmp` sidecar files next to them when you sync.
- **The catalog** is what Vireo knows about those photos: workspaces, keywords,
  ratings, flags, species identifications, collections, edits, and history. It
  lives in Vireo's data folder, apart from the photos.

A copy of your photo folders does not include the catalog, and a copy of the
catalog does not include a single photo. You need a backup of both.

This page describes macOS. Windows and Linux use the same files in different
folders, listed in the [storage locations guide](uninstalling.md#windows-and-linux).

## Find where the catalog lives

The catalog is a file named `vireo.db` in Vireo's data folder. The data folder
is in one of two places, depending on when you first installed Vireo:

| Installation | Data folder | Cache folder |
| --- | --- | --- |
| An installation that already had `~/.vireo` | `~/.vireo` | Inside `~/.vireo` |
| A fresh installation | `~/Library/Application Support/Vireo` | `~/Library/Caches/Vireo` |

`~` is your home folder. Both data folders are hidden in Finder. To see which
one your installation uses, open **Storage** in Vireo and click **Open folder**
next to **Catalog and masks**. To open a hidden folder yourself, choose
**Go → Go to Folder** in Finder (`Shift-Command-G`) and paste the path.

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
| `staging`, `offline`, `local-folders`, `local-workspaces` | Copies of original photos: imports that have not reached the archive, and photos kept on this Mac by **Work Locally**. Some can be the only copy of a photo. |
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

## Know what the `.bak` files are

A file such as `vireo.db.pre-v57.bak`, next to `vireo.db`, is a snapshot Vireo
took of the catalog before an update changed it.

Some updates change the catalog's format. The first time such a version
starts, it copies the whole catalog to `vireo.db.pre-v<N>.bak` and then makes
its changes. `<N>` is the format number the update moves the catalog to. The
snapshot is a complete catalog in one file, as it was at that moment, and it
needs no `-wal` or `-shm` file.

A snapshot is a way back from a bad update. It is not a backup:

- It sits on the same disk as the catalog, so a failed disk takes both.
- Vireo keeps one. After a later format update succeeds, Vireo deletes the
  snapshots from earlier updates.
- It holds the catalog only. Settings, masks, and photos are not in it.
- It can be months old. Nothing you did after that update is in it.
- If the disk is too full to write the snapshot, Vireo records a warning in
  its log and updates the catalog without one.

A snapshot is about as large as the catalog. **Storage** lists it under
**Catalog backups**. Once you have used the new version for a while and your
catalog looks right, you can delete the file in Finder to get the space back.

Two other leftover files have a similar purpose:

- `config.json.corrupt` is a copy of a settings file Vireo could not read. Vireo
  saved it before going back to default settings.
- `pipeline_results_ws<N>.json.corrupt-<number>` is a workspace results file
  Vireo could not read and moved aside.

## Back up Vireo

1. **Back up your photo folders together with their `.xmp` sidecars**, using
   whatever you already use for files: Time Machine, a second drive, or a
   storage server.
2. **Sync to XMP before you back up.** Syncing writes your keywords, ratings,
   flags, locations, and edits into the sidecars next to your photos, so they
   exist in a second place. Workspaces, collections, species review decisions,
   highlights, and history exist only in the catalog.
3. **Back up the data folder.** Time Machine includes it unless you excluded
   it. Time Machine skips `~/Library/Caches`, which holds nothing you need to
   keep.

To copy the data folder by hand, quit Vireo first and then copy the whole
folder to another disk. A copy made while Vireo is writing can pair a
`vireo.db` with a `vireo.db-wal` from a different moment.

To copy only the catalog while Vireo is running, use SQLite's own backup
command in Terminal. It produces one consistent file:

```bash
sqlite3 ~/.vireo/vireo.db ".backup '/Volumes/Backup/vireo-catalog.db'"
```

Use `~/Library/Application\ Support/Vireo/vireo.db` as the first path if that
is where your catalog lives.

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

Put the folder back before you start Vireo. If Vireo starts while `~/.vireo`
is absent, it sets up a fresh installation in
`~/Library/Application Support/Vireo`. From then on it ignores a `~/.vireo`
you restore later, until you quit Vireo and move that new folder to the Trash.

The restored catalog is as old as the backup. Photos you added since then are
not in it; add their folders again on the **Import** page. Photos you moved or
deleted since then show up as missing. Vireo reports them in a notice; click
**Review** on it to deal with them.

## Go back to a `.bak` snapshot

Use this when an update left the catalog in a state you do not want and you
have no better backup.

1. Quit Vireo.
2. In the data folder, rename `vireo.db` to `vireo.db.before-restore`.
3. Move `vireo.db-wal` and `vireo.db-shm` out of the folder if they are there.
   They belong to the catalog you just renamed and must not be combined with
   another one.
4. Move `pipeline_results_ws*.json` and `culling_results_ws*.json` out of the
   folder if they are there. These workspace result files refer to photos by
   catalog id, and the snapshot can hold a different photo under the same id,
   so leaving them in place would show the old result for the wrong image.
   Vireo makes them again the next time Process or Cull runs.
5. Duplicate the `.bak` file and name the duplicate `vireo.db`. Keep the
   original `.bak` file.
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

## Move Vireo to a new Mac

Apple's Migration Assistant is the least work. When it transfers your user
account, it brings your home folder, which contains Vireo's data folder, and
it keeps your account name, so the paths in the catalog still match. Afterward,
install Vireo if it was not transferred, connect the drives that hold your
photos, and continue at step 6.

To move by hand:

1. **Finish up on the old Mac.** Let running jobs finish. Sync pending changes
   to XMP. If you use **Work Locally**, sync or discard the local copies. Open
   **Storage** and write down the listed locations, then quit Vireo.
2. **Copy the data folder** to an external drive. Copy the whole folder.
3. **Bring your photos to the same paths.** The catalog records each photo
   folder by its full path. An external drive keeps its path
   (`/Volumes/<drive name>`) on any Mac. A storage server keeps its path if you
   mount the same share. Photos inside your home folder keep their path only
   if your account name is the same on both Macs.
4. **Put the data folder at the same path on the new Mac before you open Vireo
   there.** That is `~/.vireo` or `~/Library/Application Support/Vireo`,
   whichever the old Mac used.
5. **Install Vireo.** Use the version the old Mac had or a newer one. An
   older version refuses to open a newer catalog.
6. **Open Vireo and check it.** **Storage** shows the catalog location. If a
   notice says folders can't be found on disk, click **Review** and then
   **Relocate** for each folder that moved; its missing subfolders move with
   it. Vireo downloads a missing model the first time a job needs it, or you
   can download it ahead of time in **Settings → Models**. Vireo rebuilds
   missing thumbnails as you browse.
7. **Keep the old Mac's data folder until the new one looks right.** After
   that, use one of them. Vireo cannot merge two catalogs that have both
   changed.

If you already opened Vireo on the new Mac, it created a fresh, empty data
folder in `~/Library/Application Support/Vireo`. Quit Vireo and move that
folder to the Trash before step 4. While it exists, Vireo uses it and ignores
`~/.vireo`.

When `~/.vireo` is the data folder, the models and thumbnails are inside it
and come along with the copy. A fresh installation keeps them in
`~/Library/Caches/Vireo`. Copying that folder too saves the downloads, and
leaving it behind loses nothing.

Programs Vireo calls, such as darktable or Adobe DNG Converter, are not part
of Vireo. Install them on the new Mac. If one ends up at a different path,
correct the path in **Settings**.

### When the account name is different

If your home folder has a different name on the new Mac, every path that
starts with `/Users/<old name>` stops matching. Photo folders inside your home
folder show up as missing, and you can relocate them as in step 6. The catalog
also records each subject mask by its full path, so Vireo does not find the
masks that were made on the old Mac, and Process has to make them again. Using
the same account name on both Macs avoids both problems.
