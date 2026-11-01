# Virtual copies and snapshots

Use virtual copies to keep several independently editable looks for one photo,
such as a tight crop, a wider crop, and a black-and-white treatment.

In **Browse**, right-click a photo and choose **Create Virtual Copy**. In the
photo editor, the **Versions** panel offers the same action. Give the copy a
name or leave the name blank for an automatic name. A copy starts with the
current adjustments, including unsaved editor changes, without changing the
original's saved edit.

The Versions panel shows the original and its copies together with previews.
Click **Edit** to switch versions. Save changes before switching, or confirm
that you want to discard unsaved changes. Each copy has independent geometry,
color, detail, and subject/background adjustments. The underlying file,
species, location, ratings, and analysis remain shared. Browse shows the
number of virtual copies on the original's card; **View Versions** opens the
editor. Copies do not inflate photo or species counts.

**Save Snapshot** records a named checkpoint of the current settings. A
snapshot is fixed: **Restore** saves those settings into the version currently
being edited. Rename or delete copies and snapshots in the Versions panel.
Deleting a copy does not delete the source file. Creating, editing, renaming,
and deleting versions can be undone through Edit History.

Open a copy and choose **Export** to export that version. Vireo saves unsaved
changes first, checks filename collisions, and captures the selected recipe
when starting the export. Metadata settings, including location, work as for
the original. Direct iNaturalist submission uses the original photo; export a
virtual copy first if you want to upload that rendition.

Versions belong to the photo across workspaces. A bookmarked editor URL keeps
the selected copy. If another editor changes a copy, a stale save is rejected
so it cannot silently replace the newer settings.

Version records persist in the catalog and are queued for the normal XMP sync
workflow. Sync writes a Vireo-specific `photoVersions` property; scanning can
recover versions from that property when rebuilding an empty version set.
Other photo applications do not interpret this property. Subject/background
mask snapshots remain in Vireo's managed media, so back up that directory along
with the catalog and sidecars to retain those exact masks.
