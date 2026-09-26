"""The canonical schema every catalog is opened against.

``CanonicalSchema.create_tables`` is the legacy schema setup that used to be
the body of ``Database._create_tables``: the ``CREATE TABLE IF NOT EXISTS``
script for every table, index, trigger and view, followed by the inline
one-shot column and table migrations, backfills and seeds (``db_meta``- or
``PRAGMA user_version``-guarded) that bring an older catalog up to that
shape, ending in a single commit. The body moved here verbatim and keeps its
method indentation, because the whitespace inside its multi-line SQL strings
is stored in ``sqlite_master``. It is to be split into discrete historical
migrations in ``schema.py`` over time.

What deliberately stays on ``Database``: ``_create_tables`` itself, as the
thin wrapper tests monkeypatch and whose ``OperationalError`` failures
``Database.__init__`` turns into ``IncompatibleDatabaseError``; every
post-schema startup step ``__init__`` runs after it (folder-parent repairs,
default workspace and genre seeds, keyword normalization, species identity
repair); and ``_folder_removal_root_ids``, which the workspace-folder
removal upgrade calls through the bound method it is handed so monkeypatches
of ``Database`` still reach it. ``DEFAULT_TABS`` stays in ``db`` (the web
layer and tests import it from there) and is passed in.

This module must not import ``db``: ``schema.py`` imports ``db``, and
``db`` builds this class on every schema setup.
"""

import json
import sqlite3
import time


class CanonicalSchema:
    def __init__(self, conn, *, folder_removal_root_ids, default_tabs):
        self.conn = conn
        self._folder_removal_root_ids = folder_removal_root_ids
        self.default_tabs = default_tabs

    def create_tables(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS folders (
                id          INTEGER PRIMARY KEY,
                path        TEXT UNIQUE,
                parent_id   INTEGER REFERENCES folders(id),
                name        TEXT,
                photo_count INTEGER DEFAULT 0,
                status      TEXT NOT NULL DEFAULT 'ok'
            );

            CREATE TABLE IF NOT EXISTS photos (
                id                       INTEGER PRIMARY KEY,
                folder_id                INTEGER REFERENCES folders(id),
                filename                 TEXT,
                extension                TEXT,
                file_size                INTEGER,
                file_mtime               REAL,
                xmp_mtime                REAL,
                timestamp                TEXT,
                width                    INTEGER,
                height                   INTEGER,
                rating                   INTEGER DEFAULT 0,
                flag                     TEXT DEFAULT 'none',
                thumb_path               TEXT,
                sharpness                REAL,
                detection_box            TEXT,
                detection_conf           REAL,
                subject_sharpness        REAL,
                subject_size             REAL,
                quality_score            REAL,
                latitude                 REAL,
                longitude                REAL,
                phash                    TEXT,
                mask_path                TEXT,
                dino_subject_embedding   BLOB,
                dino_global_embedding    BLOB,
                subject_tenengrad        REAL,
                bg_tenengrad             REAL,
                crop_complete            REAL,
                bg_separation            REAL,
                subject_clip_high        REAL,
                subject_clip_low         REAL,
                subject_y_median         REAL,
                phash_crop               TEXT,
                noise_estimate           REAL,
                dino_embedding_variant   TEXT,
                active_mask_variant      TEXT,
                focal_length             REAL,
                burst_id                 TEXT,
                file_hash                TEXT,
                companion_path           TEXT,
                exif_data                TEXT,
                working_copy_path        TEXT,
                working_copy_evicted_mtime REAL,
                working_copy_failed_at   TEXT,
                working_copy_failed_mtime REAL,
                working_copy_failed_source TEXT,
                last_move_source_folder_path TEXT,
                eye_x                    REAL,
                eye_y                    REAL,
                eye_conf                 REAL,
                eye_tenengrad            REAL,
                eye_kp_fingerprint       TEXT,
                miss_no_subject          INTEGER,
                miss_clipped             INTEGER,
                miss_oof                 INTEGER,
                miss_computed_at         TEXT,
                wildlife_excluded        INTEGER NOT NULL DEFAULT 0,
                hash_checked_at          TEXT,
                hash_status              TEXT,
                UNIQUE(folder_id, filename)
            );

            CREATE TABLE IF NOT EXISTS taxa (
                id          INTEGER PRIMARY KEY,
                inat_id     INTEGER UNIQUE,
                name        TEXT NOT NULL,
                common_name TEXT,
                rank        TEXT NOT NULL,
                parent_id   INTEGER REFERENCES taxa(id),
                kingdom     TEXT
            );

            CREATE TABLE IF NOT EXISTS companion_identities (
                photo_id INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,
                filename TEXT NOT NULL,
                file_size INTEGER,
                timestamp TEXT,
                file_hash TEXT
            );

            CREATE TABLE IF NOT EXISTS keywords (
                id          INTEGER PRIMARY KEY,
                name        TEXT,
                parent_id   INTEGER REFERENCES keywords(id),
                is_species  INTEGER DEFAULT 0,
                type        TEXT NOT NULL DEFAULT 'general',
                latitude    REAL,
                longitude   REAL,
                taxon_id    INTEGER REFERENCES taxa(id),
                UNIQUE(name, parent_id)
            );

            -- ``source`` is durable provenance for the association itself.
            -- 'manual' means "a person explicitly added this; never treat it
            -- as generated". NULL means unknown (legacy rows, scanner/XMP
            -- imports, model output). Authorship used to be recoverable only
            -- from ``edit_history``, which ``_prune_edit_history`` trims to
            -- ``max_edit_history`` rows — so a hand-added tag could outlive
            -- every trace that a human added it. Provenance belongs on the
            -- row it describes, where nothing prunes it.
            CREATE TABLE IF NOT EXISTS photo_keywords (
                photo_id    INTEGER REFERENCES photos(id),
                keyword_id  INTEGER REFERENCES keywords(id),
                source      TEXT,
                PRIMARY KEY (photo_id, keyword_id)
            );

            -- Singleton key/value table for one-shot migration markers.
            CREATE TABLE IF NOT EXISTS db_meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS workspaces (
                id              INTEGER PRIMARY KEY,
                name            TEXT NOT NULL UNIQUE,
                config_overrides TEXT,
                ui_state        TEXT,
                tabs            TEXT,
                created_at      TEXT DEFAULT (datetime('now')),
                last_opened_at  TEXT,
                pinned_at       TEXT,
                last_grouped_at         INTEGER,
                last_group_fingerprint  TEXT
            );

            CREATE TABLE IF NOT EXISTS workspace_folders (
                workspace_id    INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                folder_id       INTEGER REFERENCES folders(id),
                is_root         INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (workspace_id, folder_id)
            );

            -- Remember removed catalog entries that survive in other
            -- workspaces, so recursive discovery cannot link them back.
            -- These are catalog removals, not filesystem scan exclusions:
            -- explicitly importing a folder again restores its membership.
            CREATE TABLE IF NOT EXISTS workspace_folder_removals (
                workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                folder_id INTEGER REFERENCES folders(id) ON DELETE CASCADE,
                recursive INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (workspace_id, folder_id)
            );
            CREATE INDEX IF NOT EXISTS idx_workspace_folder_removals_folder
                ON workspace_folder_removals(folder_id);
            CREATE TRIGGER IF NOT EXISTS workspace_folder_restore_on_link
            AFTER INSERT ON workspace_folders
            BEGIN
                DELETE FROM workspace_folder_removals
                WHERE workspace_id = NEW.workspace_id AND folder_id = NEW.folder_id;
            END;

            -- Sync-only photo grants. Rows here give ``_resolve_xmp_paths``
            -- a way to find a photo's sidecar for a workspace that owns a
            -- queued edit on the photo but has no ``workspace_folders`` link
            -- to its folder. Tracked-merge collision handling adds a row per
            -- sibling workspace whose ``pending_changes`` were remapped onto
            -- a survivor, so the row can sync without the workspace gaining
            -- library membership on every other photo in that folder.
            --
            -- Keyed by photo, not by folder: the grant authorizes one
            -- photo's sidecar, and ``move_photos`` rewrites
            -- ``photos.folder_id`` without touching anything here -- a
            -- folder-keyed grant would silently stop applying the moment the
            -- active workspace moved the survivor. Resolving the folder at
            -- read time instead means the grant follows the photo.
            --
            -- Read only by ``get_sync_only_photo_paths`` and
            -- ``_photo_syncable_in_workspace``; every browse/library query
            -- stays folder-scoped through ``workspace_folders``.
            CREATE TABLE IF NOT EXISTS workspace_sync_only_photos (
                workspace_id    INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                photo_id        INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                PRIMARY KEY (workspace_id, photo_id)
            );

            CREATE TABLE IF NOT EXISTS local_workspaces (
                workspace_id INTEGER PRIMARY KEY REFERENCES workspaces(id) ON DELETE CASCADE,
                state        TEXT NOT NULL,
                created_at   REAL,
                activated_at REAL
            );

            CREATE TABLE IF NOT EXISTS local_workspace_folders (
                workspace_id    INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                folder_id       INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
                source_path     TEXT NOT NULL,
                local_path      TEXT NOT NULL,
                original_status TEXT NOT NULL DEFAULT 'ok',
                is_root         INTEGER NOT NULL DEFAULT 0,
                root_index      INTEGER,
                PRIMARY KEY (workspace_id, folder_id)
            );

            -- Folder-scoped managed local copies.  A root folder is a
            -- library resource shared by every workspace that references it;
            -- workspace-local status is derived from these rows rather than
            -- owning a second copy of the lifecycle state.
            CREATE TABLE IF NOT EXISTS local_folders (
                root_folder_id INTEGER PRIMARY KEY REFERENCES folders(id) ON DELETE CASCADE,
                state          TEXT NOT NULL,
                created_at     REAL,
                activated_at   REAL
            );

            CREATE TABLE IF NOT EXISTS local_folder_mappings (
                root_folder_id INTEGER NOT NULL REFERENCES local_folders(root_folder_id) ON DELETE CASCADE,
                folder_id      INTEGER NOT NULL UNIQUE REFERENCES folders(id) ON DELETE CASCADE,
                source_path    TEXT NOT NULL,
                local_path     TEXT NOT NULL,
                original_status TEXT NOT NULL DEFAULT 'ok',
                is_root        INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (root_folder_id, folder_id)
            );

            CREATE TABLE IF NOT EXISTS collections (
                id           INTEGER PRIMARY KEY,
                name         TEXT,
                rules        TEXT,
                workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                visual_json  TEXT
            );

            CREATE TABLE IF NOT EXISTS pending_archives (
                id TEXT PRIMARY KEY,
                workspace_id INTEGER NOT NULL,
                collection_id INTEGER,
                destination TEXT NOT NULL,
                staging_destination TEXT NOT NULL,
                target_json TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending',
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TRIGGER IF NOT EXISTS pending_archives_require_workspace
            BEFORE INSERT ON pending_archives
            WHEN NOT EXISTS (SELECT 1 FROM workspaces WHERE id = NEW.workspace_id)
            BEGIN
                SELECT RAISE(ABORT, 'Pending NAS transfer workspace no longer exists');
            END;

            CREATE TRIGGER IF NOT EXISTS pending_archives_protect_workspace
            BEFORE DELETE ON workspaces
            WHEN EXISTS (
                SELECT 1 FROM pending_archives
                WHERE workspace_id = OLD.id AND state != 'complete'
            )
            BEGIN
                SELECT RAISE(ABORT, 'Send pending photos to NAS before deleting this workspace');
            END;

            CREATE TRIGGER IF NOT EXISTS pending_archives_cleanup_workspace
            AFTER DELETE ON workspaces
            BEGIN
                DELETE FROM pending_archives WHERE workspace_id = OLD.id;
            END;

            CREATE TRIGGER IF NOT EXISTS pending_archives_clear_collection
            AFTER DELETE ON collections
            BEGIN
                UPDATE pending_archives SET collection_id = NULL WHERE collection_id = OLD.id;
            END;

            CREATE TABLE IF NOT EXISTS pending_changes (
                id          INTEGER PRIMARY KEY,
                photo_id    INTEGER REFERENCES photos(id) ON DELETE CASCADE,
                change_type TEXT,
                value       TEXT,
                change_token TEXT,
                sync_started INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT DEFAULT (datetime('now')),
                workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS location_gps_reviews (
                photo_id INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,
                fingerprint TEXT NOT NULL,
                reviewed_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS detections (
                id                  INTEGER PRIMARY KEY,
                photo_id            INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                detector_model      TEXT NOT NULL DEFAULT 'megadetector-v6',
                runtime_fingerprint TEXT NOT NULL DEFAULT 'legacy',
                box_x               REAL,
                box_y               REAL,
                box_w               REAL,
                box_h               REAL,
                detector_confidence REAL,
                category            TEXT,
                created_at          TEXT DEFAULT (datetime('now'))
            );

            -- subject_size is declared REAL because compute_all_quality_features
            -- stores it as a fraction in [0, 1]. SQLite's flexible type affinity
            -- means existing databases that pre-date this fix (where the column
            -- was declared INTEGER) still tolerate REAL values without an
            -- ALTER, so we don't bother emitting a migration for the column
            -- type — only fresh DBs see the corrected declaration.
            -- prompt_* are declared REAL because detections.box_* are
            -- normalized values in [0, 1] and any int truncation would
            -- collapse every prompt to (0, 0, 0, 0). SQLite's column
            -- type affinity already accepts REAL into INTEGER-declared
            -- columns, so older DBs created with INTEGER continue to
            -- store the new REAL prompts verbatim — no migration is
            -- needed; legacy rows with prompt_x = 0 will simply be
            -- detected as stale on the next pipeline run.
            CREATE TABLE IF NOT EXISTS photo_masks (
                photo_id          INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                variant           TEXT    NOT NULL,
                path              TEXT    NOT NULL,
                created_at        INTEGER NOT NULL,
                detector_model    TEXT    NOT NULL,
                prompt_x          REAL    NOT NULL,
                prompt_y          REAL    NOT NULL,
                prompt_w          REAL    NOT NULL,
                prompt_h          REAL    NOT NULL,
                subject_size      REAL,
                subject_tenengrad REAL,
                bg_tenengrad      REAL,
                crop_complete     REAL,
                quality_input_recipe TEXT,
                subject_clip_high REAL,
                subject_clip_low REAL,
                subject_y_median REAL,
                bg_separation REAL,
                phash_crop TEXT,
                noise_estimate REAL,
                PRIMARY KEY (photo_id, variant)
            );

            CREATE TABLE IF NOT EXISTS subject_raw_analysis (
                detection_id INTEGER PRIMARY KEY REFERENCES detections(id) ON DELETE CASCADE,
                recipe TEXT NOT NULL,
                report_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS detection_subjects (
                detection_id INTEGER PRIMARY KEY REFERENCES detections(id) ON DELETE CASCADE,
                source_key TEXT NOT NULL,
                crop TEXT NOT NULL,
                quality_score REAL NOT NULL,
                exposure_ev REAL NOT NULL,
                features TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS photo_subject_choices (
                photo_id INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,
                detection_id INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS photo_subject_state (
                photo_id INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,
                detection_id INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS predictions (
                id                   INTEGER PRIMARY KEY,
                detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
                classifier_model     TEXT NOT NULL,
                labels_fingerprint   TEXT NOT NULL DEFAULT 'legacy',
                labels_fingerprint_full TEXT,
                species              TEXT,
                confidence           REAL,
                category             TEXT,
                scientific_name      TEXT,
                taxonomy_kingdom     TEXT,
                taxonomy_phylum     TEXT,
                taxonomy_class       TEXT,
                taxonomy_order       TEXT,
                taxonomy_family      TEXT,
                taxonomy_genus       TEXT,
                created_at           TEXT DEFAULT (datetime('now')),
                UNIQUE(detection_id, classifier_model, labels_fingerprint, species)
            );

            CREATE TABLE IF NOT EXISTS detector_runs (
                photo_id        INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                detector_model  TEXT NOT NULL,
                runtime_fingerprint TEXT NOT NULL DEFAULT 'legacy',
                input_fingerprint TEXT,
                run_at          TEXT DEFAULT (datetime('now')),
                box_count       INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (photo_id, detector_model)
            );

            CREATE TABLE IF NOT EXISTS classifier_runs (
                detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
                classifier_model     TEXT NOT NULL,
                labels_fingerprint   TEXT NOT NULL,
                labels_fingerprint_full TEXT,
                runtime_fingerprint TEXT NOT NULL DEFAULT 'legacy',
                input_recipe TEXT,
                input_fingerprint TEXT,
                run_at               TEXT DEFAULT (datetime('now')),
                prediction_count     INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (detection_id, classifier_model, labels_fingerprint)
            );

            -- Absolute (pre-softmax) match strength for one classifier run.
            --
            -- Deliberately NOT folded into classifier_runs: that table is the
            -- skip-gate for re-classification and is written only when a run
            -- produced at least one prediction, because a zero-count row there
            -- would strand the detection as permanently "done". The run that
            -- matched nothing is exactly the run this table exists to record,
            -- so it keeps its own key and is written unconditionally. It has
            -- no effect on caching.
            --
            -- max_match_score is the best raw score over the WHOLE label list,
            -- including labels that never cleared the prediction threshold.
            -- score_kind names its scale ('cosine' for BioCLIP, 'logit' for a
            -- supervised model) because the two are not comparable and a
            -- threshold calibrated on one is meaningless on the other.
            CREATE TABLE IF NOT EXISTS classifier_match_scores (
                detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
                classifier_model     TEXT NOT NULL,
                labels_fingerprint   TEXT NOT NULL,
                max_match_score      REAL,
                match_margin         REAL,
                top_species          TEXT,
                label_count          INTEGER,
                score_kind           TEXT,
                run_at               TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (detection_id, classifier_model, labels_fingerprint)
            );

            CREATE TABLE IF NOT EXISTS photo_embeddings (
                photo_id    INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                model       TEXT NOT NULL,
                variant     TEXT NOT NULL DEFAULT '',
                embedding   BLOB NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (photo_id, model, variant)
            );

            CREATE TABLE IF NOT EXISTS labels_fingerprints (
                fingerprint    TEXT PRIMARY KEY,
                full_fingerprint TEXT,
                display_name   TEXT,
                sources_json   TEXT,
                label_count    INTEGER,
                created_at     TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS prediction_review (
                prediction_id  INTEGER NOT NULL REFERENCES predictions(id) ON DELETE CASCADE,
                workspace_id   INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                status         TEXT NOT NULL DEFAULT 'pending',
                reviewed_at    TEXT,
                individual     TEXT,
                group_id       TEXT,
                vote_count     INTEGER,
                total_votes    INTEGER,
                PRIMARY KEY (prediction_id, workspace_id)
            );

            CREATE TABLE IF NOT EXISTS inat_submissions (
                id              INTEGER PRIMARY KEY,
                photo_id        INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                observation_id  INTEGER NOT NULL,
                observation_url TEXT NOT NULL,
                submitted_at    TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(photo_id, observation_id)
            );

            CREATE TABLE IF NOT EXISTS edit_history (
                id           INTEGER PRIMARY KEY,
                workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                action_type  TEXT NOT NULL,
                description  TEXT NOT NULL,
                new_value    TEXT,
                is_batch     INTEGER DEFAULT 0,
                undone       INTEGER DEFAULT 0,
                created_at   TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS edit_history_items (
                id        INTEGER PRIMARY KEY,
                edit_id   INTEGER NOT NULL REFERENCES edit_history(id) ON DELETE CASCADE,
                photo_id  INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                old_value TEXT,
                new_value TEXT
            );

            -- Large per-edit blobs (the before/after encounter snapshots a
            -- ``pipeline_grouping`` edit restores from) live here, not in
            -- ``edit_history.new_value``. Those snapshots run to tens of MB
            -- each; kept inline they made every scan of ``edit_history``
            -- (undo status after each flag, the prune inside every
            -- record_edit) walk gigabytes of overflow pages. Loaded only by
            -- an actual undo/redo; deleted with the parent row.
            CREATE TABLE IF NOT EXISTS edit_history_payloads (
                edit_id  INTEGER PRIMARY KEY REFERENCES edit_history(id) ON DELETE CASCADE,
                payload  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS taxa_common_names (
                taxon_id    INTEGER REFERENCES taxa(id) ON DELETE CASCADE,
                name        TEXT NOT NULL,
                locale      TEXT DEFAULT 'en',
                PRIMARY KEY (taxon_id, name)
            );

            CREATE TABLE IF NOT EXISTS informal_groups (
                id          INTEGER PRIMARY KEY,
                name        TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS informal_group_taxa (
                group_id    INTEGER REFERENCES informal_groups(id) ON DELETE CASCADE,
                taxon_id    INTEGER REFERENCES taxa(id) ON DELETE CASCADE,
                PRIMARY KEY (group_id, taxon_id)
            );

            CREATE TABLE IF NOT EXISTS move_rules (
                id          INTEGER PRIMARY KEY,
                name        TEXT NOT NULL,
                destination TEXT NOT NULL,
                criteria    TEXT DEFAULT '{}',
                created_at  TEXT DEFAULT (datetime('now')),
                last_run_at TEXT
            );

            CREATE TABLE IF NOT EXISTS photo_color_labels (
                photo_id      INTEGER REFERENCES photos(id) ON DELETE CASCADE,
                workspace_id  INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
                color         TEXT NOT NULL,
                PRIMARY KEY (photo_id, workspace_id)
            );

            -- Which rejections the duplicate resolver made. ``photos.flag``
            -- alone cannot tell them from a rejection the user made by hand,
            -- and the duplicate scan's auto-reopen may only undo its own.
            -- Any flag change away from 'rejected' drops the row, so a photo
            -- the user un-rejects and later rejects again counts as theirs.
            CREATE TABLE IF NOT EXISTS duplicate_rejections (
                photo_id  INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE
            );
            CREATE TRIGGER IF NOT EXISTS trg_duplicate_rejections_clear
            AFTER UPDATE OF flag ON photos
            WHEN NEW.flag IS NOT 'rejected'
            BEGIN
                DELETE FROM duplicate_rejections WHERE photo_id = NEW.id;
            END;

            CREATE TABLE IF NOT EXISTS photo_edit_recipes (
                photo_id    INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,
                recipe_json TEXT NOT NULL,
                updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS edit_presets (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL UNIQUE,
                recipe_json TEXT NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS photo_preferences (
                workspace_id  INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                purpose       TEXT NOT NULL,
                species       TEXT NOT NULL,
                photo_id      INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                created_at    TEXT DEFAULT (datetime('now')),
                updated_at    TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (workspace_id, purpose, species)
            );

            CREATE TABLE IF NOT EXISTS species_representatives (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                species        TEXT NOT NULL,
                photo_id       INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                selected_order INTEGER NOT NULL,
                created_at     TEXT DEFAULT (datetime('now')),
                updated_at     TEXT DEFAULT (datetime('now')),
                UNIQUE(species, photo_id)
            );

            CREATE TABLE IF NOT EXISTS species_highlights (
                workspace_id  INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                species       TEXT NOT NULL,
                photo_id      INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                rank          INTEGER NOT NULL,
                created_at    TEXT DEFAULT (datetime('now')),
                updated_at    TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (workspace_id, species, photo_id)
            );

            CREATE TABLE IF NOT EXISTS preview_cache (
                photo_id INTEGER NOT NULL,
                size INTEGER NOT NULL,
                bytes INTEGER NOT NULL,
                last_access_at REAL NOT NULL,
                PRIMARY KEY (photo_id, size),
                FOREIGN KEY (photo_id) REFERENCES photos(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS offline_originals (
                photo_id INTEGER NOT NULL PRIMARY KEY,
                original_path TEXT,
                xmp_path TEXT,
                companion_path TEXT,
                bytes INTEGER NOT NULL DEFAULT 0,
                source_size INTEGER,
                source_mtime REAL,
                cached_at REAL NOT NULL,
                status TEXT NOT NULL,
                error TEXT,
                FOREIGN KEY (photo_id) REFERENCES photos(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS new_image_snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
              created_at TEXT NOT NULL,
              file_count INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS new_image_snapshot_files (
              snapshot_id INTEGER NOT NULL REFERENCES new_image_snapshots(id) ON DELETE CASCADE,
              file_path TEXT NOT NULL,
              PRIMARY KEY (snapshot_id, file_path)
            );

            -- Last-run record per audit check (drift, orphans, untracked,
            -- sidecars, integrity). One row per (workspace, check); the
            -- audit page's summary banner reads these so its "archive
            -- intact" light reflects checks that actually ran, with
            -- timestamps, rather than assuming absence of evidence.
            CREATE TABLE IF NOT EXISTS audit_runs (
                workspace_id  INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                check_name    TEXT NOT NULL,
                ran_at        TEXT NOT NULL,
                problem_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (workspace_id, check_name)
            );

            CREATE TABLE IF NOT EXISTS place_reverse_geocode_cache (
                lat_grid    INTEGER NOT NULL,
                lng_grid    INTEGER NOT NULL,
                place_id    TEXT,
                response    TEXT NOT NULL,
                fetched_at  INTEGER NOT NULL,
                PRIMARY KEY (lat_grid, lng_grid)
            );

            -- User-editable "saved processes": named snapshots of the process
            -- page's stage toggles. Global (shared across workspaces); the
            -- per-workspace and app-wide *default* pointers live in config as
            -- ``pipeline.default_process_id`` (an id from this table). Seeded
            -- once from process_strategies.SEED_PROCESSES; see the db_meta
            -- guards in the migration section.
            CREATE TABLE IF NOT EXISTS saved_processes (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                name              TEXT NOT NULL UNIQUE,
                skip_classify     INTEGER NOT NULL DEFAULT 0,
                skip_extract_masks INTEGER NOT NULL DEFAULT 0,
                skip_eye_keypoints INTEGER NOT NULL DEFAULT 0,
                skip_regroup      INTEGER NOT NULL DEFAULT 0,
                miss_enabled      INTEGER NOT NULL DEFAULT 1,
                review_mode       TEXT,
                is_seed           INTEGER NOT NULL DEFAULT 0,
                sort_order        INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_taxa_parent ON taxa(parent_id);
            CREATE INDEX IF NOT EXISTS idx_taxa_rank ON taxa(rank);
            CREATE INDEX IF NOT EXISTS idx_taxa_name ON taxa(name);
            CREATE INDEX IF NOT EXISTS idx_taxa_common ON taxa(common_name);
            -- SpeciesResolver._preferred_common matches a model label or a
            -- keyword name against the preferred name case-insensitively;
            -- idx_taxa_common is BINARY, so without this one every unresolved
            -- name scans the whole 1.3M-row taxa table.
            CREATE INDEX IF NOT EXISTS idx_taxa_common_lower ON taxa(lower(common_name));

            CREATE INDEX IF NOT EXISTS idx_photos_timestamp ON photos(timestamp);
            CREATE INDEX IF NOT EXISTS idx_photos_folder ON photos(folder_id);

            -- Undo/redo status, undo_last_edit, and _prune_edit_history all
            -- filter on (workspace_id, undone) and order by (created_at, id);
            -- the prune's NOT EXISTS and every cascade/retarget look items
            -- up by edit_id.
            CREATE INDEX IF NOT EXISTS idx_edit_history_ws_undone_created
                ON edit_history(workspace_id, undone, created_at, id);
            CREATE INDEX IF NOT EXISTS idx_edit_history_items_edit
                ON edit_history_items(edit_id);
            CREATE INDEX IF NOT EXISTS idx_photos_rating ON photos(rating);
            CREATE INDEX IF NOT EXISTS idx_photos_file_hash ON photos(file_hash);

            CREATE INDEX IF NOT EXISTS idx_keywords_name ON keywords(name);
            CREATE INDEX IF NOT EXISTS idx_keywords_parent_id ON keywords(parent_id);
            CREATE INDEX IF NOT EXISTS idx_keywords_taxon_id ON keywords(taxon_id);
            -- type is low-cardinality (5-value enum) but heavily filtered by
            -- subject rules, classifier skip gates, and migration probes.
            -- Without an index those scan the full keywords table on every
            -- _get_db()-per-request Database instantiation.
            CREATE INDEX IF NOT EXISTS idx_keywords_type ON keywords(type);
            CREATE INDEX IF NOT EXISTS idx_photo_keywords_photo ON photo_keywords(photo_id);
            CREATE INDEX IF NOT EXISTS idx_photo_keywords_keyword ON photo_keywords(keyword_id);
            CREATE INDEX IF NOT EXISTS idx_photo_color_labels_ws
                ON photo_color_labels(workspace_id);
            CREATE INDEX IF NOT EXISTS idx_photo_preferences_photo
                ON photo_preferences(photo_id);
            CREATE INDEX IF NOT EXISTS idx_species_representatives_photo
                ON species_representatives(photo_id);
            CREATE INDEX IF NOT EXISTS idx_species_representatives_order
                ON species_representatives(species, selected_order DESC);
            CREATE INDEX IF NOT EXISTS idx_species_highlights_photo
                ON species_highlights(photo_id);
            CREATE INDEX IF NOT EXISTS idx_species_highlights_rank
                ON species_highlights(workspace_id, species, rank);
            CREATE INDEX IF NOT EXISTS preview_cache_last_access
                ON preview_cache(last_access_at);
            CREATE INDEX IF NOT EXISTS idx_offline_originals_status
                ON offline_originals(status);
            CREATE INDEX IF NOT EXISTS idx_new_image_snapshots_ws
                ON new_image_snapshots(workspace_id);

            CREATE INDEX IF NOT EXISTS idx_detections_photo
                ON detections(photo_id);
            CREATE INDEX IF NOT EXISTS idx_detections_photo_model
                ON detections(photo_id, detector_model);
            CREATE INDEX IF NOT EXISTS idx_detections_conf
                ON detections(photo_id, detector_confidence);
            CREATE INDEX IF NOT EXISTS idx_predictions_detection
                ON predictions(detection_id);
            -- Explicit unique index on the predictions identity tuple. The
            -- CREATE TABLE declares the same UNIQUE, but SQLite's auto-
            -- generated unique index (sqlite_autoindex_*) has NULL `sql` in
            -- sqlite_master, which makes it impossible to assert against in
            -- tests that inspect index SQL. This explicit index gives us a
            -- stable name and a visible CREATE statement.
            CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_identity
                ON predictions(detection_id, classifier_model,
                               labels_fingerprint, species);
            CREATE INDEX IF NOT EXISTS idx_classifier_runs_detection
                ON classifier_runs(detection_id);
            CREATE INDEX IF NOT EXISTS idx_classifier_match_scores_detection
                ON classifier_match_scores(detection_id);
            CREATE INDEX IF NOT EXISTS idx_photo_embeddings_model
                ON photo_embeddings(model, variant);
            CREATE INDEX IF NOT EXISTS idx_prediction_review_workspace
                ON prediction_review(workspace_id);
            CREATE INDEX IF NOT EXISTS idx_collections_workspace
                ON collections(workspace_id);
            CREATE INDEX IF NOT EXISTS idx_pending_workspace
                ON pending_changes(workspace_id);
            -- The tracked-merge collision loop probes and remaps
            -- ``pending_changes`` by ``photo_id`` alone, once per colliding
            -- staged photo. Without this index each probe scans the whole
            -- queue, so the cost grows with the pending backlog times the
            -- collision count.
            CREATE INDEX IF NOT EXISTS idx_pending_photo
                ON pending_changes(photo_id);

            -- Monotonic observation marker shared by folder-health endpoints.
            -- Clients use it to order responses by the SQLite snapshot they
            -- observed rather than by request start or network delivery.
            INSERT OR IGNORE INTO db_meta(key, value)
                VALUES ('folder_health_version', '0');
            CREATE TRIGGER IF NOT EXISTS trg_folder_health_version_insert
            AFTER INSERT ON folders
            BEGIN
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'folder_health_version';
            END;
            CREATE TRIGGER IF NOT EXISTS trg_folder_health_version_delete
            AFTER DELETE ON folders
            BEGIN
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'folder_health_version';
            END;
            CREATE TRIGGER IF NOT EXISTS trg_folder_health_version_status
            AFTER UPDATE OF status ON folders
            WHEN OLD.status IS NOT NEW.status
            BEGIN
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'folder_health_version';
            END;
            CREATE TRIGGER IF NOT EXISTS trg_folder_health_version_ws_insert
            AFTER INSERT ON workspace_folders
            BEGIN
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'folder_health_version';
            END;
            CREATE TRIGGER IF NOT EXISTS trg_folder_health_version_ws_delete
            AFTER DELETE ON workspace_folders
            BEGIN
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'folder_health_version';
            END;

            -- Per-workspace monotonic write counter for `pending_changes`.
            -- The sync-preview cache in app.py keys its snapshot on this
            -- version to detect row replacements — `pending_changes.id` is
            -- a plain INTEGER PRIMARY KEY (no AUTOINCREMENT), so SQLite
            -- will reuse the highest deleted id on the next INSERT. A
            -- cheap COUNT/MAX/SUM aggregate can stay identical across
            -- such a delete+insert even though `change_token`, `value`,
            -- `change_type`, or `photo_id` differ; this counter changes
            -- for every row write and closes that stale-hit window.
            CREATE TRIGGER IF NOT EXISTS trg_pending_changes_version_insert
            AFTER INSERT ON pending_changes
            BEGIN
                INSERT OR IGNORE INTO db_meta(key, value)
                    VALUES ('pending_changes_version:' || NEW.workspace_id, '0');
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'pending_changes_version:' || NEW.workspace_id;
            END;
            CREATE TRIGGER IF NOT EXISTS trg_pending_changes_version_delete
            AFTER DELETE ON pending_changes
            BEGIN
                INSERT OR IGNORE INTO db_meta(key, value)
                    VALUES ('pending_changes_version:' || OLD.workspace_id, '0');
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'pending_changes_version:' || OLD.workspace_id;
            END;
            CREATE TRIGGER IF NOT EXISTS trg_pending_changes_version_update
            AFTER UPDATE ON pending_changes
            BEGIN
                INSERT OR IGNORE INTO db_meta(key, value)
                    VALUES ('pending_changes_version:' || NEW.workspace_id, '0');
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'pending_changes_version:' || NEW.workspace_id;
            END;
            -- When move_folders_to_workspace() reassigns a pending_changes
            -- row from one workspace to another, the update trigger above
            -- only bumps the destination workspace's counter. Without this
            -- second trigger, a cached progressive preview for the source
            -- workspace keeps its fingerprint and continues serving the
            -- rows that have already moved away instead of returning 409.
            CREATE TRIGGER IF NOT EXISTS trg_pending_changes_version_update_source_ws
            AFTER UPDATE OF workspace_id ON pending_changes
            WHEN OLD.workspace_id IS NOT NEW.workspace_id
            BEGIN
                INSERT OR IGNORE INTO db_meta(key, value)
                    VALUES ('pending_changes_version:' || OLD.workspace_id, '0');
                UPDATE db_meta
                SET value = CAST(value AS INTEGER) + 1
                WHERE key = 'pending_changes_version:' || OLD.workspace_id;
            END;
        """
        )
        cur = self.conn.cursor()
        removal_cols = {r[1] for r in cur.execute("PRAGMA table_info(workspace_folder_removals)")}
        if "recursive" not in removal_cols:
            # The old table only recorded exact folder IDs. A single-folder
            # unlink and a subtree removal followed by an explicit child
            # restore can leave identical rows, so recursion cannot safely
            # be inferred from current membership. Preserve the stored
            # exact scope; future tree removals record recursion explicitly.
            cur.execute(
                "ALTER TABLE workspace_folder_removals "
                "ADD COLUMN recursive INTEGER NOT NULL DEFAULT 0"
            )
        scope_version = cur.execute(
            "SELECT value FROM db_meta WHERE key = 'workspace_folder_removal_scope_version'"
        ).fetchone()
        if scope_version is None or scope_version[0] != "1":
            # Upgrade catalogs created by earlier branch builds too: their
            # view scanned the full catalog for every exact removal, and
            # every descendant could carry a redundant recursive record.
            recursive_by_workspace = {}
            for row in cur.execute(
                "SELECT workspace_id, folder_id FROM workspace_folder_removals WHERE recursive = 1"
            ).fetchall():
                recursive_by_workspace.setdefault(row["workspace_id"], set()).add(row["folder_id"])
            for workspace_id, folder_ids in recursive_by_workspace.items():
                redundant = folder_ids - self._folder_removal_root_ids(folder_ids)
                cur.executemany(
                    "UPDATE workspace_folder_removals SET recursive = 0 WHERE workspace_id = ? AND folder_id = ?",
                    [(workspace_id, fid) for fid in redundant],
                )
            cur.execute("DROP VIEW IF EXISTS workspace_removed_folders")
            cur.execute(
                "INSERT OR REPLACE INTO db_meta(key, value) VALUES ('workspace_folder_removal_scope_version', '1')"
            )
        # Share the effective removal scope across passive discovery,
        # membership reads and local-copy preparation. Source paths keep
        # the scope stable while folders are rebased into local storage.
        cur.execute("""CREATE VIEW IF NOT EXISTS workspace_removed_folders AS
            WITH paths AS NOT MATERIALIZED (
                SELECT f.id,
                       RTRIM(REPLACE(f.path, '\\', '/'), '/') AS path,
                       RTRIM(REPLACE(COALESCE(m.source_path, f.path), '\\', '/'), '/') AS source_path
                FROM folders f
                LEFT JOIN local_folder_mappings m ON m.folder_id = f.id
            ), scopes AS NOT MATERIALIZED (
                -- Exact records use primary-key lookups; only recursive
                -- roots need to search the catalog for descendants.
                SELECT workspace_id, folder_id AS root_id, folder_id
                FROM workspace_folder_removals
                UNION ALL
                SELECT removed.workspace_id, root.id, candidate.id
                FROM workspace_folder_removals removed
                JOIN paths root ON root.id = removed.folder_id
                JOIN paths candidate
                  ON substr(candidate.path, 1, length(root.path) + 1) = root.path || '/'
                  OR substr(candidate.source_path, 1, length(root.source_path) + 1) = root.source_path || '/'
                WHERE removed.recursive = 1 AND candidate.id != root.id
            )
            SELECT removed.workspace_id, candidate.id AS folder_id
            FROM scopes removed
            JOIN paths root ON root.id = removed.root_id
            JOIN paths candidate ON candidate.id = removed.folder_id
            WHERE NOT EXISTS (
                SELECT 1 FROM workspace_folders direct
                WHERE direct.workspace_id = removed.workspace_id
                  AND direct.folder_id = candidate.id
            ) AND NOT EXISTS (
                -- An explicitly restored subfolder root may cover new
                -- descendants without restoring its removed ancestors.
                SELECT 1 FROM workspace_folders restored
                JOIN paths restored_path ON restored_path.id = restored.folder_id
                WHERE restored.workspace_id = removed.workspace_id
                  AND restored.is_root = 1
                  AND (substr(restored_path.path, 1, length(root.path) + 1) = root.path || '/'
                       OR substr(restored_path.source_path, 1, length(root.source_path) + 1) = root.source_path || '/')
                  AND (candidate.id = restored.folder_id
                       OR substr(candidate.path, 1, length(restored_path.path) + 1) = restored_path.path || '/'
                       OR substr(candidate.source_path, 1, length(restored_path.source_path) + 1) = restored_path.source_path || '/')
            )
        """)
        pending_cols = {r[1] for r in cur.execute("PRAGMA table_info(pending_changes)")}
        if "sync_started" not in pending_cols:
            cur.execute("ALTER TABLE pending_changes ADD COLUMN sync_started INTEGER NOT NULL DEFAULT 0")
        pred_cols = {r[1] for r in cur.execute("PRAGMA table_info(predictions)")}
        if "source_taxon_id" not in pred_cols:
            cur.execute("ALTER TABLE predictions ADD COLUMN source_taxon_id INTEGER")
        # This row's own raw (pre-softmax) score. NULL on every row written
        # before the column existed, and NULL is the honest value there — it
        # means "not recorded", never "matched badly". Probed by column rather
        # than PRAGMA user_version for the reason normalize_keyword_data()
        # documents: branch builds have already advanced live DBs past the
        # next free version number, so a version-gated migration silently
        # skips on exactly the databases that need it.
        if "match_score" not in pred_cols:
            cur.execute("ALTER TABLE predictions ADD COLUMN match_score REAL")
        cur.execute("PRAGMA table_info(keywords)")
        kw_cols = {row[1] for row in cur.fetchall()}
        if "source_taxon_id" not in kw_cols:
            cur.execute("ALTER TABLE keywords ADD COLUMN source_taxon_id INTEGER")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_keywords_source_taxon_id ON keywords(source_taxon_id)")
        if "place_id" not in kw_cols:
            cur.execute("ALTER TABLE keywords ADD COLUMN place_id TEXT")
        cur.execute("""CREATE TABLE IF NOT EXISTS keyword_import_aliases (
            path_key TEXT PRIMARY KEY,
            path_json TEXT NOT NULL,
            keyword_id INTEGER NOT NULL REFERENCES keywords(id) ON DELETE CASCADE
        )""")
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_keywords_place_id "
            "ON keywords(place_id) WHERE place_id IS NOT NULL"
        )
        # Migration: folders.parent_id. Truly legacy databases predate the
        # column, and CREATE TABLE IF NOT EXISTS above is a no-op for them —
        # so add the column here so repair_missing_folder_parents() (and
        # every other query that reads parent_id) can run.
        try:
            self.conn.execute("SELECT parent_id FROM folders LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE folders "
                "ADD COLUMN parent_id INTEGER REFERENCES folders(id)"
            )
        # Phase 1 storage-philosophy migration: classifier embeddings move
        # from single-slot photos.(embedding, embedding_model) columns into
        # the per-(photo, model, variant) photo_embeddings table. Rows whose
        # embedding_model was never recorded have no key in the new schema
        # and are dropped — they are recomputable from pixels. Truly legacy
        # databases that pre-date embedding_model fall into the same bucket.
        try:
            self.conn.execute("SELECT embedding FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            pass
        else:
            try:
                self.conn.execute("SELECT embedding_model FROM photos LIMIT 0")
                has_embedding_model = True
            except sqlite3.OperationalError:
                has_embedding_model = False
            if has_embedding_model:
                self.conn.execute(
                    """INSERT OR IGNORE INTO photo_embeddings
                           (photo_id, model, variant, embedding)
                       SELECT id, embedding_model, '', embedding
                       FROM photos
                       WHERE embedding IS NOT NULL
                         AND embedding_model IS NOT NULL"""
                )
                self.conn.execute("ALTER TABLE photos DROP COLUMN embedding_model")
            self.conn.execute("ALTER TABLE photos DROP COLUMN embedding")
        # Migration: add `tabs` column. Per the unified-tabs design (2026-04-30),
        # we reset every workspace's tabs to DEFAULT_TABS — solo-user app, no
        # preservation of prior nav_order / open_tabs customizations.
        try:
            self.conn.execute("SELECT tabs FROM workspaces LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE workspaces ADD COLUMN tabs TEXT")
            self.conn.execute(
                "UPDATE workspaces SET tabs = ? WHERE tabs IS NULL",
                (json.dumps(self.default_tabs),),
            )
        # Migration (import/process split PR 3): insert the Import tab
        # before Process ("pipeline") in every saved tabs row that predates
        # the split. One-shot, guarded by PRAGMA user_version so a later
        # unpin isn't silently undone on the next Database.__init__ call
        # (and `_get_db()` opens a fresh Database per request).
        current_user_version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if current_user_version < 1:
            rows = self.conn.execute(
                "SELECT id, tabs FROM workspaces WHERE tabs IS NOT NULL"
            ).fetchall()
            for row in rows:
                try:
                    tabs = json.loads(row["tabs"])
                except (TypeError, ValueError):
                    continue
                if not isinstance(tabs, list) or "import" in tabs:
                    continue
                if "pipeline" in tabs:
                    tabs.insert(tabs.index("pipeline"), "import")
                else:
                    tabs.insert(0, "import")
                self.conn.execute(
                    "UPDATE workspaces SET tabs = ? WHERE id = ?",
                    (json.dumps(tabs), row["id"]),
                )
            self.conn.execute("PRAGMA user_version = 1")
            current_user_version = 1

        # Migration (storage page): cache/storage controls moved out of
        # Settings and Dashboard, so existing workspaces need a visible
        # Storage tab once. Guard with user_version so a later user unpin
        # stays respected across fresh Database handles.
        if current_user_version < 2:
            rows = self.conn.execute(
                "SELECT id, tabs FROM workspaces WHERE tabs IS NOT NULL"
            ).fetchall()
            for row in rows:
                try:
                    tabs = json.loads(row["tabs"])
                except (TypeError, ValueError):
                    continue
                if not isinstance(tabs, list) or "storage" in tabs:
                    continue
                # A legacy table that lacked the tabs column was initialized
                # above with today's compact primary workflow. Do not let this
                # historical migration append a secondary page to that new
                # default; Storage remains available under Tools.
                if tabs == self.default_tabs:
                    continue
                if "settings" in tabs:
                    tabs.insert(tabs.index("settings"), "storage")
                elif "misses" in tabs:
                    tabs.insert(tabs.index("misses") + 1, "storage")
                else:
                    tabs.append("storage")
                self.conn.execute(
                    "UPDATE workspaces SET tabs = ? WHERE id = ?",
                    (json.dumps(tabs), row["id"]),
                )
            self.conn.execute("PRAGMA user_version = 2")
            current_user_version = 2

        # (Version 3 was briefly used on the fix-import-page-routing branch
        # for an "import catch-up" that tried to backfill Import for
        # databases suspected of having skipped the v1 migration. It was
        # dropped before shipping: chronologically v1 (dae1653, 2026-07-05)
        # landed before v2 (e988f21, 2026-07-08) and both live in this same
        # method, so no real database can be at user_version 2 without
        # having run v1. The catch-up therefore only fired on rows whose
        # shape matched a user who unpinned Import from the current
        # default — clobbering a legitimate preference to fix a scenario
        # that cannot occur. The number is skipped rather than reused so
        # any dev DB that briefly reached user_version 3 keeps monotonic
        # ordering into v4.)

        # Migration (import page prominence): Import is now the first pinned
        # page, because adding photos is the natural starting workflow. Move
        # an existing Import tab to the front once. Rows that lack Import
        # are left alone — a one-shot migration must not silently re-add a
        # tab a user removed.
        if current_user_version < 4:
            rows = self.conn.execute(
                "SELECT id, tabs FROM workspaces WHERE tabs IS NOT NULL"
            ).fetchall()
            for row in rows:
                try:
                    tabs = json.loads(row["tabs"])
                except (TypeError, ValueError):
                    continue
                if not isinstance(tabs, list) or "import" not in tabs:
                    continue
                if tabs[0] == "import":
                    continue
                tabs = [t for t in tabs if t != "import"]
                tabs.insert(0, "import")
                self.conn.execute(
                    "UPDATE workspaces SET tabs = ? WHERE id = ?",
                    (json.dumps(tabs), row["id"]),
                )
            self.conn.execute("PRAGMA user_version = 4")
            current_user_version = 4

        # Migration: drop legacy open_tabs column (replaced by `tabs`).
        try:
            self.conn.execute("SELECT open_tabs FROM workspaces LIMIT 0")
            self.conn.execute("ALTER TABLE workspaces DROP COLUMN open_tabs")
        except sqlite3.OperationalError:
            pass  # column already absent (already dropped or fresh schema)
        # Migration: per-workspace grouping provenance. last_grouped_at is
        # the unix epoch when run_full_pipeline last completed for this
        # workspace; last_group_fingerprint is a stable hash of the encounter
        # + burst params used. Both NULL for fresh workspaces.
        try:
            self.conn.execute("SELECT last_grouped_at FROM workspaces LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE workspaces ADD COLUMN last_grouped_at INTEGER"
            )
        try:
            self.conn.execute("SELECT last_group_fingerprint FROM workspaces LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE workspaces ADD COLUMN last_group_fingerprint TEXT"
            )
        # Migration: add `pinned_at` for the alphabetical-with-pinned-on-top
        # workspace dropdown. NULL means unpinned; an ISO timestamp marks the
        # workspace as pinned.
        try:
            self.conn.execute("SELECT pinned_at FROM workspaces LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE workspaces ADD COLUMN pinned_at TEXT")
        # Migration: distinguish user-facing workspace roots from internal
        # descendant links materialized for recursive roots.
        try:
            self.conn.execute("SELECT is_root FROM workspace_folders LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE workspace_folders "
                "ADD COLUMN is_root INTEGER NOT NULL DEFAULT 1"
            )
            self.conn.execute(
                """UPDATE workspace_folders AS child_wf
                   SET is_root = 0
                   WHERE EXISTS (
                     SELECT 1
                     FROM workspace_folders AS root_wf
                     JOIN folders root ON root.id = root_wf.folder_id
                     JOIN folders child ON child.id = child_wf.folder_id
                     WHERE root_wf.workspace_id = child_wf.workspace_id
                       AND root_wf.folder_id != child_wf.folder_id
                       AND substr(
                         REPLACE(child.path, '\\', '/'),
                         1,
                         length(RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/')
                       ) = RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/'
                   )"""
            )
        # Migration: workspace_sync_only_folders -> workspace_sync_only_photos.
        # #1661 briefly recorded these grants keyed by folder; a database
        # opened by that parent commit still carries them, and every reader
        # in this commit prefers the new photo-keyed table. Without this
        # migration the sibling-workspace pending edits that #1661 preserved
        # lose their path grant on upgrade and stay queued as inaccessible
        # with nothing saying why. Rewrite what we can identify: every
        # ``workspace_sync_only_folders`` row was written by
        # ``_link_survivor_for_sibling_edits`` for a specific survivor
        # sitting in that folder at grant time. Match each legacy row to the
        # pending photos that were actually authorized by it -- photos still
        # in the granted folder, and photos that ``move_photos`` later
        # relocated out of it (matched by ``last_move_source_folder_path``,
        # the exact provenance the mover records for this purpose).
        # Restricting the migration this way keeps unrelated pending edits
        # in the same workspace -- for example, an edit for a folder
        # subsequently unlinked from the workspace by
        # ``remove_workspace_folder`` -- from silently gaining sync-only
        # access on upgrade, which was never something the legacy grant
        # authorized. Grants for library-visible photos are inert:
        # ``_photo_syncable_in_workspace`` short-circuits on library
        # membership before consulting the grant.
        #
        # The legacy table stays after this best-effort copy: ``move_photos``
        # clears ``last_move_source_folder_path`` after draining the last
        # same-stem move from a source folder, so a survivor moved before
        # upgrade can match neither its current folder nor its stale
        # provenance and slip past the migration. Retaining the row lets
        # ``_photo_syncable_in_workspace`` and ``get_sync_only_photo_paths``
        # keep resolving the grant at read time -- via the same criteria,
        # so a photo that returns to the granted folder or gets its
        # provenance restamped is still recoverable -- rather than losing
        # the record and the sibling's preserved edit with it. The
        # migration is idempotent (``INSERT OR IGNORE``) so re-running it
        # on subsequent opens fills in whatever the previous run missed.
        legacy_sof = self.conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='workspace_sync_only_folders'"
        ).fetchone()
        if legacy_sof is not None:
            self.conn.execute(
                """INSERT OR IGNORE INTO workspace_sync_only_photos
                       (workspace_id, photo_id)
                   SELECT DISTINCT pc.workspace_id, pc.photo_id
                   FROM pending_changes pc
                   JOIN photos p ON p.id = pc.photo_id
                   JOIN workspace_sync_only_folders sof
                     ON sof.workspace_id = pc.workspace_id
                   LEFT JOIN folders granted
                     ON granted.id = sof.folder_id
                   WHERE sof.folder_id = p.folder_id
                      OR (granted.path IS NOT NULL
                          AND granted.path
                              = p.last_move_source_folder_path)"""
            )
        # Migration: working-copy failure markers. Backfill (and the inline
        # scan extraction) record a failure here when extract_working_copy
        # returns False, gated by file_mtime so a user-replaced file retries
        # on the next pass instead of being permanently skipped.
        try:
            self.conn.execute("SELECT working_copy_failed_at FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN working_copy_failed_at TEXT"
            )
        try:
            self.conn.execute("SELECT working_copy_failed_mtime FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN working_copy_failed_mtime REAL"
            )
        try:
            self.conn.execute("SELECT working_copy_failed_source FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN working_copy_failed_source TEXT"
            )
        # Quota eviction is not an extraction failure: keep its source-mtime
        # marker separate so startup backfill does not immediately recreate
        # deliberately removed working copies.
        try:
            self.conn.execute("SELECT working_copy_evicted_mtime FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN working_copy_evicted_mtime REAL"
            )
        # Record the folder a photo most recently moved from. This lets
        # per-photo moves prove that a same-stem file already at the
        # destination is a RAW/JPEG sibling from the same source instead of
        # an unrelated photo whose developed render would be overwritten.
        # The value is the source folder's path (not its folders.id): SQLite
        # INTEGER PRIMARY KEY without AUTOINCREMENT reuses freed rowids after
        # ``delete_folder``, so a stale id could compare equal to an unrelated
        # new folder and bypass the collision guard.
        try:
            self.conn.execute(
                "SELECT last_move_source_folder_path FROM photos LIMIT 0"
            )
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos "
                "ADD COLUMN last_move_source_folder_path TEXT"
            )
        # Migration: add eye_kp_fingerprint column. Set to NULL for new
        # photos; populated when the eye-keypoint stage runs. Phase 1 also
        # backfills existing eye-keypoint rows to the current fingerprint
        # in a separate migration step (see Task 2.1).
        try:
            self.conn.execute("SELECT eye_kp_fingerprint FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN eye_kp_fingerprint TEXT"
            )
        # One-shot backfill: stamp the current EYE_KP_FINGERPRINT_VERSION
        # onto photos that already have eye-keypoint data, so existing
        # users don't see "Outdated" for unchanged data on first upgrade.
        # Gated by db_meta so it runs exactly once per DB. Probe for
        # eye_tenengrad first — synthetic old-shape DBs in tests can
        # predate that column, in which case there's no eye-keypoint data
        # to backfill anyway and we just record the marker so we don't
        # keep probing.
        marker = self.conn.execute(
            "SELECT value FROM db_meta WHERE key='eye_kp_fingerprint_backfill'"
        ).fetchone()
        if marker is None:
            try:
                self.conn.execute("SELECT eye_tenengrad FROM photos LIMIT 0")
            except sqlite3.OperationalError:
                pass
            else:
                from pipeline import EYE_KP_FINGERPRINT_VERSION
                self.conn.execute(
                    "UPDATE photos SET eye_kp_fingerprint = ? "
                    "WHERE eye_tenengrad IS NOT NULL AND eye_kp_fingerprint IS NULL",
                    (EYE_KP_FINGERPRINT_VERSION,),
                )
            self.conn.execute(
                "INSERT INTO db_meta(key, value) VALUES ('eye_kp_fingerprint_backfill', '1')"
            )
        try:
            self.conn.execute("SELECT active_mask_variant FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN active_mask_variant TEXT"
            )
        try:
            self.conn.execute("SELECT wildlife_excluded FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos "
                "ADD COLUMN wildlife_excluded INTEGER NOT NULL DEFAULT 0"
            )
        # Migration: quality recipe and miss-classifier columns. PHOTO_COLS/get_collection_photos
        # and misses.py both reference these; without the fallback ALTER, any
        # DB created before the miss-classifier feature fails every photo-list
        # query with "no such column".
        for column, column_type in (
            ("quality_input_recipe", "TEXT"),
            ("miss_no_subject", "INTEGER"),
            ("miss_clipped", "INTEGER"),
            ("miss_oof", "INTEGER"),
            ("miss_computed_at", "TEXT"),
        ):
            try:
                self.conn.execute(f"SELECT {column} FROM photos LIMIT 0")
            except sqlite3.OperationalError:
                self.conn.execute(
                    f"ALTER TABLE photos ADD COLUMN {column} {column_type}"
                )
        try:
            self.conn.execute("SELECT quality_input_recipe FROM photo_masks LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE photo_masks ADD COLUMN quality_input_recipe TEXT")
            self.conn.execute(
                "UPDATE photo_masks SET quality_input_recipe = ("
                "SELECT p.quality_input_recipe FROM photos p WHERE p.id=photo_masks.photo_id) "
                "WHERE variant = (SELECT p.active_mask_variant FROM photos p WHERE p.id=photo_masks.photo_id)"
            )
            # Earlier experimental builds recorded only the active recipe.
            # Inactive masks on RAW-analyzed photos have unknown provenance;
            # force a refresh when selected instead of assuming normal scores.
            self.conn.execute(
                "UPDATE photo_masks SET quality_input_recipe='unknown-raw-analysis-recipe' "
                "WHERE variant IS NOT (SELECT p.active_mask_variant FROM photos p WHERE p.id=photo_masks.photo_id) "
                "AND photo_id IN (SELECT d.photo_id FROM subject_raw_analysis a "
                "JOIN detections d ON d.id=a.detection_id)"
            )
        try:
            self.conn.execute("SELECT input_recipe FROM classifier_runs LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE classifier_runs ADD COLUMN input_recipe TEXT")
            # Older experimental runs did not record recipe ownership.
            self.conn.execute(
                "UPDATE classifier_runs SET input_recipe='unknown-raw-recipe' "
                "WHERE detection_id IN (SELECT detection_id FROM subject_raw_analysis)"
            )
        # Quality features belong to the mask/recipe that produced them.
        # Only the active variant can be backfilled from the old photo row.
        for column, column_type in (
            ("subject_clip_high", "REAL"), ("subject_clip_low", "REAL"),
            ("subject_y_median", "REAL"), ("bg_separation", "REAL"),
            ("phash_crop", "TEXT"), ("noise_estimate", "REAL"),
        ):
            try:
                self.conn.execute(f"SELECT {column} FROM photo_masks LIMIT 0")
            except sqlite3.OperationalError:
                self.conn.execute(f"ALTER TABLE photo_masks ADD COLUMN {column} {column_type}")
                self.conn.execute(
                    f"UPDATE photo_masks SET {column}=(SELECT p.{column} FROM photos p "
                    "WHERE p.id=photo_masks.photo_id) WHERE variant=(SELECT p.active_mask_variant "
                    "FROM photos p WHERE p.id=photo_masks.photo_id)"
                )
                self.conn.execute(
                    "UPDATE photo_masks SET quality_input_recipe='unknown-mask-quality-recipe' "
                    "WHERE variant IS NOT (SELECT p.active_mask_variant FROM photos p "
                    "WHERE p.id=photo_masks.photo_id)"
                )
        # Migration: integrity-verification markers. hash_checked_at is when
        # the file's content was last re-hashed against photos.file_hash;
        # hash_status records the verdict ('ok', 'modified', 'corrupt',
        # 'unreadable'). NULL means the file has never been verified.
        try:
            self.conn.execute("SELECT hash_checked_at FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN hash_checked_at TEXT"
            )
        try:
            self.conn.execute("SELECT hash_status FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN hash_status TEXT"
            )

        # Migration: collections carry the universal filter's visual clause
        # alongside rules — the clause deliberately lives outside the rule
        # tree, so without this column a saved expression with a visual
        # component would silently reopen as metadata-only.
        try:
            self.conn.execute("SELECT visual_json FROM collections LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE collections ADD COLUMN visual_json TEXT"
            )

        # Migration: durable keyword-association provenance. Authorship used
        # to be inferred from ``edit_history``, which ``_prune_edit_history``
        # trims to ``max_edit_history`` rows, so evidence that a person added
        # a keyword could disappear while the keyword itself survived — and
        # provenance-driven cleanups would then misread it as generated.
        # 'manual' on the association row cannot be pruned.
        try:
            self.conn.execute("SELECT source FROM photo_keywords LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photo_keywords ADD COLUMN source TEXT"
            )

        # Migration: promote EXIF camera fields out of the exif_data JSON
        # blob into real columns so the universal filter engine can query
        # them with indexes and plain SQL (design:
        # docs/plans/2026-07-19-universal-filters-design.md). Scans populate
        # these for new/changed files; the one-shot backfill below covers
        # existing rows.
        for column, column_type in (
            ("camera_make", "TEXT"),
            ("camera_model", "TEXT"),
            ("lens", "TEXT"),
            ("aperture", "REAL"),
            ("shutter_speed", "REAL"),
            ("iso", "INTEGER"),
        ):
            try:
                self.conn.execute(f"SELECT {column} FROM photos LIMIT 0")
            except sqlite3.OperationalError:
                self.conn.execute(
                    f"ALTER TABLE photos ADD COLUMN {column} {column_type}"
                )
        # One-shot backfill from stored exif_data, gated by db_meta (not
        # user_version, which has drifted on live DBs). Rows whose exif_data
        # is the minimal "{}" marker were scanned with
        # ``extract_full_metadata=False`` before the promoted columns
        # existed — nothing to backfill from the JSON, and the scanner's
        # incremental pre-pass treats any non-NULL ``exif_data`` as
        # "already extracted", so leaving the marker in place would keep
        # camera/lens/iso NULL forever until a user manually forces a full
        # non-incremental scan. Clear those rows back to NULL so the next
        # scan re-runs ExifTool and populates the promoted columns
        # (``scanner._compute_file_features`` writes them whenever
        # ``file_meta`` is present, independent of the full-JSON flag).
        marker = self.conn.execute(
            "SELECT value FROM db_meta WHERE key='exif_summary_backfill_v1'"
        ).fetchone()
        if marker is None:
            from metadata import exif_summary_columns
            # Probe first: synthetic old-shape DBs in tests can predate the
            # exif_data column entirely. Nothing to backfill there — just
            # record the marker so we don't keep probing.
            try:
                self.conn.execute("SELECT exif_data FROM photos LIMIT 0")
            except sqlite3.OperationalError:
                rows = []
                exif_column_present = False
            else:
                rows = self.conn.execute(
                    "SELECT id, exif_data FROM photos "
                    "WHERE exif_data IS NOT NULL AND exif_data != '{}'"
                ).fetchall()
                exif_column_present = True
            for row in rows:
                try:
                    grouped = json.loads(row["exif_data"])
                except (TypeError, ValueError):
                    continue
                cols = exif_summary_columns(grouped)
                if not cols:
                    continue
                assignments = ", ".join(f"{col} = ?" for col in cols)
                self.conn.execute(
                    f"UPDATE photos SET {assignments} WHERE id = ?",
                    [*cols.values(), row["id"]],
                )
            if exif_column_present:
                # Clear the minimal ``'{}'`` marker left by older scans that
                # ran with ``extract_full_metadata=False``. Those rows have
                # no JSON to backfill from, and the scanner's incremental
                # pre-pass otherwise skips them forever (their ``exif_data``
                # is non-NULL, so they're treated as already extracted),
                # leaving the new camera/lens/aperture/... columns
                # permanently empty on upgraded libraries. Clearing to NULL
                # lets the pre-pass's ``summary_needs_extract`` query pick
                # them up on the next scan and populate the promoted
                # columns in a single re-extraction.
                self.conn.execute(
                    "UPDATE photos SET exif_data = NULL WHERE exif_data = '{}'"
                )
            self.conn.execute(
                "INSERT INTO db_meta(key, value) VALUES ('exif_summary_backfill_v1', '1')"
            )

        # Migration: add ON DELETE CASCADE foreign key on
        # local_workspace_folders.folder_id. Early builds of this table
        # declared folder_id as a bare INTEGER, so a folder DELETE on those
        # DBs would leave a dangling local-workspace mapping and break
        # sync/discard's catalog restore. SQLite can't add a FK via ALTER
        # TABLE, so rebuild the table when the constraint is absent.
        fk_rows = self.conn.execute(
            "PRAGMA foreign_key_list(local_workspace_folders)"
        ).fetchall()
        has_folder_fk = any(
            row["from"] == "folder_id" and row["table"] == "folders"
            for row in fk_rows
        )
        if not has_folder_fk:
            # Earlier migrations in this method may have executed DML (for
            # example the db_meta backfill marker above) which sqlite3
            # wraps in an implicit transaction. Toggling foreign_keys and
            # starting BEGIN IMMEDIATE both require no open transaction, so
            # commit any pending migration writes before the rebuild.
            self.conn.commit()
            self.conn.execute("PRAGMA foreign_keys=OFF")
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                self.conn.execute(
                    """CREATE TABLE local_workspace_folders_new (
                        workspace_id    INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                        folder_id       INTEGER NOT NULL REFERENCES folders(id) ON DELETE CASCADE,
                        source_path     TEXT NOT NULL,
                        local_path      TEXT NOT NULL,
                        original_status TEXT NOT NULL DEFAULT 'ok',
                        is_root         INTEGER NOT NULL DEFAULT 0,
                        root_index      INTEGER,
                        PRIMARY KEY (workspace_id, folder_id)
                    )"""
                )
                # Only carry over rows whose folder_id still exists; a
                # concurrent-with-migration folder delete on the old shape
                # is the exact bug this FK closes, and dragging a dangling
                # row into the new table would immediately trip the FK.
                self.conn.execute(
                    """INSERT INTO local_workspace_folders_new
                       SELECT lwf.* FROM local_workspace_folders lwf
                       JOIN folders f ON f.id = lwf.folder_id"""
                )
                self.conn.execute("DROP TABLE local_workspace_folders")
                self.conn.execute(
                    "ALTER TABLE local_workspace_folders_new "
                    "RENAME TO local_workspace_folders"
                )
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise
            finally:
                self.conn.execute("PRAGMA foreign_keys=ON")

        # Backfill pre-existing photos with mask_path set on the photos
        # row but no row in photo_masks. They get migrated to
        # variant='unknown' with a sentinel prompt; detector_model='unknown'
        # + prompt=-1 mean the staleness check will treat these masks as
        # stale on the next pipeline run, so they get regenerated against
        # whatever SAM2 variant the user has configured.
        #
        # Resumable: gating only on the per-photo NOT EXISTS clause means
        # a startup crash partway through (e.g. after inserting some
        # 'unknown' rows but before completing) still finishes the rest
        # of the legacy photos on the next startup. An earlier outer
        # ``if total_unknown_rows == 0`` guard caused remaining photos
        # to be skipped forever, leaving orphaned mask_path values that
        # variant-aware APIs and cleanup logic couldn't see.
        try:
            rows = self.conn.execute(
                "SELECT p.id, p.mask_path, p.subject_size, "
                "p.subject_tenengrad, p.bg_tenengrad, p.crop_complete "
                "FROM photos p "
                "WHERE p.mask_path IS NOT NULL "
                "  AND NOT EXISTS ("
                "    SELECT 1 FROM photo_masks pm WHERE pm.photo_id = p.id"
                "  )"
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []
        now = int(time.time())
        for r in rows:
            self.conn.execute(
                "INSERT OR IGNORE INTO photo_masks "
                "(photo_id, variant, path, created_at, detector_model, "
                "prompt_x, prompt_y, prompt_w, prompt_h, "
                "subject_size, subject_tenengrad, bg_tenengrad, crop_complete) "
                "VALUES (?, 'unknown', ?, ?, 'unknown', -1, -1, -1, -1, ?, ?, ?, ?)",
                (r["id"], r["mask_path"], now,
                 r["subject_size"], r["subject_tenengrad"],
                 r["bg_tenengrad"], r["crop_complete"]),
            )
            self.conn.execute(
                "UPDATE photos SET active_mask_variant='unknown' "
                "WHERE id=? AND active_mask_variant IS NULL",
                (r["id"],),
            )

        # Seed user-editable saved processes once. db_meta-guarded (NOT
        # user_version-guarded) because the live DB's user_version can run
        # ahead of main on parallel branches, which would silently skip a
        # version-gated seed. The marker also means a user who deletes all
        # their processes never has the seeds reappear on the next Database
        # handle. The table itself is created in _create_tables above.
        import process_strategies as ps

        seeded = self.conn.execute(
            "SELECT value FROM db_meta WHERE key='saved_processes_seeded'"
        ).fetchone()
        if seeded is None:
            for order, seed in enumerate(ps.SEED_PROCESSES):
                flags = ps.seed_flags(seed)
                self.conn.execute(
                    "INSERT OR IGNORE INTO saved_processes "
                    "(name, skip_classify, skip_extract_masks, "
                    " skip_eye_keypoints, skip_regroup, miss_enabled, "
                    " review_mode, is_seed, sort_order) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)",
                    (
                        seed["name"],
                        int(flags["skip_classify"]),
                        int(flags["skip_extract_masks"]),
                        int(flags["skip_eye_keypoints"]),
                        int(flags["skip_regroup"]),
                        int(flags["miss_enabled"]),
                        flags["review_mode"],
                        order,
                    ),
                )
            self.conn.execute(
                "INSERT INTO db_meta(key, value) "
                "VALUES ('saved_processes_seeded', '1')"
            )

        # One-shot: migrate the former per-workspace pipeline.default_strategy
        # (a strategy name) to pipeline.default_process_id (a saved_processes
        # id). Unknown/removed names -> unset (import only). Runs after seeding
        # so the name->id lookup finds the seed rows; db_meta-guarded so a
        # later manual edit of the override isn't reverted on the next handle.
        migrated = self.conn.execute(
            "SELECT value FROM db_meta WHERE key='default_strategy_to_process_id'"
        ).fetchone()
        if migrated is None:
            name_to_id = {
                row["name"]: row["id"]
                for row in self.conn.execute(
                    "SELECT id, name FROM saved_processes"
                ).fetchall()
            }
            ws_rows = self.conn.execute(
                "SELECT id, config_overrides FROM workspaces "
                "WHERE config_overrides IS NOT NULL"
            ).fetchall()
            for row in ws_rows:
                try:
                    overrides = json.loads(row["config_overrides"])
                except (TypeError, ValueError):
                    continue
                if not isinstance(overrides, dict):
                    continue
                pipeline_ov = overrides.get("pipeline")
                if not isinstance(pipeline_ov, dict):
                    continue
                if "default_strategy" not in pipeline_ov:
                    continue
                old = pipeline_ov.pop("default_strategy")
                seed_name = (
                    ps.LEGACY_STRATEGY_NAMES.get(old)
                    if isinstance(old, str) else None
                )
                pid = name_to_id.get(seed_name) if seed_name else None
                # Always write ``default_process_id`` (even ``None``) so the
                # workspace's explicit override intent survives the migration.
                # An old ``default_strategy: null`` meant "import only"; without
                # this line, popping the legacy key would let
                # ``get_effective_config()``'s deep_merge inherit the *global*
                # default and silently start auto-processing on imports for a
                # workspace that had explicitly said otherwise. Same reasoning
                # for an unrecognized legacy name — the user's explicit choice
                # was not the current global default.
                pipeline_ov["default_process_id"] = pid
                self.conn.execute(
                    "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
                    (json.dumps(overrides), row["id"]),
                )
            self.conn.execute(
                "INSERT INTO db_meta(key, value) "
                "VALUES ('default_strategy_to_process_id', '1')"
            )

        # One-shot backfill for ``duplicate_rejections``. Before this table
        # existed the duplicate scan's reopen path un-rejected every row
        # under a shared hash. New rejections now record provenance, but a
        # catalog upgraded from before it does not, so groups resolved
        # pre-upgrade would never auto-reopen — if the kept file later
        # disappears, the surviving twin stays rejected behind a ghost
        # winner. Adopt any existing rejection that shares a ``file_hash``
        # with a non-rejected sibling as a resolver rejection so those
        # groups behave the way they used to. A hand-rejection that
        # coincidentally shared a hash gets the same treatment, which
        # matches the pre-upgrade behaviour; new hand-rejections after
        # this point are excluded from ``duplicate_rejections`` normally.
        backfilled = self.conn.execute(
            "SELECT value FROM db_meta WHERE key='duplicate_rejections_backfill_v1'"
        ).fetchone()
        if backfilled is None:
            # Probe for the ``flag`` and ``file_hash`` columns before the
            # backfill runs: synthetic old-shape DBs in tests can predate
            # either. Nothing to backfill there — just record the marker
            # so we don't keep probing on every open.
            try:
                self.conn.execute(
                    "SELECT flag, file_hash FROM photos LIMIT 0"
                )
            except sqlite3.OperationalError:
                pass
            else:
                self.conn.execute(
                    "INSERT OR IGNORE INTO duplicate_rejections(photo_id) "
                    "SELECT p.id FROM photos p "
                    "WHERE p.flag = 'rejected' AND p.file_hash IS NOT NULL "
                    "AND EXISTS ("
                    "    SELECT 1 FROM photos q "
                    "    WHERE q.file_hash = p.file_hash "
                    "      AND q.id != p.id "
                    "      AND (q.flag IS NULL OR q.flag != 'rejected')"
                    ")"
                )
            self.conn.execute(
                "INSERT INTO db_meta(key, value) "
                "VALUES ('duplicate_rejections_backfill_v1', '1')"
            )
        self.conn.commit()
