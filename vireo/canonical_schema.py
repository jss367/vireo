"""The canonical schema every catalog is opened against.

``CanonicalSchema.create_tables`` creates every table, index, trigger and
view with ``IF NOT EXISTS`` and seeds the saved processes, ending in a single
commit. It builds the current shape directly; schema changes from here on are
numbered migrations in ``schema.py``. The SQL keeps its method indentation,
because the whitespace inside multi-line SQL strings is stored in
``sqlite_master`` (pinned by ``test_db_canonical_schema``).

What deliberately stays on ``Database``: ``_create_tables`` itself, as the
thin wrapper tests monkeypatch and whose ``OperationalError`` failures
``Database.__init__`` turns into ``IncompatibleDatabaseError``, and every
post-schema startup step ``__init__`` runs after it.

This module must not import ``db``: ``schema.py`` imports ``db``, and
``db`` builds this class on every schema setup.
"""

# ``PRAGMA user_version`` of the shape ``create_tables`` builds, stamped on a
# database it creates from nothing. It is the newest registry migration in
# ``schema.MIGRATIONS``: ``create_tables`` builds every migration's end state,
# so a fresh database has nothing to migrate.
SCHEMA_VERSION = 14


class CanonicalSchema:
    def __init__(self, conn):
        self.conn = conn

    def create_tables(self):
        fresh = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' LIMIT 1"
        ).fetchone() is None
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
                camera_make              TEXT,
                camera_model             TEXT,
                lens                     TEXT,
                aperture                 REAL,
                shutter_speed            REAL,
                iso                      INTEGER,
                quality_input_recipe     TEXT,
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
                file_hash TEXT,
                file_mtime REAL,
                needs_sync INTEGER NOT NULL DEFAULT 0
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
                source_taxon_id INTEGER,
                place_id    TEXT,
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

            -- Durable record of embedded keyword values the scanner has
            -- offered to a photo. Vireo never writes into image files, so a
            -- user removal that reaches the pending-changes queue (or the
            -- photo's catalog row directly) can't erase an embedded value;
            -- without this record, a later full scan or image rewrite would
            -- re-read the same embedded value and silently re-tag it.
            -- ``keyword_key`` is the normalized key (``keyword_match_key``),
            -- the same shape used elsewhere for alias comparisons.
            CREATE TABLE IF NOT EXISTS photo_embedded_keyword_offered (
                photo_id    INTEGER REFERENCES photos(id),
                keyword_key TEXT,
                PRIMARY KEY (photo_id, keyword_key)
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
                source_taxon_id      INTEGER,
                -- This row's own raw (pre-softmax) score. NULL means "not
                -- recorded", never "matched badly".
                match_score          REAL,
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
            CREATE INDEX IF NOT EXISTS idx_photo_embedded_keyword_offered_photo
                ON photo_embedded_keyword_offered(photo_id);
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
        # Taxon identities recovered for label lists saved before lists
        # recorded them (see ``label_source_identities.py``), keyed by the
        # label set's fingerprint so they never change that fingerprint and
        # never force a reclassification. The triggers give every prediction
        # row written under such a fingerprint its label's identity, whichever
        # write path produced it (classify, cache materialization, refresh),
        # exactly as a list that carried the identity would have.
        cur.execute("""CREATE TABLE IF NOT EXISTS label_source_identities (
            labels_fingerprint TEXT NOT NULL,
            species            TEXT NOT NULL,
            source_taxon_id    INTEGER NOT NULL,
            scientific_name    TEXT NOT NULL,
            PRIMARY KEY (labels_fingerprint, species)
        )""")
        for trigger, event in (
            ("trg_predictions_label_source_identity_insert", "INSERT"),
            ("trg_predictions_label_source_identity_update", "UPDATE OF source_taxon_id"),
        ):
            cur.execute(f"""CREATE TRIGGER IF NOT EXISTS {trigger}
                AFTER {event} ON predictions
                WHEN NEW.source_taxon_id IS NULL AND EXISTS (
                    SELECT 1 FROM label_source_identities i
                    WHERE i.labels_fingerprint = NEW.labels_fingerprint
                      AND i.species = NEW.species)
                BEGIN
                    UPDATE predictions SET (source_taxon_id, scientific_name) =
                        (SELECT i.source_taxon_id, i.scientific_name
                         FROM label_source_identities i
                         WHERE i.labels_fingerprint = NEW.labels_fingerprint
                           AND i.species = NEW.species)
                    WHERE id = NEW.id;
                END""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_keywords_source_taxon_id ON keywords(source_taxon_id)")
        cur.execute("""CREATE TABLE IF NOT EXISTS keyword_import_aliases (
            path_key TEXT PRIMARY KEY,
            path_json TEXT NOT NULL,
            keyword_id INTEGER NOT NULL REFERENCES keywords(id) ON DELETE CASCADE
        )""")
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_keywords_place_id "
            "ON keywords(place_id) WHERE place_id IS NOT NULL"
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
        self._create_exif_search_text()
        from photo_visibility_schema import create_photo_visibility_schema

        create_photo_visibility_schema(self.conn)
        if fresh:
            self.conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        self.conn.commit()

    def _create_exif_search_text(self):
        """Each photo's searchable EXIF tag values, for metadata search.

        Triggers keep a row current for every ``exif_data`` write; photos
        written before the table existed are filled in by the startup
        backfill (``StartupTasks.kickoff_exif_search_backfill``). The rows
        are SQLite's renderings of the values under the trigger definition,
        so a changed definition or SQLite version invalidates all of them:
        the stamp in ``db_meta`` notices, rebuilds the triggers and empties
        the table for the backfill to refill.
        """
        import hashlib

        from metadata_search import EXIF_SEARCH_TEXT_TABLE, exif_search_text_triggers

        self.conn.execute(f"""CREATE TABLE IF NOT EXISTS {EXIF_SEARCH_TEXT_TABLE} (
            photo_id   INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,
            value_text TEXT NOT NULL
        )""")
        triggers = exif_search_text_triggers()
        (sqlite_version,) = self.conn.execute("SELECT sqlite_version()").fetchone()
        stamp = hashlib.sha256(
            "\n".join([sqlite_version, *(sql for _name, sql in triggers)]).encode()
        ).hexdigest()
        current = self.conn.execute(
            "SELECT value FROM db_meta WHERE key = 'exif_search_text_definition'"
        ).fetchone()
        if current is not None and current[0] == stamp:
            return
        for name, sql in triggers:
            self.conn.execute(f"DROP TRIGGER IF EXISTS {name}")
            self.conn.execute(sql)
        self.conn.execute(f"DELETE FROM {EXIF_SEARCH_TEXT_TABLE}")
        self.conn.execute(
            "INSERT INTO db_meta (key, value) VALUES ('exif_search_text_definition', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (stamp,),
        )
