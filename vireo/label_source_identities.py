"""Taxon identities for label lists saved before lists recorded them.

A regional list downloaded today stores each prompt's iNaturalist taxon
(``label_identities`` in its JSON), and a BioCLIP prediction made from it
carries that taxon as ``source_taxon_id``. Lists saved earlier hold names
only, so their predictions are identified by looking the name up in the
taxonomy, and a name the taxonomy cannot pin to one taxon never resolves:
"Redhead" is also an alternate English name of the Common Pochard. Review
then shows one bird twice, the BioCLIP label under its text and the iNat21
prediction under its taxon, each with its own Accept button.

A name lookup cannot settle it. iNaturalist's first-listed English name is
not always the one people mean ("Terciopelo" is a plant's first name and the
fer-de-lance's alternate), so preferring it would misidentify as many labels
as it fixes. The list's own provenance can: every legacy list records the
place, taxon groups and observation filter it was fetched with, and running
that query again names the taxon behind each prompt it still returns. An
exact, unambiguous name match is the evidence a fresh download records.

Identities are stored against the label set's fingerprint in
``label_source_identities``, never written into the list file. Writing them
there would change the fingerprint (it hashes identities) and mark every
photo classified with the list as needing reclassification, although the
classifier's scores do not depend on identities. Triggers on ``predictions``
(see ``canonical_schema.py``) apply them to rows written later; this module
records them and stamps the rows already there, with a before/after audit
in ``species_identity_repairs``.
"""

import hashlib
import json
import logging
import os

from keyword_normalization import keyword_match_key, normalize_keyword_display
from species_identity_repair import apply_repairs

log = logging.getLogger(__name__)

# One db_meta row per label fingerprint, holding the outcome. Present means
# done: the list was matched (possibly with some names left unidentified),
# so later startups skip it without touching the network.
MARKER_PREFIX = "label_source_identities:"
REPAIR_REASON = "label-list-source-identity"
# Predictions written before label fingerprints existed. Re-run whenever a
# list is newly matched, since its identities can settle more of them.
LEGACY_MARKER = MARKER_PREFIX + "legacy"
LEGACY_REPAIR_REASON = "label-list-consensus-identity"
# The pre-fingerprint mode was not recorded. Only unenriched BioCLIP text
# rows are eligible: a scientific name or rank may be fixed-head ToL output,
# and must never be overwritten from a regional list's consensus.
_UNSTAMPED_LEGACY_WHERE = (
    "labels_fingerprint = 'legacy' AND classifier_model GLOB 'BioCLIP*' AND "
    + " AND ".join(
        f"COALESCE(trim({column}), '') = ''" for column in (
            "scientific_name", "taxonomy_kingdom", "taxonomy_phylum", "taxonomy_class",
            "taxonomy_order", "taxonomy_family", "taxonomy_genus",
        )
    )
)


def _identity(taxon_id, scientific_name):
    return {"source_taxon_id": taxon_id, "scientific_name": scientific_name.strip()}


def _changes(conn, where, params, identities, reason):
    """``apply_repairs`` changes for unstamped rows whose label is identified."""
    changes = []
    for row in conn.execute(
        "SELECT id, species, detection_id, classifier_model, labels_fingerprint, "
        "scientific_name, source_taxon_id FROM predictions "
        "WHERE source_taxon_id IS NULL AND " + where,
        params,
    ).fetchall():
        identity = identities.get(row["species"])
        if identity is not None:
            changes.append({
                **dict(row),
                "new_scientific_name": identity["scientific_name"],
                "new_source_taxon_id": identity["source_taxon_id"],
                "reason": reason,
            })
    return changes


class _BackfillCancelled(RuntimeError):
    pass


def _check_cancelled(cancel_check):
    if cancel_check is not None and cancel_check():
        raise _BackfillCancelled("Label identity backfill cancelled")


def _same_path(a, b):
    return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))


def _source_snapshot(metas):
    """Content digests of source text and identity/query sidecars for one plan."""
    snapshot = []
    for meta in metas:
        path = meta["labels_file"]
        for candidate in (path, os.path.splitext(path)[0] + ".json"):
            try:
                with open(candidate, "rb") as source:
                    digest = hashlib.sha256(source.read()).hexdigest()
            except FileNotFoundError:
                digest = None
            snapshot.append((candidate, digest))
    return snapshot



def _snapshot_metadata(meta):
    """Parse query provenance between matching content snapshots."""
    snapshot = _source_snapshot([meta])
    sidecar = os.path.splitext(meta["labels_file"])[0] + ".json"
    with open(sidecar, encoding="utf-8") as source:
        current = json.load(source)
    if not isinstance(current, dict) or current.get("labels_file") != meta["labels_file"]:
        raise RuntimeError("Label source metadata changed; retry required")
    if _source_snapshot([meta]) != snapshot:
        raise RuntimeError("Label source files changed while reading metadata; retry required")
    return current, snapshot


def pending_label_sets(db, saved_metas=None):
    """Label sets whose predictions lack identities a list query can supply.

    Each entry is ``{"fingerprint", "metas", "labels"}``. A set qualifies
    when its predictions include rows without ``source_taxon_id``, it is not
    marked done, and its source files still rebuild to the same fingerprint.
    A file that was edited or re-downloaded since no longer describes those
    predictions, so the set is skipped rather than matched against it.
    """
    from labels import get_saved_labels, load_merged_labels, read_label_file
    from labels_fingerprint import LEGACY_SENTINEL, TOL_SENTINEL, compute_fingerprint

    if saved_metas is None:
        saved_metas = get_saved_labels()
    pending = []
    for row in db.conn.execute(
        "SELECT fingerprint, sources_json FROM labels_fingerprints "
        "WHERE fingerprint NOT IN (?, ?) ORDER BY fingerprint",
        (TOL_SENTINEL, LEGACY_SENTINEL),
    ).fetchall():
        fingerprint = row["fingerprint"]
        if db.get_meta(MARKER_PREFIX + fingerprint) is not None:
            continue
        if db.conn.execute(
            "SELECT 1 FROM predictions WHERE labels_fingerprint = ? "
            "AND source_taxon_id IS NULL LIMIT 1",
            (fingerprint,),
        ).fetchone() is None:
            continue
        try:
            sources = json.loads(row["sources_json"] or "[]")
        except ValueError:
            continue
        metas = []
        for path in sources:
            meta = next(
                (m for m in saved_metas if m.get("labels_file") and _same_path(m["labels_file"], path)),
                None,
            )
            if meta is None or not os.path.exists(meta["labels_file"]):
                break
            metas.append(meta)
        if not sources or len(metas) != len(sources):
            continue
        # The list as today's merge reads it, or, for one file, as it was
        # read before merging folded case-only duplicates. Either rebuilding
        # the recorded fingerprint proves the file is the one classified.
        try:
            captured = [_snapshot_metadata(meta) for meta in metas]
        except (OSError, ValueError, RuntimeError):
            continue
        metas = [meta for meta, _ in captured]
        source_snapshot = [entry for _, snapshot in captured for entry in snapshot]
        candidates = [load_merged_labels(metas)]
        if len(metas) == 1:
            candidates.append(read_label_file(metas[0]["labels_file"]))
        labels = next((c for c in candidates if compute_fingerprint(c) == fingerprint), None)
        if labels is None:
            log.info(
                "Label set %s no longer matches its source files; "
                "not recovering species IDs for it", fingerprint,
            )
            continue
        if _source_snapshot(metas) != source_snapshot:
            continue
        pending.append({
            "fingerprint": fingerprint, "metas": metas, "labels": labels,
            "source_snapshot": source_snapshot,
        })
    return pending


def _valid_identity(entry):
    return (
        isinstance(entry, dict)
        and not entry.get("ambiguous")
        and type(entry.get("taxon_id")) is int
        and entry["taxon_id"] > 0
        and isinstance(entry.get("scientific_name"), str)
        and entry["scientific_name"].strip()
    )


def source_identities(meta, fetch):
    """``{prompt: identity}`` for one list file, from the list or iNaturalist.

    A list that records identities supplies them. A legacy list is matched
    against its own iNaturalist query: a prompt is identified only when
    exactly one fetched taxon carries that name. A name the fetch had to
    qualify ("Redhead (Aythya americana)") means two taxa in that place share
    it, so the bare legacy prompt stays unidentified. Raises when the list
    cannot be re-queried in full.
    """
    from labels import read_label_file

    names = read_label_file(meta["labels_file"])
    if names.identities:
        return {name: entry for name, entry in names.identities.items() if _valid_identity(entry)}
    if not meta.get("place_id") or not meta.get("taxon_groups"):
        return {}
    fetched = fetch(
        meta["place_id"], meta["taxon_groups"],
        meta.get("observation_filter") or "research", strict=True,
    )
    by_key = {}
    for name, entry in fetched.identities.items():
        if not _valid_identity(entry):
            continue
        key = keyword_match_key(name)
        previous = by_key.get(key)
        if previous is not None and (previous.get("conflict") or previous["taxon_id"] != entry["taxon_id"]):
            by_key[key] = {"conflict": True}
        elif previous is None:
            by_key[key] = entry
    found = {}
    for name in names:
        entry = by_key.get(keyword_match_key(name))
        if entry and not entry.get("conflict"):
            found[name] = entry
    return found


def plan_label_set(label_set, identities_by_file):
    """Identity per stored species spelling for one label set.

    A prompt two source files identify differently is left out. Only the
    taxon and its binomial are recorded: the rank columns stay as stored,
    as everywhere else predictions keep them (see
    ``species_identity.stored_taxonomy_is_evidence``).
    """
    from labels import read_label_file

    file_names = {}
    for meta in label_set["metas"]:
        path = meta["labels_file"]
        folded = {}
        for source_name in read_label_file(path):
            folded.setdefault(keyword_match_key(source_name), []).append(source_name)
        file_names[path] = folded
    planned = {}
    unidentified = []
    for name in label_set["labels"]:
        taxa = {}
        key = keyword_match_key(name)
        vetoed = False
        for path, names in file_names.items():
            for source_name in names.get(key, []):
                entry = identities_by_file.get(path, {}).get(source_name)
                if _valid_identity(entry):
                    taxa[entry["taxon_id"]] = entry
                else:
                    vetoed = True
        species = normalize_keyword_display(name)
        if vetoed or len(taxa) != 1 or not species:
            unidentified.append(name)
            continue
        entry = next(iter(taxa.values()))
        planned[species] = _identity(entry["taxon_id"], entry["scientific_name"])
    return planned, unidentified


def apply_label_set(db, fingerprint, planned, label_count, unidentified=(), *, label_set=None):
    """Record identities for one label set and stamp its existing predictions.

    Returns the number of predictions changed. One writer transaction, so a
    classify run cannot slip a row between the stamp and the trigger taking
    over. Classifier runs are not retired from export as other repairs do:
    the triggers make a fresh run in this catalog write the same identities,
    so the cached output still matches what its fingerprint would produce.
    """
    conn = db.conn
    marker = MARKER_PREFIX + fingerprint
    with conn:
        conn.execute("BEGIN IMMEDIATE")
        if db.get_meta(marker) is not None:
            return 0
        if label_set is not None and _source_snapshot(label_set["metas"]) != label_set["source_snapshot"]:
            raise RuntimeError("Label source files changed during identity lookup; retry required")
        conn.executemany(
            "INSERT OR REPLACE INTO label_source_identities "
            "(labels_fingerprint, species, source_taxon_id, scientific_name) VALUES (?, ?, ?, ?)",
            [
                (fingerprint, species, identity["source_taxon_id"], identity["scientific_name"])
                for species, identity in planned.items()
            ],
        )
        changes = _changes(conn, "labels_fingerprint = ?", (fingerprint,), planned, REPAIR_REASON)
        count = apply_repairs(conn, changes, retire_artifacts=False)
        db.set_meta(marker, json.dumps({
            "labels": label_count,
            "identified": len(planned),
            "predictions_updated": count,
            "unidentified": sorted(set(unidentified)),
        }), _commit=False)
    return count


def legacy_pass_needed(db):
    """Whether pre-fingerprint predictions are waiting for the legacy pass."""
    return db.get_meta(LEGACY_MARKER) is None and db.conn.execute(
        "SELECT 1 FROM predictions WHERE source_taxon_id IS NULL AND "
        + _UNSTAMPED_LEGACY_WHERE + " LIMIT 1",
    ).fetchone() is not None


def consensus_identities(db, saved_metas, *, fetch=None, identities_by_file=None, source_snapshots=None, cancel_check=None):
    """Identity per species spelling that every known list agrees on.

    Predictions older than label fingerprints (``labels_fingerprint =
    'legacy'``) do not record which list produced them, so the per-list match
    cannot apply. The best remaining evidence is the lists themselves: a label
    is identified only when every list on record that holds it names the same
    taxon, and no list holds it unidentified (ambiguous in its place, or gone
    from iNaturalist). This is weaker than the per-list match, because the
    list that made the prediction may have been deleted since, so it is
    audited under its own reason.
    """
    from labels import read_label_file

    votes = {}
    vetoed = set()
    spellings = set()
    if fetch is None:
        from labels import fetch_species_list as fetch
    if identities_by_file is None:
        identities_by_file = {}
    if source_snapshots is None:
        source_snapshots = {}

    def vote(species, taxon_id, identity):
        spellings.add(species)
        votes.setdefault(keyword_match_key(species), {}).setdefault(taxon_id, identity)

    for row in db.conn.execute(
        "SELECT species, source_taxon_id, scientific_name FROM label_source_identities",
    ).fetchall():
        vote(row["species"], row["source_taxon_id"],
             _identity(row["source_taxon_id"], row["scientific_name"]))
    for (value,) in db.conn.execute(
        "SELECT value FROM db_meta WHERE key LIKE ? AND key != ?",
        (MARKER_PREFIX + "%", LEGACY_MARKER),
    ).fetchall():
        try:
            vetoed.update(keyword_match_key(n) for n in json.loads(value).get("unidentified", []))
        except (ValueError, AttributeError):
            continue
    for meta in saved_metas:
        path = meta.get("labels_file")
        if not path or not os.path.exists(path):
            continue
        if path not in source_snapshots:
            meta, source_snapshots[path] = _snapshot_metadata(meta)
        snapshot = source_snapshots[path]
        if _source_snapshot([meta]) != snapshot:
            raise RuntimeError("Label source files changed before legacy consensus; retry required")
        names = read_label_file(path)
        # Include legacy-only lists too: they may never have produced a
        # tracked fingerprint, but still constrain older predictions.
        if path not in identities_by_file:
            try:
                identities_by_file[path] = source_identities(meta, fetch)
            except Exception as exc:
                raise RuntimeError(f"{os.path.basename(path)}: {exc}") from exc
        _check_cancelled(cancel_check)
        if _source_snapshot([meta]) != snapshot:
            raise RuntimeError("Label source files changed during legacy consensus; retry required")
        identities = identities_by_file[path]
        for name in names:
            entry = identities.get(name)
            species = normalize_keyword_display(name)
            if _valid_identity(entry):
                vote(species, entry["taxon_id"],
                     _identity(entry["taxon_id"], entry["scientific_name"]))
            else:
                vetoed.add(keyword_match_key(species))
    spellings.update(row["species"] for row in db.conn.execute(
        "SELECT DISTINCT species FROM predictions WHERE source_taxon_id IS NULL AND "
        + _UNSTAMPED_LEGACY_WHERE,
    ))
    return {
        species: next(iter(votes[key].values()))
        for species in spellings
        if (key := keyword_match_key(species)) not in vetoed and len(votes.get(key, {})) == 1
    }


def apply_legacy(db, saved_metas=None, *, fetch=None, identities_by_file=None, source_snapshots=None, cancel_check=None):
    """Stamp pre-fingerprint text-label predictions from the lists' consensus.

    Returns the number of predictions changed. Model-native rows (iNat21)
    already carry their own scientific identity and are left alone.
    """
    from labels import get_saved_labels

    if saved_metas is None:
        saved_metas = get_saved_labels()
    if source_snapshots is None:
        source_snapshots = {}
    _check_cancelled(cancel_check)
    identities = consensus_identities(
        db, saved_metas, fetch=fetch, identities_by_file=identities_by_file,
        source_snapshots=source_snapshots, cancel_check=cancel_check,
    )
    conn = db.conn
    with conn:
        conn.execute("BEGIN IMMEDIATE")
        for meta in saved_metas:
            snapshot = source_snapshots.get(meta.get("labels_file"))
            if snapshot is not None and _source_snapshot([meta]) != snapshot:
                raise RuntimeError("Label source files changed before legacy commit; retry required")
        _check_cancelled(cancel_check)
        changes = _changes(conn, _UNSTAMPED_LEGACY_WHERE, (), identities, LEGACY_REPAIR_REASON)
        # Nothing ever writes new rows under 'legacy', so these runs are
        # retired from export like any other corrected historical run.
        count = apply_repairs(conn, changes)
        db.set_meta(LEGACY_MARKER, json.dumps({"predictions_updated": count}), _commit=False)
    return count


def backfill(db, fetch=None, progress=None, cancel_check=None):
    """Recover identities for every pending label set. Returns a job result.

    A list that cannot be re-queried (offline, iNaturalist down) is reported
    in ``errors`` and left unmarked, so the next startup tries it again.
    """
    if fetch is None:
        from labels import fetch_species_list as fetch

    pending = pending_label_sets(db)
    result = {
        "label_sets": 0,
        "labels": 0,
        "labels_identified": 0,
        "predictions_updated": 0,
        "unidentified_labels": [],
        "unidentified_labels_total": 0,
        "errors": [],
    }
    identities_by_file = {}
    source_snapshots = {}
    for index, label_set in enumerate(pending):
        if cancel_check is not None and cancel_check():
            result["cancelled"] = True
            break
        if progress is not None:
            names = ", ".join(os.path.basename(m["labels_file"]) for m in label_set["metas"])
            progress(index, len(pending), f"Looking up species IDs for {names}")
        try:
            for meta in label_set["metas"]:
                path = meta["labels_file"]
                if path not in identities_by_file:
                    source_snapshots[path] = [entry for entry in label_set["source_snapshot"]
                                              if entry[0] in (path, os.path.splitext(path)[0] + ".json")]
                    if _source_snapshot([meta]) != source_snapshots[path]:
                        raise RuntimeError("Label source files changed before lookup; retry required")
                    identities_by_file[path] = source_identities(meta, fetch)
                _check_cancelled(cancel_check)
        except _BackfillCancelled:
            result["cancelled"] = True
            break
        except Exception as exc:
            log.warning("Could not re-query label list for %s", label_set["fingerprint"], exc_info=True)
            result["errors"].append(
                f"{os.path.basename(meta['labels_file'])}: {exc}"
            )
            continue
        planned, unidentified = plan_label_set(label_set, identities_by_file)
        try:
            _check_cancelled(cancel_check)
            result["predictions_updated"] += apply_label_set(
                db, label_set["fingerprint"], planned, len(label_set["labels"]), unidentified,
                label_set=label_set,
            )
        except _BackfillCancelled:
            result["cancelled"] = True
            break
        except RuntimeError as exc:
            result["errors"].append(f"{label_set['fingerprint']}: {exc}")
            continue
        result["label_sets"] += 1
        result["labels"] += len(label_set["labels"])
        result["labels_identified"] += len(planned)
        result["unidentified_labels_total"] += len(unidentified)
        room = 20 - len(result["unidentified_labels"])
        result["unidentified_labels"].extend(unidentified[:max(room, 0)])
    if not result.get("cancelled") and not result["errors"] and (result["label_sets"] or legacy_pass_needed(db)):
        if progress is not None:
            progress(len(pending), len(pending), "Matching older predictions across label lists")
        try:
            result["legacy_predictions_updated"] = apply_legacy(
                db, fetch=fetch, identities_by_file=identities_by_file,
                source_snapshots=source_snapshots, cancel_check=cancel_check,
            )
        except _BackfillCancelled:
            result["cancelled"] = True
        except Exception as exc:
            log.warning("Could not complete legacy label-list consensus", exc_info=True)
            result["errors"].append(f"Legacy label-list consensus: {exc}")
        else:
            result["predictions_updated"] += result["legacy_predictions_updated"]
    # A list that could not be re-queried leaves its species split, so the
    # run failed even though every other list was handled.
    result["ok"] = not result["errors"]
    if progress is not None:
        progress(len(pending), len(pending), "Done")
    log.info(
        "Label list species IDs: %d of %d labels identified across %d label sets; "
        "%d predictions updated",
        result["labels_identified"], result["labels"], result["label_sets"],
        result["predictions_updated"],
    )
    return result
