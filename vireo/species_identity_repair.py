"""Audited, idempotent repair of known legacy common-name enrichment errors."""

import json

from species_identity import COMMON_NAME_CORRECTIONS, SpeciesResolver, resolution_identity

TAXONOMY_RANKS = ("kingdom", "phylum", "class", "order", "family", "genus")
TAXONOMY_COLUMNS = ("scientific_name", *("taxonomy_" + rank for rank in TAXONOMY_RANKS))

# Bump the suffix only when the inferred-taxonomy rule below changes; it is
# deliberately not tied to resolution_identity(), which also keys portable
# caches and pipeline results that this data-only repair does not affect.
INFERRED_TAXONOMY_MARKER = "inferred_taxonomy_repair:v1"


def plan_repairs(conn):
    """Only repair custom BioCLIP labels whose identity has verified evidence.

    Raw labels, confidences, prediction IDs, keywords and all review state are
    preserved. Model-native scientific labels and hybrids are outside this
    repair: a similar common name is not evidence to rewrite their identity.
    """
    changes = []
    for name, evidence in COMMON_NAME_CORRECTIONS.items():
        rows = conn.execute(
            "SELECT id, species, detection_id, classifier_model, labels_fingerprint, scientific_name, "
            "source_taxon_id FROM predictions WHERE species = ? COLLATE NOCASE "
            "AND classifier_model LIKE 'BioCLIP%' AND labels_fingerprint NOT IN ('tol', 'legacy') "
            "AND source_taxon_id IS NULL AND scientific_name IS NOT ?",
            (name, evidence["scientific_name"]),
        ).fetchall()
        for row in rows:
            row = dict(row)
            changes.append({
                **row,
                "before": {"scientific_name": row["scientific_name"], "source_taxon_id": row["source_taxon_id"]},
                "after": {"scientific_name": evidence["scientific_name"], "source_taxon_id": evidence["taxon_id"]},
                "reason": "verified-common-name-correction:" + name,
            })
    return changes


def _taxon_names(conn, scientific_name):
    """Every common name, preferred or alternate, of taxa with this binomial."""
    names = set()
    for taxon in conn.execute(
        "SELECT id, common_name FROM taxa WHERE name = ? COLLATE NOCASE", (scientific_name,),
    ).fetchall():
        if taxon["common_name"]:
            names.add(taxon["common_name"].casefold())
        names.update(row["name"].casefold() for row in conn.execute(
            "SELECT name FROM taxa_common_names WHERE taxon_id = ?", (taxon["id"],),
        ).fetchall())
    return names


def _taxon_lineage(conn, taxon_id):
    """Higher-rank columns for a taxon, walked up the local ``taxa`` table."""
    lineage = {}
    row = conn.execute(
        "SELECT name, rank, parent_id FROM taxa WHERE inat_id = ?", (taxon_id,),
    ).fetchone()
    seen = set()
    while row is not None:
        if row["rank"] in TAXONOMY_RANKS:
            lineage["taxonomy_" + row["rank"]] = row["name"]
        if row["parent_id"] is None or row["parent_id"] in seen:
            break
        seen.add(row["parent_id"])
        row = conn.execute(
            "SELECT name, rank, parent_id FROM taxa WHERE id = ?", (row["parent_id"],),
        ).fetchone()
    return lineage


def plan_inferred_taxonomy_repairs(db):
    """Correct or clear scientific names that old enrichment guessed wrongly.

    Before source-backed labels, custom-label BioCLIP predictions stored a
    scientific name inferred from text, and burst grouping stamped the
    consensus species' taxonomy onto every frame, whatever that frame's own
    label was (fixed in #1165). Rows from that era can carry another species'
    binomial: "Lilac-crowned Amazon" stored as Amazona rhodocorytha (Red-browed
    Amazon), "Allen's Hummingbird" as Selasphorus rufus. Readers that bypass
    ``SpeciesResolver.prediction`` (the iNaturalist taxon default, the Pipeline
    Inspector, metadata search) show or submit that wrong binomial.

    Per row, keyed on the raw label, which is never changed:

    - the resolver verifies a different taxon -> store that taxon;
    - the resolver cannot verify the label, and the stored taxon does not carry
      it as any of its common names (hybrids, a neighbour's binomial) -> clear
      the scientific name and every rank column, since none is evidence;
    - otherwise the stored value is consistent with the label and stays.

    ``source_taxon_id`` stays NULL: a name lookup is not source evidence.
    """
    conn = db.conn
    resolver = SpeciesResolver(db=db)
    rows = conn.execute(
        "SELECT id, species, detection_id, classifier_model, labels_fingerprint, source_taxon_id, "
        + ", ".join(TAXONOMY_COLUMNS) + " FROM predictions "
        "WHERE classifier_model LIKE 'BioCLIP%' AND labels_fingerprint != 'tol' "
        "AND source_taxon_id IS NULL AND scientific_name IS NOT NULL",
    ).fetchall()
    verdicts = {}
    changes = []
    for row in rows:
        row = dict(row)
        key = (row["species"], row["scientific_name"])
        if key not in verdicts:
            verdicts[key] = _inferred_verdict(conn, resolver, *key)
        target, reason = verdicts[key]
        if target is None:
            continue
        before = {column: row[column] for column in TAXONOMY_COLUMNS}
        if before == target:
            continue
        changes.append({**row, "before": before, "after": target, "reason": reason})
    return changes


def _inferred_verdict(conn, resolver, species, stored):
    identity = resolver.display(species)
    if identity.scientific_name:
        if identity.scientific_name.casefold() == stored.casefold():
            return None, None
        target = dict.fromkeys(TAXONOMY_COLUMNS)
        target["scientific_name"] = identity.scientific_name
        if identity.taxon_id:
            target.update(_taxon_lineage(conn, identity.taxon_id))
        return target, "inferred-taxonomy-replaced"
    label = str(species or "").strip().casefold()
    if stored.casefold() == label or label in _taxon_names(conn, stored):
        return None, None
    return dict.fromkeys(TAXONOMY_COLUMNS), "inferred-taxonomy-cleared"


def apply_repairs(conn, changes):
    """Apply the plan atomically, retaining an in-database before/after audit.

    Optimistic row checks reject a plan if predictions changed after preview.
    The caller owns the transaction; no partial commit can escape here.
    """
    conn.execute("""CREATE TABLE IF NOT EXISTS species_identity_repairs (
        id INTEGER PRIMARY KEY, prediction_id INTEGER NOT NULL,
        resolution_identity TEXT NOT NULL, before_json TEXT NOT NULL,
        after_json TEXT NOT NULL, reason TEXT NOT NULL,
        repaired_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""")
    count = 0
    for change in changes:
        before, after = change["before"], change["after"]
        checks = {**before, "source_taxon_id": change["source_taxon_id"], "species": change["species"],
                  "classifier_model": change["classifier_model"],
                  "labels_fingerprint": change["labels_fingerprint"], "detection_id": change["detection_id"]}
        updated = conn.execute(
            "UPDATE predictions SET " + ", ".join(f"{column} = ?" for column in after)
            + " WHERE id = ? AND " + " AND ".join(f"{column} IS ?" for column in checks),
            (*after.values(), change["id"], *checks.values()),
        )
        if updated.rowcount != 1:
            raise ValueError(f"Prediction {change['id']} changed after the repair was planned")
        conn.execute(
            "INSERT INTO species_identity_repairs "
            "(prediction_id, resolution_identity, before_json, after_json, reason) "
            "VALUES (?, ?, ?, ?, ?)",
            (change["id"], resolution_identity(), json.dumps(before), json.dumps(after), change["reason"]),
        )
        # Do not export corrected rows under an old artifact fingerprint.
        conn.execute(
            "UPDATE classifier_runs SET runtime_fingerprint = 'legacy' "
            "WHERE detection_id = ? AND classifier_model = ? AND labels_fingerprint = ?",
            (change["detection_id"], change["classifier_model"], change["labels_fingerprint"]),
        )
        count += 1
    if count:
        conn.execute("UPDATE workspaces SET last_group_fingerprint = NULL")
    return count


def repair_on_upgrade(db):
    count = 0
    marker = "species_identity_repair:" + resolution_identity()
    if db.get_meta(marker) != "1":
        with db.conn:
            count += apply_repairs(db.conn, plan_repairs(db.conn))
            db.set_meta(marker, "1", _commit=False)
    # Planned after the verified corrections commit, so rows they just gave a
    # source taxon are out of scope here. An empty ``taxa`` table is a
    # supported first-run state (the taxonomy download is optional and runs
    # later); without it the resolver verifies nothing and the "clear" branch
    # would wipe every legacy row's binomial and every rank. Defer the whole
    # pass and marker until scientific taxa AND the verified common-name
    # import have completed. A partial scientific-only import is not enough.
    from taxonomy import COMMON_NAME_IDENTITY_VERSION

    if (db.get_meta(INFERRED_TAXONOMY_MARKER) != "1"
            and db.get_meta("common_name_identity_version") == str(COMMON_NAME_IDENTITY_VERSION)
            and _local_taxonomy_populated(db.conn)):
        with db.conn:
            count += apply_repairs(db.conn, plan_inferred_taxonomy_repairs(db))
            db.set_meta(INFERRED_TAXONOMY_MARKER, "1", _commit=False)
    return count


def _local_taxonomy_populated(conn):
    """True once the local ``taxa`` table has rows to verify labels against."""
    row = conn.execute("SELECT 1 FROM taxa LIMIT 1").fetchone()
    return row is not None
