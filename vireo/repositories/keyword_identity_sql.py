"""The data-layer half of keyword identity: SQL fragments and alias lookups.

``keyword_identity`` owns keyword merges and the import-time location checks,
which reach services and sidecars. The pieces the repositories and the
``Database`` façade need live here instead, so the data layer never imports
``keyword_identity``: the species/place identity expression
(``identity_sql``), the import-alias key and lookups (``path_key``,
``resolve_import_alias``, ``resolve_import_path``), and the embedded-keyword
carry used when one photo row merges into another, and the species-claim
and sibling-name checks a keyword merge makes (``taxon_identity``,
``keywords_claim_different_taxa``, ``free_sibling_name``).
"""

import json

from keyword_normalization import keyword_match_key


def identity_sql(alias="k"):
    # A label source's explicit identity wins over a common-name lookup.
    return f"""CASE
        WHEN {alias}.type = 'taxonomy' OR {alias}.is_species = 1 THEN
          CASE WHEN COALESCE({alias}.source_taxon_id,
                    (SELECT inat_id FROM taxa WHERE id = {alias}.taxon_id)) IS NOT NULL
            THEN 'inat:' || COALESCE({alias}.source_taxon_id,
                    (SELECT inat_id FROM taxa WHERE id = {alias}.taxon_id))
            WHEN {alias}.taxon_id IS NOT NULL THEN 'taxon:' || {alias}.taxon_id
            ELSE 'keyword:' || {alias}.id END
        WHEN {alias}.type = 'location' AND {alias}.place_id IS NOT NULL
          THEN 'place:' || {alias}.place_id
        ELSE 'keyword:' || {alias}.id END"""


def path_key(parts):
    return json.dumps([keyword_match_key(p) for p in parts], ensure_ascii=False)


def embedded_keyword_associations_for_merge(conn, losing_id, survivor_id, *,
                                            include_non_embedded=False):
    """Select carried tags while respecting detached embedded values on the survivor.

    Pairing also carries manual/sidecar associations; other embedded merges
    carry only values actually offered by the embedded importer. A loser
    row stamped ``source='manual'`` bypasses the embedded-removal filter so
    a tag the user re-added after the survivor's removal still carries.
    """
    def offered(photo_id):
        return {row["keyword_key"] for row in conn.execute(
            "SELECT keyword_key FROM photo_embedded_keyword_offered WHERE photo_id = ?",
            (photo_id,),
        )}

    source_keys, target_keys = offered(losing_id), offered(survivor_id)
    if not source_keys and not include_non_embedded:
        return
    for row in conn.execute(
            "SELECT k.id, k.name, k.parent_id, pk.source, pk.species_provenance "
            "FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id WHERE pk.photo_id = ?",
            (losing_id,)).fetchall():
        parts, parent_id, seen = [row["name"]], row["parent_id"], {row["id"]}
        while parent_id is not None and parent_id not in seen:
            seen.add(parent_id)
            parent = conn.execute(
                "SELECT name, parent_id FROM keywords WHERE id = ?", (parent_id,),
            ).fetchone()
            if parent is None:
                break
            parts.insert(0, parent["name"])
            parent_id = parent["parent_id"]
        paths = [parts] + [json.loads(alias["path_json"]) for alias in conn.execute(
            "SELECT path_json FROM keyword_import_aliases WHERE keyword_id = ?",
            (row["id"],),
        )]
        matched = set()
        for path in paths:
            full = "|".join(keyword_match_key(part) for part in path)
            leaf = keyword_match_key(path[-1])
            if full in source_keys:
                matched.add(full)
            elif leaf in source_keys:
                matched.add(leaf)
        if not matched and not include_non_embedded:
            continue
        attached = conn.execute(
            "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
            (survivor_id, row["id"]),
        ).fetchone()
        if (matched & target_keys and attached is None
                and row["source"] != "manual"):
            continue
        from repositories.species_provenance import evidence_for_photo_copy
        yield {**dict(row), 'species_provenance': evidence_for_photo_copy(row['species_provenance'], losing_id)}


def resolve_import_alias(db, name, parent_id, *, kw_type=None):
    parts = [name]
    seen = set()
    while parent_id is not None and parent_id not in seen:
        seen.add(parent_id)
        row = db.keyword_merges.name_and_parent(parent_id)
        if row is None:
            return None
        parts.append(row['name'])
        parent_id = row['parent_id']
    return resolve_import_path(db, list(reversed(parts)), kw_type=kw_type)


def resolve_import_path(db, parts, *, kw_type=None, linked_locations_only=False):
    """Resolve reviewed import paths without overriding an explicit type."""
    row = db.keyword_merges.alias_target(path_key(parts), kw_type, linked_locations_only)
    if row:
        return row['keyword_id']
    if len(parts) != 1:
        return None
    # Flat-only metadata cannot distinguish parent paths. Resolve a merged
    # leaf only when every matching alias/live identity agrees on its target.
    name = keyword_match_key(parts[0])
    candidates = {
        row['keyword_id'] for row in db.keyword_merges.alias_paths_of_type(kw_type)
        if keyword_match_key(json.loads(row['path_json'])[-1]) == name
    }
    if not candidates:
        return None
    candidates.update(
        row['id'] for row in db.keyword_merges.ids_and_names_of_type(kw_type)
        if keyword_match_key(row['name']) == name
    )
    if len(candidates) != 1:
        return None
    target_id = candidates.pop()
    if linked_locations_only:
        target = db.keyword_merges.type_and_place(target_id)
        if target['type'] != 'location' or target['place_id'] is None:
            return None
    return target_id


def taxon_identity(db, row):
    """Resolve a keyword row's species identity the way ``identity_sql`` does.

    ``source_taxon_id`` is the label source's own iNat id and outranks the
    local ``taxon_id``; a ``taxon_id`` whose taxon carries an ``inat_id``
    normalizes to the same ``inat:`` form, so a row linked by local id and a
    row linked by iNat id resolve equal when they mean the same species.
    Returns ``None`` for a row that claims no species at all.
    """
    if not (row['is_species'] or row['type'] == 'taxonomy'):
        return None
    inat = row['source_taxon_id']
    if inat is None and row['taxon_id'] is not None:
        found = db.keyword_merges.inat_id_of_taxon(row['taxon_id'])
        inat = found['inat_id'] if found else None
    if inat is not None:
        return f'inat:{inat}'
    if row['taxon_id'] is not None:
        return f'taxon:{row["taxon_id"]}'
    return None


def keywords_claim_different_taxa(db, first, second):
    """Whether two same-named rows stand for two genuinely different species.

    A row that claims no species, or that agrees with the other, is safe to
    merge -- the metadata fold fills whichever side is missing. Two different
    claims are not: the merge keeps the destination's taxon and drops the
    source's, so the migrating row's photos would come out tagged as the
    other species.
    """
    first_id = taxon_identity(db, first)
    second_id = taxon_identity(db, second)
    return first_id is not None and second_id is not None and first_id != second_id


def free_sibling_name(taken, base, suffix):
    """A ``base (suffix)`` name no existing sibling already occupies.

    The disambiguation suffixes are not guaranteed unique on their own -- a
    user can have typed the very name they produce, or renamed a row into it.
    Appending an ordinal until the slot is free keeps the merge from hitting
    ``UNIQUE(name, parent_id)``, which surfaced as an uncaught SQLite error.
    Deterministic so the preview and the write path agree on the result.

    Comparison uses ``keyword_match_key`` so a case-different sibling still
    counts as occupied: SQLite's UNIQUE(name, parent_id) is BINARY, but every
    lookup and dedup path in the app folds case, and picking `Foo (id-3)`
    while `foo (id-3)` already sits under this parent would leave two
    semantic peers no import could tell apart.
    """
    taken_keys = {keyword_match_key(name) for name in taken}
    candidate = f'{base} ({suffix})'
    ordinal = 2
    while keyword_match_key(candidate) in taken_keys:
        candidate = f'{base} ({suffix}) {ordinal}'
        ordinal += 1
    return candidate
