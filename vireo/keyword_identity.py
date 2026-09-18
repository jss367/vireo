"""Keyword identities and explicit reconciliation of imported place names.

Hierarchy nodes remain distinct records. Taxon/place identity groups them for
display; only a user-confirmed location reconciliation moves associations.
"""

import json
from collections import defaultdict

from keyword_normalization import keyword_match_key
from xmp import location_keyword_entries, read_vireo_location_keywords


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


def keyword_paths(rows):
    by_id = {r['id']: r for r in rows}
    paths = {}
    for row in rows:
        parts, seen = [], set()
        current = row
        while current and current['id'] not in seen:
            seen.add(current['id'])
            parts.append(current['name'])
            current = by_id.get(current['parent_id'])
        paths[row['id']] = list(reversed(parts))
    return paths


def path_key(parts):
    return json.dumps([keyword_match_key(p) for p in parts], ensure_ascii=False)


def resolve_import_alias(db, name, parent_id):
    parts = [name]
    seen = set()
    while parent_id is not None and parent_id not in seen:
        seen.add(parent_id)
        row = db.conn.execute(
            'SELECT name, parent_id FROM keywords WHERE id = ?', (parent_id,),
        ).fetchone()
        if row is None:
            return None
        parts.append(row['name'])
        parent_id = row['parent_id']
    return resolve_import_path(db, list(reversed(parts)))


def resolve_import_path(db, parts):
    row = db.conn.execute(
        "SELECT a.keyword_id FROM keyword_import_aliases a "
        "JOIN keywords k ON k.id = a.keyword_id "
        "WHERE a.path_key = ? AND k.type = 'location' AND k.place_id IS NOT NULL",
        (path_key(parts),),
    ).fetchone()
    return row['keyword_id'] if row else None


def drop_stale_vireo_location_keywords(db, photo_id, xmp_path,
                                       flat_keywords, hierarchical_keywords):
    """Filter out location keywords Vireo wrote that the DB has since changed.

    Location keywords are the one kind Vireo owns end to end: the user assigns
    a place in Vireo and the sidecar receives a copy. While a ``location``
    change is queued the sidecar's copy is by definition out of date -- the
    user has already picked a different place, or none -- so importing it
    would re-attach the place they just moved away from and leave the photo
    carrying two locations until someone noticed.

    Only the two entries the last write recorded in its sidecar marker are
    dropped, so a location keyword the user typed in Lightroom is imported
    exactly as before. Returns ``(flat_set, hierarchical_list)``.
    """
    flat, hierarchical = set(flat_keywords), list(hierarchical_keywords)
    if not db.has_pending_location_change(photo_id):
        return flat, hierarchical
    leaf, path = location_keyword_entries(read_vireo_location_keywords(xmp_path))
    if not path:
        return flat, hierarchical

    leaf_key = keyword_match_key(leaf)
    path_keys = [keyword_match_key(part) for part in path.split('|')]
    return (
        {name for name in flat
         if not leaf_key or keyword_match_key(name) != leaf_key},
        [entry for entry in hierarchical
         if [keyword_match_key(part) for part in entry.split('|')] != path_keys],
    )


def validate_import_locations(db, photo_id, flat_keywords, hierarchical_keywords, *, additive=True):
    """Reject conflicting confirmed locations before an import changes tags.

    Scans/catalog imports retain existing tags; sync retains existing tags
    only when they are still named in the sidecar. Ancestor places in the
    same location chain are compatible with its more specific leaf.
    """
    targets, hierarchy_leaves = set(), set()
    for hierarchy in hierarchical_keywords:
        parts = hierarchy.split('|')
        if any(not keyword_match_key(part) for part in parts):
            continue
        hierarchy_leaves.add(keyword_match_key(parts[-1]))
        target = resolve_import_path(db, parts)
        if target is not None:
            targets.add(target)
    flat_keys = {keyword_match_key(name) for name in flat_keywords}
    for name in flat_keywords:
        if keyword_match_key(name) not in hierarchy_leaves:
            target = resolve_import_path(db, [name])
            if target is not None:
                targets.add(target)
    if not targets:
        return
    for row in db.conn.execute(
        "SELECT k.id, k.name FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location' AND k.place_id IS NOT NULL", (photo_id,),
    ):
        if additive or keyword_match_key(row['name']) in flat_keys:
            targets.add(row['id'])
    ancestors = set()
    for target in targets:
        seen = {target}
        while target is not None:
            row = db.conn.execute('SELECT parent_id FROM keywords WHERE id = ?', (target,)).fetchone()
            target = row['parent_id'] if row else None
            if target is None or target in seen:
                break
            seen.add(target)
            ancestors.add(target)
    if len(targets - ancestors) > 1:
        raise ValueError('Imported keywords resolve to different linked places; choose one location before importing.')


def grouped_keywords(db):
    rows = [dict(r) for r in db.get_all_keywords()]
    identities = {r['id']: r['identity'] for r in db.conn.execute(
        f'SELECT k.id, {identity_sql()} AS identity FROM keywords k'
    )}
    paths = keyword_paths(rows)
    groups = defaultdict(list)
    for row in rows:
        row['path'] = paths[row['id']]
        groups[identities[row['id']]].append(row)

    # Count the union of each member's subtree, preserving the management
    # page's existing parent counts without summing overlapping photos.
    counts = {r['identity']: r for r in db.conn.execute(
        f"""WITH RECURSIVE descendants(identity, id) AS (
            SELECT {identity_sql()}, k.id FROM keywords k
            UNION
            SELECT d.identity, k.id FROM descendants d
            JOIN keywords k ON k.parent_id = d.id
        )
        SELECT d.identity, COUNT(DISTINCT pk.photo_id) AS photo_count,
               COUNT(DISTINCT CASE WHEN ({identity_sql()}) = d.identity
                 THEN pk.photo_id END) AS direct_photo_count
        FROM descendants d JOIN photo_keywords pk ON pk.keyword_id = d.id
        JOIN keywords k ON k.id = pk.keyword_id
        JOIN photos p ON p.id = pk.photo_id
        JOIN workspace_folders wf ON wf.folder_id = p.folder_id
        WHERE wf.workspace_id = ? GROUP BY d.identity""", (db._ws_id(),),
    )}
    aliases = defaultdict(list)
    for row in db.conn.execute('SELECT keyword_id, path_json FROM keyword_import_aliases'):
        aliases[row['keyword_id']].append(json.loads(row['path_json']))
    result = []
    for identity, members in groups.items():
        members.sort(key=lambda r: (r['parent_id'] is not None, r['id']))
        group = dict(members[0])
        group['identity'] = identity
        group['members'] = members
        group['paths'] = [m['path'] for m in members]
        for member in members:
            group['paths'].extend(aliases[member['id']])
        group['photo_count'] = counts.get(identity, {'photo_count': 0})['photo_count']
        group['direct_photo_count'] = counts.get(identity, {'direct_photo_count': 0})['direct_photo_count']
        result.append(group)
    return result


def location_candidates(db):
    """Preview global effects for visible leaf keywords, never infer a match."""
    rows = [dict(r) for r in db.conn.execute(
        'SELECT id, name, parent_id, type, is_species, place_id FROM keywords'
    )]
    paths = keyword_paths(rows)
    by_id = {r['id']: r for r in rows}
    visible = {r['id'] for r in db.get_keyword_tree()}
    parents = {r['parent_id'] for r in rows}
    children = defaultdict(list)
    for row in rows:
        children[row['parent_id']].append(row['id'])
    places = defaultdict(list)
    for row in rows:
        if row['type'] == 'location' and row['place_id']:
            places[keyword_match_key(row['name'])].append(row)
    result = []
    for source in rows:
        if (source['id'] not in visible or source['id'] in parents
                or source['type'] not in ('general', 'location')
                or source['is_species'] or source['place_id']):
            continue
        for target in places[keyword_match_key(source['name'])]:
            same_chain = {source['id'], target['id']}
            parent_id = target['parent_id']
            while parent_id is not None and parent_id not in same_chain:
                same_chain.add(parent_id)
                parent_id = by_id[parent_id]['parent_id']
            descendants = list(children[target['id']])
            while descendants:
                descendant = descendants.pop()
                if descendant not in same_chain:
                    same_chain.add(descendant)
                    descendants.extend(children[descendant])
            placeholders = ','.join('?' for _ in same_chain)
            conflicts = db.conn.execute(
                f"""SELECT COUNT(DISTINCT source.photo_id) FROM photo_keywords source
                    JOIN photo_keywords other ON other.photo_id = source.photo_id
                    JOIN keywords k ON k.id = other.keyword_id
                    WHERE source.keyword_id = ? AND k.type = 'location'
                      AND k.place_id IS NOT NULL AND k.id NOT IN ({placeholders})""",
                (source['id'], *sorted(same_chain)),
            ).fetchone()[0]
            counts = db.conn.execute(
                """SELECT COUNT(DISTINCT CASE WHEN keyword_id = ? THEN photo_id END) AS source_count,
                          COUNT(DISTINCT photo_id) AS combined_count
                   FROM photo_keywords WHERE keyword_id IN (?, ?)""",
                (source['id'], source['id'], target['id']),
            ).fetchone()
            if not counts['source_count']:
                continue
            result.append({
                'source_id': source['id'], 'target_id': target['id'],
                'name': source['name'], 'source_path': paths[source['id']],
                'target_path': paths[target['id']], 'place_id': target['place_id'],
                'conflicting_photo_count': conflicts,
                **dict(counts),
            })
    return result


def reconcile_location(db, source_id, target_id):
    """Apply one reviewed pair atomically, retaining its import spelling/path."""
    with db.conn:
        # Reserve the writer before validating a preview that may be stale.
        db.conn.execute('UPDATE db_meta SET value = value WHERE 0')
        candidate = next((c for c in location_candidates(db)
                          if c['source_id'] == source_id and c['target_id'] == target_id), None)
        if candidate is None:
            raise ValueError('This location match is no longer available. Refresh the preview.')
        if candidate['conflicting_photo_count']:
            raise ValueError('Some photos already have a different linked place. Resolve those locations before combining.')
        key = path_key(candidate['source_path'])
        existing = db.conn.execute(
            'SELECT keyword_id FROM keyword_import_aliases WHERE path_key = ?', (key,),
        ).fetchone()
        if existing and existing['keyword_id'] != target_id:
            raise ValueError('This imported path is already linked to a different place.')
        db.conn.execute(
            'INSERT OR REPLACE INTO keyword_import_aliases(path_key, path_json, keyword_id) VALUES (?, ?, ?)',
            (key, json.dumps(candidate['source_path'], ensure_ascii=False), target_id),
        )
        # No children are eligible, so this cannot move an imported subtree.
        # The shared merger preserves provenance, pending edits and history.
        affected = db.conn.execute(
            'SELECT pk.photo_id, wf.workspace_id FROM photo_keywords pk '
            'JOIN photos p ON p.id = pk.photo_id '
            'JOIN workspace_folders wf ON wf.folder_id = p.folder_id '
            'WHERE pk.keyword_id = ?', (source_id,),
        ).fetchall()
        db._merge_keyword_into(source_id, target_id)
        for row in affected:
            db.remove_pending_changes(row['photo_id'], 'location',
                                      workspace_id=row['workspace_id'], _commit=False)
            db.queue_change(row['photo_id'], 'location', 'effective',
                            workspace_id=row['workspace_id'], _commit=False)
    return candidate
