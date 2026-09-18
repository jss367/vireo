"""Keyword identities and explicit keyword merges.

Hierarchy nodes remain distinct records. Taxon/place identity groups them for
display; only a user-confirmed merge moves associations.
"""

import hashlib
import json
from collections import defaultdict

from keyword_normalization import keyword_match_key
from xmp import (
    _parse_location_keywords_owned,
    location_keyword_entries,
    read_vireo_location_keywords,
    read_vireo_location_keywords_owned,
)


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


def resolve_import_alias(db, name, parent_id, *, kw_type=None):
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
    return resolve_import_path(db, list(reversed(parts)), kw_type=kw_type)


def resolve_import_path(db, parts, *, kw_type=None, linked_locations_only=False):
    """Resolve reviewed import paths without overriding an explicit type."""
    row = db.conn.execute(
        "SELECT a.keyword_id FROM keyword_import_aliases a "
        "JOIN keywords k ON k.id = a.keyword_id "
        "WHERE a.path_key = ? AND (? IS NULL OR k.type = ?) "
        "AND (? = 0 OR (k.type = 'location' AND k.place_id IS NOT NULL))",
        (path_key(parts), kw_type, kw_type, linked_locations_only),
    ).fetchone()
    if row:
        return row['keyword_id']
    if len(parts) != 1:
        return None
    # Flat-only metadata cannot distinguish parent paths. Resolve a merged
    # leaf only when every matching alias/live identity agrees on its target.
    name = keyword_match_key(parts[0])
    candidates = {
        row['keyword_id'] for row in db.conn.execute(
            'SELECT a.keyword_id, a.path_json FROM keyword_import_aliases a '
            'JOIN keywords k ON k.id = a.keyword_id WHERE (? IS NULL OR k.type = ?)',
            (kw_type, kw_type),
        ) if keyword_match_key(json.loads(row['path_json'])[-1]) == name
    }
    if not candidates:
        return None
    candidates.update(
        row['id'] for row in db.conn.execute(
            'SELECT id, name FROM keywords WHERE (? IS NULL OR type = ?)', (kw_type, kw_type),
        ) if keyword_match_key(row['name']) == name
    )
    if len(candidates) != 1:
        return None
    target_id = candidates.pop()
    if linked_locations_only:
        target = db.conn.execute('SELECT type, place_id FROM keywords WHERE id = ?', (target_id,)).fetchone()
        if target['type'] != 'location' or target['place_id'] is None:
            return None
    return target_id


def resolve_merge_target(db, merge):
    """Follow subsequent merges through the durable source-path alias."""
    return resolve_import_path(db, merge['source_path']) or merge['target_id']


def filter_removed_import_aliases(db, photo_id, flat_keywords, hierarchical_keywords,
                                 flat_removals, hierarchical_removals):
    """Do not restore a removed tag through an old, still-unsynced alias."""
    merges = [json.loads(row['value']) for row in db.conn.execute(
        "SELECT value FROM pending_changes WHERE photo_id = ? AND change_type = 'keyword_merge'", (photo_id,),
    )]
    if not flat_removals and not hierarchical_removals and not merges:
        return flat_keywords, hierarchical_keywords
    aliases = {
        row['path_key']: keyword_match_key(row['name'])
        for row in db.conn.execute(
            'SELECT a.path_key, k.name FROM keyword_import_aliases a '
            'JOIN keywords k ON k.id = a.keyword_id'
        )
    }
    blocked_paths, blocked_names = set(), set()
    if merges:
        tagged = db.get_photo_keywords(photo_id)
        tagged_ids = {k['id'] for k in tagged}
        tagged_names = {keyword_match_key(k['name']) for k in tagged}
        paths = keyword_paths(db.conn.execute('SELECT id, name, parent_id FROM keywords').fetchall())
        tagged_paths = {path_key(paths[k['id']]) for k in tagged}
        for merge in merges:
            if resolve_merge_target(db, merge) in tagged_ids:
                continue
            target_path = paths.get(resolve_merge_target(db, merge), merge['target_path'])
            for removed_path in (merge['source_path'], target_path):
                key = path_key(removed_path)
                if key not in tagged_paths:
                    blocked_paths.add(key)
                if keyword_match_key(removed_path[-1]) not in tagged_names:
                    blocked_names.add(keyword_match_key(removed_path[-1]))
    return (
        [name for name in flat_keywords if aliases.get(path_key([name])) not in flat_removals
         and keyword_match_key(name) not in blocked_names],
        [path for path in hierarchical_keywords
         if aliases.get(path_key(path.split('|'))) not in hierarchical_removals
         and path_key(path.split('|')) not in blocked_paths],
    )


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
    dropped, and only when the ownership companion says Vireo actually
    inserted them -- a matching keyword the user typed in Lightroom is not
    Vireo's to drop and is imported exactly as before. Returns
    ``(flat_set, hierarchical_list)``.
    """
    flat, hierarchical = set(flat_keywords), list(hierarchical_keywords)
    if not db.has_pending_location_change(photo_id):
        return flat, hierarchical
    leaf, path = location_keyword_entries(read_vireo_location_keywords(xmp_path))
    if not path:
        return flat, hierarchical
    owns_flat, owns_hier = _parse_location_keywords_owned(
        read_vireo_location_keywords_owned(xmp_path),
    )

    leaf_key = keyword_match_key(leaf) if owns_flat else None
    path_keys = (
        [keyword_match_key(part) for part in path.split('|')] if owns_hier else None
    )
    return (
        {name for name in flat
         if not leaf_key or keyword_match_key(name) != leaf_key},
        [entry for entry in hierarchical
         if path_keys is None
         or [keyword_match_key(part) for part in entry.split('|')] != path_keys],
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
        target = resolve_import_path(db, parts, linked_locations_only=True)
        if target is not None:
            targets.add(target)
    flat_keys = {keyword_match_key(name) for name in flat_keywords}
    for name in flat_keywords:
        if keyword_match_key(name) not in hierarchy_leaves:
            target = resolve_import_path(db, [name], linked_locations_only=True)
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


def preview_keyword_merge(db, keyword_ids, target_id):
    """Validate an explicit merge of leaf records and describe catalog-wide effects."""
    if (not isinstance(keyword_ids, list) or not 2 <= len(keyword_ids) <= 100
            or any(type(k) is not int or k <= 0 for k in keyword_ids)
            or len(set(keyword_ids)) != len(keyword_ids)
            or type(target_id) is not int or target_id not in keyword_ids):
        raise ValueError('Select between 2 and 100 different keywords and choose one to keep.')
    selected = set(keyword_ids)
    visible = {r['id'] for r in db.get_keyword_tree()}
    if not selected <= visible:
        raise ValueError('A selected keyword is no longer available in this workspace. Refresh the list.')
    rows = [dict(r) for r in db.conn.execute('SELECT * FROM keywords ORDER BY id')]
    by_id = {r['id']: r for r in rows}
    paths = keyword_paths(rows)
    target = by_id[target_id]
    sources = [by_id[k] for k in sorted(selected - {target_id})]
    parents = {r['parent_id'] for r in rows}
    if any(r['id'] in parents for r in sources):
        raise ValueError('A keyword being merged has child keywords. Select individual leaf keywords instead.')
    for source in sources:
        if (source['type'] != target['type']
                and not (source['type'] == 'general' and target['type'] == 'location')):
            raise ValueError('Choose keywords of the same type, or merge general keywords into a location.')
        if bool(source['is_species'] or source['type'] == 'taxonomy') != bool(
                target['is_species'] or target['type'] == 'taxonomy'):
            raise ValueError('Species keywords cannot be merged with other keyword types.')
        for field in ('taxon_id', 'source_taxon_id', 'place_id'):
            if source[field] is not None and source[field] != target[field]:
                raise ValueError('Linked places or species must match. Choose the linked keyword to keep; different links cannot be combined.')

    placeholders = ','.join('?' for _ in keyword_ids)
    tags = [dict(r) for r in db.conn.execute(
        f'''WITH RECURSIVE descendants(id) AS (
                SELECT id FROM keywords WHERE id IN ({placeholders})
                UNION
                SELECT k.id FROM keywords k JOIN descendants d ON k.parent_id = d.id
            )
            SELECT photo_id, keyword_id, source FROM photo_keywords
            WHERE keyword_id IN (SELECT id FROM descendants)
            ORDER BY photo_id, keyword_id''', keyword_ids,
    )]
    if target['type'] == 'location' and target['place_id']:
        # Ancestors/descendants of the retained place are compatible; a second
        # independent linked place on any affected photo is not.
        compatible = set(selected)
        parent_id = target['parent_id']
        while parent_id is not None and parent_id not in compatible:
            compatible.add(parent_id)
            parent_id = by_id[parent_id]['parent_id']
        children = defaultdict(list)
        for row in rows:
            children[row['parent_id']].append(row['id'])
        pending = list(children[target_id])
        while pending:
            kid = pending.pop()
            if kid not in compatible:
                compatible.add(kid)
                pending.extend(children[kid])
        others = db.conn.execute(
            f"""SELECT DISTINCT k.id FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id
                WHERE k.type = 'location' AND k.place_id IS NOT NULL
                AND pk.photo_id IN (SELECT photo_id FROM photo_keywords
                                   WHERE keyword_id IN ({placeholders}))""", keyword_ids,
        )
        if any(r['id'] not in compatible for r in others):
            raise ValueError('Some photos already have a different linked place. Resolve those locations before merging.')
    for source in sources:
        alias = db.conn.execute('SELECT keyword_id FROM keyword_import_aliases WHERE path_key = ?',
                                (path_key(paths[source['id']]),)).fetchone()
        if alias and alias['keyword_id'] not in selected:
            raise ValueError('An imported path already resolves to a different keyword.')

    # Keep coordinate pairs together; never synthesize a point from two rows.
    coordinate_record = next((r for r in [target, *sources]
                              if r['latitude'] is not None and r['longitude'] is not None), None)
    result = {
        'target': {**target, 'path': paths[target_id]},
        'sources': [{**r, 'path': paths[r['id']]} for r in sources],
        'combined_count': len({t['photo_id'] for t in tags}),
        'latitude': coordinate_record['latitude'] if coordinate_record else None,
        'longitude': coordinate_record['longitude'] if coordinate_record else None,
    }
    result['preview_token'] = hashlib.sha256(
        json.dumps([result, tags], sort_keys=True).encode()
    ).hexdigest()
    return result


def merge_keywords(db, keyword_ids, target_id, preview_token):
    """Merge a reviewed selection atomically, including pending sidecar edits."""
    with db.conn:
        db.conn.execute('UPDATE db_meta SET value = value WHERE 0')
        preview = preview_keyword_merge(db, keyword_ids, target_id)
        if preview_token != preview['preview_token']:
            raise ValueError('The selected keywords changed. Review the updated preview before merging.')
        target = preview['target']
        affected = []
        for source in preview['sources']:
            affected.extend((dict(r), source) for r in db.conn.execute(
                'SELECT pk.photo_id, wf.workspace_id FROM photo_keywords pk '
                'JOIN photos p ON p.id = pk.photo_id '
                'JOIN workspace_folders wf ON wf.folder_id = p.folder_id WHERE pk.keyword_id = ?',
                (source['id'],),
            ))
            # Remember every merged path, including same-name leaves under
            # different parents. Existing sidecars/catalogs can retain that
            # hierarchy even after a flat keyword_add has been synchronized.
            db.conn.execute(
                'INSERT OR REPLACE INTO keyword_import_aliases(path_key, path_json, keyword_id) VALUES (?, ?, ?)',
                (path_key(source['path']), json.dumps(source['path'], ensure_ascii=False), target_id),
            )
            db._merge_keyword_into(source['id'], target_id, pending_source_only=True)
        db.conn.execute('UPDATE keywords SET latitude = ?, longitude = ? WHERE id = ?',
                        (preview['latitude'], preview['longitude'], target_id))
        for row, source in affected:
            pid, ws = row['photo_id'], row['workspace_id']
            old_name = source['name']
            db.queue_change(pid, 'keyword_merge', json.dumps({
                'source_path': source['path'], 'target_id': target_id, 'target_path': target['path'],
            }, sort_keys=True), workspace_id=ws, _commit=False)
            if old_name != target['name']:
                # Only remove the old flat name if another surviving keyword
                # on this photo does not still need it.
                still_used = db.conn.execute(
                    'SELECT k.name FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id '
                    'WHERE pk.photo_id = ?', (pid,),
                )
                if not any(keyword_match_key(r['name']) == keyword_match_key(old_name) for r in still_used):
                    db.queue_change(pid, 'keyword_remove_flat', old_name, workspace_id=ws, _commit=False)
            db.remove_pending_changes(pid, 'keyword_remove', target['name'], workspace_id=ws, _commit=False)
            db.clear_equivalent_flat_removals(
                [{'photo_id': pid, 'change_type': 'keyword_remove_flat', 'value': target['name']}], _commit=False,
            )
            db.queue_change(pid, 'keyword_add', target['name'], workspace_id=ws, _commit=False)
        if target['type'] == 'location':
            # Filling previously missing coordinates also affects photos that
            # already had the retained keyword before the merge.
            for row in db.conn.execute(
                'SELECT pk.photo_id, wf.workspace_id FROM photo_keywords pk '
                'JOIN photos p ON p.id = pk.photo_id '
                'JOIN workspace_folders wf ON wf.folder_id = p.folder_id WHERE pk.keyword_id = ?',
                (target_id,),
            ).fetchall():
                db.remove_pending_changes(row['photo_id'], 'location', workspace_id=row['workspace_id'], _commit=False)
                db.queue_change(row['photo_id'], 'location', 'effective', workspace_id=row['workspace_id'], _commit=False)
    return preview
