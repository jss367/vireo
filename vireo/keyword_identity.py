"""Keyword identities and explicit keyword merges.

Hierarchy nodes remain distinct records. Taxon/place identity groups them for
display; only a user-confirmed merge moves associations.
"""

import hashlib
import json
from collections import defaultdict

from keyword_normalization import keyword_match_key, normalize_keyword_display
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
        found = db.conn.execute(
            'SELECT inat_id FROM taxa WHERE id = ?', (row['taxon_id'],),
        ).fetchone()
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

def _child_index(rows):
    """parent_id -> ordered child ids, as the merge walks them."""
    children = defaultdict(list)
    for row in sorted(rows, key=lambda r: r['id']):
        children[row['parent_id']].append(row['id'])
    return children


def _subtree_ids(children, root_id):
    found, pending = set(), [root_id]
    while pending:
        for kid in children.get(pending.pop(), ()):
            if kid not in found:
                found.add(kid)
                pending.append(kid)
    return found


def _plan_subtree_merge(db, nodes, children, src_id, dst_id, plan):
    """Mirror ``Database._merge_keyword_into``'s child handling in memory.

    The preview has to state what happens to every descendant, and
    ``merge_keywords`` needs each survivor's new path to queue the sidecar
    rewrite -- both require replaying the same collision rules the write
    path applies, so the resolution lives here once and is asserted against
    the real merge in the tests.
    """
    plan['removed'].add(src_id)
    for child_id in list(children.get(src_id, ())):
        child = nodes[child_id]
        existing = next(
            (nodes[k] for k in children.get(dst_id, ())
             if k not in plan['removed'] and nodes[k]['name'] == child['name']),
            None,
        )
        if existing is None:
            outcome = 'move'
        elif keywords_claim_different_taxa(db, existing, child):
            # Distinct species that collide on (name, parent): merging would
            # retag the incoming row's photos as the other species, so the
            # write path keeps both under an id suffix.
            outcome = 'rename'
            plan['renames'][child_id] = f"{child['name']} (id-{child_id})"
        elif (existing['type'] == 'location' and child['type'] == 'location'
              and existing['place_id'] is not None and child['place_id'] is not None
              and existing['place_id'] != child['place_id']):
            # Distinct Google places that collide on (name, parent): the write
            # path keeps both by suffixing the incoming row.
            outcome = 'rename'
            plan['renames'][child_id] = f"{child['name']} ({child['place_id'][-8:]})"
        elif existing['type'] == child['type']:
            outcome = 'merge'
        else:
            outcome = 'rename'
            plan['renames'][child_id] = f"{child['name']} (id-{child_id})"
        plan['children'].append({
            'id': child_id, 'name': child['name'], 'type': child['type'],
            'outcome': outcome,
            'into_id': existing['id'] if outcome == 'merge' else None,
            'new_name': plan['renames'].get(child_id, child['name']),
        })
        if outcome == 'merge':
            _plan_subtree_merge(db, nodes, children, child_id, existing['id'], plan)
        else:
            children[src_id].remove(child_id)
            children[dst_id].append(child_id)
            nodes[child_id]['parent_id'] = dst_id
            nodes[child_id]['name'] = plan['renames'].get(child_id, child['name'])


def _coordinate_pair(row):
    if row['latitude'] is None or row['longitude'] is None:
        return None
    return (row['latitude'], row['longitude'])


def _distinct(values):
    """Preserve first-seen order while grouping the ids that carry each value."""
    ordered = {}
    for key, source_id, payload in values:
        entry = ordered.setdefault(key, {**payload, 'from': []})
        entry['from'].append(source_id)
    return list(ordered.values())


def _merge_options(records, paths, valid_parents):
    """Every value each editable attribute can take, tagged with its source rows."""
    return {
        'name': _distinct([(r['name'], r['id'], {'value': r['name']}) for r in records]),
        'parent': _distinct([
            (r['parent_id'], r['id'],
             {'parent_id': r['parent_id'], 'path': paths[r['id']][:-1]})
            for r in records if r['parent_id'] in valid_parents
        ]),
        'type': _distinct([(r['type'], r['id'], {'value': r['type']}) for r in records]),
        'place': _distinct([
            (r['place_id'], r['id'],
             {'place_id': r['place_id'], 'name': r['name'], 'path': paths[r['id']],
              'latitude': r['latitude'], 'longitude': r['longitude']})
            for r in records if r['place_id'] is not None
        ]),
        'species': _distinct([
            ((r['taxon_id'], r['source_taxon_id']), r['id'],
             {'taxon_id': r['taxon_id'], 'source_taxon_id': r['source_taxon_id'],
              'name': r['name']})
            for r in records
            if r['taxon_id'] is not None or r['source_taxon_id'] is not None
        ]),
        'coordinates': _distinct([
            (_coordinate_pair(r), r['id'],
             {'latitude': r['latitude'], 'longitude': r['longitude'], 'name': r['name']})
            for r in records if _coordinate_pair(r) is not None
        ]),
    }



def _label_species_options(db, species_options):
    """Name the taxon behind each species link.

    The chooser asks which species the combined keyword keeps, so it has to
    show the species. A bare ``taxon_id`` is a row number the user has never
    seen and cannot answer the question with.
    """
    for option in species_options:
        # ``source_taxon_id`` is the label source's own identity and outranks
        # the common-name lookup (see identity_sql). When it names a taxon the
        # local taxonomy does not have, falling back to ``taxon_id``'s name
        # would print the OTHER option's species and make the two choices
        # indistinguishable in the dialog.
        if option['source_taxon_id'] is not None:
            row = db.conn.execute(
                'SELECT name, common_name FROM taxa WHERE inat_id = ?',
                (option['source_taxon_id'],),
            ).fetchone()
        elif option['taxon_id'] is not None:
            row = db.conn.execute(
                'SELECT name, common_name FROM taxa WHERE id = ?',
                (option['taxon_id'],),
            ).fetchone()
        else:
            row = None
        option['taxon_name'] = row['name'] if row else None
        option['taxon_common_name'] = row['common_name'] if row else None

def _coerce_coordinate(value, limit, label):
    if type(value) is bool or not isinstance(value, (int, float)):
        raise ValueError(f'Enter a numeric {label}.')
    if not -limit <= float(value) <= limit:
        raise ValueError(f'{label.capitalize()} must be between -{limit:g} and {limit:g}.')
    return float(value)


def _resolve_merge_fields(records, options, overrides):
    """Fold the chooser's picks over the defaults, or report what must be asked.

    Defaults reproduce the pre-chooser behavior: the retained row's own
    identity wins and a value it lacks is filled from another selected row.
    A field where two rows each carry a DIFFERENT real value has no honest
    default -- ``requires_choice`` names it so the dialog asks instead of
    picking one silently.
    """
    target = records[0]
    resolved, requires_choice = {}, []

    name = overrides.get('name', None)
    if name is None:
        resolved['name'] = target['name']
    else:
        if not isinstance(name, str):
            raise ValueError('Enter a name for the combined keyword.')
        resolved['name'] = normalize_keyword_display(name)
        if not resolved['name']:
            raise ValueError('Enter a name for the combined keyword.')

    if 'parent_id' in overrides:
        chosen = overrides['parent_id']
        if chosen is not None and type(chosen) is not int:
            raise ValueError('Choose one of the offered parent paths.')
        if chosen not in {option['parent_id'] for option in options['parent']}:
            raise ValueError('Choose one of the offered parent paths.')
        resolved['parent_id'] = chosen
    else:
        resolved['parent_id'] = target['parent_id']

    if 'type' in overrides:
        if overrides['type'] not in {option['value'] for option in options['type']}:
            raise ValueError('Choose one of the selected keywords’ types.')
        resolved['type'] = overrides['type']
    else:
        resolved['type'] = target['type']

    if 'place_id' in overrides:
        chosen = overrides['place_id']
        if chosen is not None and chosen not in {o['place_id'] for o in options['place']}:
            raise ValueError('Choose one of the selected keywords’ linked places, or no link.')
        resolved['place_id'] = chosen
    elif len(options['place']) > 1:
        resolved['place_id'] = None
        requires_choice.append('place')
    else:
        resolved['place_id'] = options['place'][0]['place_id'] if options['place'] else None

    if 'species' in overrides:
        chosen = overrides['species']
        if chosen is None:
            resolved['taxon_id'] = resolved['source_taxon_id'] = None
        else:
            match = next((o for o in options['species']
                          if o['taxon_id'] == chosen.get('taxon_id')
                          and o['source_taxon_id'] == chosen.get('source_taxon_id')),
                         None) if isinstance(chosen, dict) else None
            if match is None:
                raise ValueError('Choose one of the selected keywords’ species links, or no link.')
            resolved['taxon_id'] = match['taxon_id']
            resolved['source_taxon_id'] = match['source_taxon_id']
    elif len(options['species']) > 1:
        resolved['taxon_id'] = resolved['source_taxon_id'] = None
        requires_choice.append('species')
    elif options['species']:
        resolved['taxon_id'] = options['species'][0]['taxon_id']
        resolved['source_taxon_id'] = options['species'][0]['source_taxon_id']
    else:
        resolved['taxon_id'] = resolved['source_taxon_id'] = None

    if 'coordinates' in overrides:
        chosen = overrides['coordinates']
        if chosen is None:
            resolved['latitude'] = resolved['longitude'] = None
        elif isinstance(chosen, dict):
            resolved['latitude'] = _coerce_coordinate(chosen.get('latitude'), 90.0, 'latitude')
            resolved['longitude'] = _coerce_coordinate(chosen.get('longitude'), 180.0, 'longitude')
        else:
            raise ValueError('Enter both a latitude and a longitude, or clear the coordinates.')
    else:
        # Coordinates travel as a pair, and a chosen place outranks an
        # unrelated row's point: a marker drawn from stale coordinates would
        # put the retained Google place somewhere it isn't.
        linked = next((o for o in options['place']
                       if o['place_id'] == resolved['place_id']
                       and o['latitude'] is not None and o['longitude'] is not None), None)
        fallback = next((o for o in options['coordinates']), None)
        picked = linked or fallback
        resolved['latitude'] = picked['latitude'] if picked else None
        resolved['longitude'] = picked['longitude'] if picked else None

    return resolved, requires_choice


def preview_keyword_merge(db, keyword_ids, target_id, overrides=None):
    """Validate an explicit merge and describe its catalog-wide effects.

    The retained row's attributes are defaults, not fixed points: ``overrides``
    carries the dialog's per-field picks (name, parent path, type, linked
    place, species link, coordinates) so a user can keep one row's spelling
    alongside another row's place. Selections whose subtrees overlap are the
    one arrangement still refused outright -- merging an ancestor into its own
    descendant would leave the survivor parented to itself.
    """
    if (not isinstance(keyword_ids, list) or not 2 <= len(keyword_ids) <= 100
            or any(type(k) is not int or k <= 0 for k in keyword_ids)
            or len(set(keyword_ids)) != len(keyword_ids)
            or type(target_id) is not int or target_id not in keyword_ids):
        raise ValueError('Select between 2 and 100 different keywords and choose one to keep.')
    if overrides is None:
        overrides = {}
    if not isinstance(overrides, dict) or not set(overrides) <= {
            'name', 'parent_id', 'type', 'place_id', 'species', 'coordinates'}:
        raise ValueError('Unrecognized merge settings. Refresh the dialog and try again.')
    selected = set(keyword_ids)
    visible = {r['id'] for r in db.get_keyword_tree()}
    if not selected <= visible:
        raise ValueError('A selected keyword is no longer available in this workspace. Refresh the list.')
    rows = [dict(r) for r in db.conn.execute('SELECT * FROM keywords ORDER BY id')]
    by_id = {r['id']: r for r in rows}
    paths = keyword_paths(rows)
    target = by_id[target_id]
    sources = [by_id[k] for k in sorted(selected - {target_id})]
    records = [target, *sources]
    children = _child_index(rows)

    if any(selected & _subtree_ids(children, source['id']) for source in sources):
        # A merged-away row hands its children to the survivor. When one of
        # those children is the survivor itself, that writes the survivor as
        # its own parent; when it is another selected row, the child is
        # absorbed before its own turn comes and its photos land somewhere
        # the preview never promised. Keeping the outermost row is safe --
        # the target may contain sources, only the reverse is refused.
        raise ValueError('A keyword you are merging contains another selected keyword. '
                         'Keep the outermost one, or merge the inner branch first.')

    species_bearing = {bool(r['is_species'] or r['type'] == 'taxonomy') for r in records}
    if len(species_bearing) > 1:
        raise ValueError('Species keywords cannot be merged with other keyword types.')

    valid_parents = ({r['parent_id'] for r in records}
                     - selected - _subtree_ids(children, target_id))
    options = _merge_options(records, paths, valid_parents)
    _label_species_options(db, options['species'])
    resolved, requires_choice = _resolve_merge_fields(records, options, overrides)

    notes = []
    if resolved['type'] != 'location' and resolved['place_id'] is not None:
        resolved['place_id'] = None
        notes.append('A Google place link only applies to location keywords, '
                     'so the combined keyword will keep no linked place.')
    if resolved['type'] == 'location' and '|' in resolved['name']:
        raise ValueError('A location name may not contain "|" — XMP keyword '
                         'hierarchies reserve it as the level delimiter.')

    # Replay the write path's reparenting so the preview can name each
    # descendant's fate and the merge can queue its sidecar path rewrite.
    nodes = {r['id']: dict(r) for r in rows}
    plan = {'removed': set(), 'renames': {}, 'children': []}
    for source in sources:
        _plan_subtree_merge(db, nodes, children, source['id'], target_id, plan)
    nodes[target_id]['name'] = resolved['name']
    nodes[target_id]['parent_id'] = resolved['parent_id']
    surviving = [node for node in nodes.values() if node['id'] not in plan['removed']]
    clash = next((n for n in surviving
                  if n['id'] != target_id and n['name'] == resolved['name']
                  and n['parent_id'] == resolved['parent_id']), None)
    if clash is not None:
        raise ValueError('Another keyword already sits at that name and parent path. '
                         'Choose a different name, a different parent, or select that keyword too.')
    new_paths = keyword_paths(surviving)
    path_changes = {n['id']: (paths[n['id']], new_paths[n['id']])
                    for n in surviving if new_paths[n['id']] != paths[n['id']]}

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
    if resolved['type'] == 'location' and resolved['place_id']:
        # Ancestors of the retained path and everything under it are one
        # place chain; a second independent linked place on an affected
        # photo is a real conflict the user has to resolve first.
        compatible = set(selected) | _subtree_ids(_child_index(surviving), target_id)
        parent_id = resolved['parent_id']
        while parent_id is not None and parent_id not in compatible:
            compatible.add(parent_id)
            parent_id = nodes[parent_id]['parent_id'] if parent_id in nodes else None
        # The affected photos are every photo under the selected SUBTREES,
        # not just the rows named in the selection. A photo tagged only on a
        # descendant the merge is about to move -- plus an unrelated linked
        # place -- would otherwise slip past this guard and then get a
        # location resync that exports the unrelated place's coordinates.
        others = db.conn.execute(
            f"""WITH RECURSIVE descendants(id) AS (
                    SELECT id FROM keywords WHERE id IN ({placeholders})
                    UNION
                    SELECT k.id FROM keywords k JOIN descendants d ON k.parent_id = d.id
                )
                SELECT DISTINCT k.id FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id
                WHERE k.type = 'location' AND k.place_id IS NOT NULL
                AND pk.photo_id IN (SELECT photo_id FROM photo_keywords
                                   WHERE keyword_id IN (SELECT id FROM descendants))""",
            keyword_ids,
        )
        if any(r['id'] not in compatible for r in others):
            raise ValueError('Some photos already have a different linked place. Resolve those locations before merging.')
    for source in sources:
        alias = db.conn.execute('SELECT keyword_id FROM keyword_import_aliases WHERE path_key = ?',
                                (path_key(paths[source['id']]),)).fetchone()
        if alias and alias['keyword_id'] not in selected:
            raise ValueError('An imported path already resolves to a different keyword.')

    subtree_counts = {}
    for entry in plan['children']:
        kid = entry['id']
        subtree = [kid, *_subtree_ids(_child_index(rows), kid)]
        subtree_counts[kid] = db.conn.execute(
            'SELECT COUNT(DISTINCT photo_id) FROM photo_keywords WHERE keyword_id IN '
            f'({",".join("?" for _ in subtree)})', subtree,
        ).fetchone()[0]
    for entry in plan['children']:
        entry['photo_count'] = subtree_counts[entry['id']]
        entry['from_path'] = paths[entry['id']]
        # A child that collapses into an existing sibling has no path of its
        # own afterwards; the honest destination is the sibling it lands in.
        entry['to_path'] = (new_paths.get(entry['id'])
                            or new_paths.get(entry['into_id']))

    result = {
        'target': {**target, 'path': paths[target_id]},
        'sources': [{**r, 'path': paths[r['id']]} for r in sources],
        'combined_count': len({t['photo_id'] for t in tags}),
        'latitude': resolved['latitude'],
        'longitude': resolved['longitude'],
        'resolved': {**resolved,
                     'path': new_paths[target_id],
                     'parent_path': new_paths[target_id][:-1]},
        'options': options,
        'requires_choice': requires_choice,
        'children': plan['children'],
        'path_changes': {str(k): v for k, v in path_changes.items()},
        'removed_count': len(plan['removed']),
        'notes': notes,
    }
    if not requires_choice:
        result['preview_token'] = hashlib.sha256(
            json.dumps([result, tags], sort_keys=True).encode()
        ).hexdigest()
    return result


def merge_keywords(db, keyword_ids, target_id, preview_token, overrides=None):
    """Merge a reviewed selection atomically, including pending sidecar edits."""
    with db.conn:
        db.conn.execute('UPDATE db_meta SET value = value WHERE 0')
        preview = preview_keyword_merge(db, keyword_ids, target_id, overrides)
        if preview['requires_choice']:
            raise ValueError('Choose which linked place or species the combined keyword keeps.')
        if preview_token != preview['preview_token']:
            raise ValueError('The selected keywords changed. Review the updated preview before merging.')
        target = preview['target']
        resolved = preview['resolved']
        # A child that collapses into an existing sibling loses its
        # photo_keywords rows to that sibling, so the photos carrying it have
        # to be read before the merge runs.
        collapsed = _collapsing_child_tags(db, preview)
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
        _apply_merge_overrides(db, target_id, target, resolved)
        _queue_survivor_rename(db, target_id, target['name'], resolved['name'])
        for row, source in affected:
            pid, ws = row['photo_id'], row['workspace_id']
            old_name = source['name']
            db.queue_change(pid, 'keyword_merge', json.dumps({
                'source_path': source['path'], 'target_id': target_id,
                'target_path': resolved['path'],
            }, sort_keys=True), workspace_id=ws, _commit=False)
            if old_name != resolved['name']:
                # Only remove the old flat name if another surviving keyword
                # on this photo does not still need it.
                still_used = db.conn.execute(
                    'SELECT k.name FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id '
                    'WHERE pk.photo_id = ?', (pid,),
                )
                if not any(keyword_match_key(r['name']) == keyword_match_key(old_name) for r in still_used):
                    db.queue_change(pid, 'keyword_remove_flat', old_name, workspace_id=ws, _commit=False)
            db.remove_pending_changes(pid, 'keyword_remove', resolved['name'], workspace_id=ws, _commit=False)
            db.clear_equivalent_flat_removals(
                [{'photo_id': pid, 'change_type': 'keyword_remove_flat', 'value': resolved['name']}], _commit=False,
            )
            db.queue_change(pid, 'keyword_add', resolved['name'], workspace_id=ws, _commit=False)
        _queue_moved_subtree_changes(db, preview, target_id, collapsed)
        if resolved['type'] == 'location' or target['type'] == 'location':
            # Filling previously missing coordinates, or moving the retained
            # place in the hierarchy, also affects photos that already had the
            # retained keyword before the merge.
            for row in db.conn.execute(
                'SELECT pk.photo_id, wf.workspace_id FROM photo_keywords pk '
                'JOIN photos p ON p.id = pk.photo_id '
                'JOIN workspace_folders wf ON wf.folder_id = p.folder_id WHERE pk.keyword_id = ?',
                (target_id,),
            ).fetchall():
                db.remove_pending_changes(row['photo_id'], 'location', workspace_id=row['workspace_id'], _commit=False)
                db.queue_change(row['photo_id'], 'location', 'effective', workspace_id=row['workspace_id'], _commit=False)
    return preview


def _apply_merge_overrides(db, target_id, target, resolved):
    """Write the chooser's resolved identity onto the survivor.

    Runs after every ``_merge_keyword_into`` so the sources are already gone:
    the partial ``UNIQUE(place_id) WHERE place_id IS NOT NULL`` index would
    otherwise reject moving a link that a still-present source row holds.
    """
    changed = {field: resolved[field] for field in
               ('name', 'parent_id', 'type', 'place_id', 'taxon_id',
                'source_taxon_id', 'latitude', 'longitude')
               if resolved[field] != target[field]}
    if not changed:
        return
    if 'name' in changed:
        # Keep every dependent name string (pending sidecar edits, species
        # curation) in lockstep with the row, exactly as a rename would.
        db._rename_keyword_dependents(target_id, target['name'], changed['name'])
    assignments = ', '.join(f'{field} = ?' for field in changed)
    db.conn.execute(f'UPDATE keywords SET {assignments} WHERE id = ?',
                    [*changed.values(), target_id])
    if resolved['type'] != 'taxonomy' and not resolved['taxon_id']:
        db.conn.execute('UPDATE keywords SET is_species = 0 WHERE id = ? AND type != ?',
                        (target_id, 'taxonomy'))



def _queue_survivor_rename(db, target_id, old_name, new_name):
    """Re-export photos that already carried the survivor under a new spelling.

    The source loop only covers photos tagged with a row the merge deleted.
    When the chooser renames the retained row, photos that were already on it
    keep their tag but their sidecar still holds the retired word, so they
    need the same flat remove/add a plain rename would queue.
    """
    if old_name == new_name:
        return
    rows = db.conn.execute(
        'SELECT pk.photo_id, wf.workspace_id FROM photo_keywords pk '
        'JOIN photos p ON p.id = pk.photo_id '
        'JOIN workspace_folders wf ON wf.folder_id = p.folder_id '
        'WHERE pk.keyword_id = ?', (target_id,),
    ).fetchall()
    for row in rows:
        pid, ws = row['photo_id'], row['workspace_id']
        still_used = db.conn.execute(
            'SELECT k.name FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id '
            'WHERE pk.photo_id = ? AND pk.keyword_id != ?', (pid, target_id),
        )
        if not any(keyword_match_key(r['name']) == keyword_match_key(old_name)
                   for r in still_used):
            db.queue_change(pid, 'keyword_remove_flat', old_name,
                            workspace_id=ws, _commit=False)
        db.queue_change(pid, 'keyword_add', new_name, workspace_id=ws, _commit=False)

def _collapsing_child_tags(db, preview):
    """Photos carrying a child that is about to collapse into a sibling.

    Read before the merge, because ``_merge_keyword_into`` moves these
    ``photo_keywords`` rows onto the surviving sibling and deletes the child.
    """
    collapsing = {child['id']: child for child in preview['children']
                  if child['outcome'] == 'merge'}
    if not collapsing:
        return []
    placeholders = ','.join('?' for _ in collapsing)
    rows = db.conn.execute(
        'SELECT pk.keyword_id, pk.photo_id, wf.workspace_id, k.type '
        'FROM photo_keywords pk '
        'JOIN keywords k ON k.id = pk.keyword_id '
        'JOIN photos p ON p.id = pk.photo_id '
        'JOIN workspace_folders wf ON wf.folder_id = p.folder_id '
        f'WHERE pk.keyword_id IN ({placeholders})', list(collapsing),
    ).fetchall()
    return [(dict(row), collapsing[row['keyword_id']]) for row in rows]


def _queue_moved_subtree_changes(db, preview, target_id, collapsed):
    """Rewrite sidecar hierarchies for every descendant the merge relocated.

    Reparenting a descendant leaves its flat ``dc:subject`` leaf alone but
    invalidates the ``lr:hierarchicalSubject`` path (and, for locations, the
    ``vireo:locationKeywords`` marker) that names its old ancestors. A
    ``keyword_merge`` change carries the old path to ``sync_to_xmp``, which
    replaces it with the current one.

    Two kinds of descendant need this. A row that survived under a new parent
    points the change at itself. A row that collapsed into a same-named
    sibling no longer exists, so its photos -- now the sibling's -- point at
    the sibling instead; without this their sidecars keep the retired
    hierarchy and a later rescan recreates the branch just merged away.
    """
    rewrites = {}
    for kid, (old_path, new_path) in preview['path_changes'].items():
        if int(kid) != target_id:
            rewrites[int(kid)] = (old_path, new_path, int(kid))
    for _, child in collapsed:
        rewrites[child['id']] = (child['from_path'], child['to_path'], child['into_id'])
    if not rewrites:
        return
    for old_path, _, destination_id in rewrites.values():
        db.conn.execute(
            'INSERT OR REPLACE INTO keyword_import_aliases(path_key, path_json, keyword_id) '
            'VALUES (?, ?, ?)',
            (path_key(old_path), json.dumps(old_path, ensure_ascii=False), destination_id),
        )
    surviving = [kid for kid in rewrites if kid not in {c['id'] for _, c in collapsed}]
    rows = []
    if surviving:
        placeholders = ','.join('?' for _ in surviving)
        rows = [(dict(r), r['keyword_id']) for r in db.conn.execute(
            'SELECT pk.keyword_id, pk.photo_id, wf.workspace_id, k.type '
            'FROM photo_keywords pk '
            'JOIN keywords k ON k.id = pk.keyword_id '
            'JOIN photos p ON p.id = pk.photo_id '
            'JOIN workspace_folders wf ON wf.folder_id = p.folder_id '
            f'WHERE pk.keyword_id IN ({placeholders})', surviving,
        ).fetchall()]
    rows.extend((row, child['id']) for row, child in collapsed)
    for row, kid in rows:
        old_path, new_path, destination_id = rewrites[kid]
        db.queue_change(row['photo_id'], 'keyword_merge', json.dumps({
            'source_path': old_path, 'target_id': destination_id,
            'target_path': new_path,
        }, sort_keys=True), workspace_id=row['workspace_id'], _commit=False)
        if row['type'] == 'location':
            db.remove_pending_changes(row['photo_id'], 'location',
                                      workspace_id=row['workspace_id'], _commit=False)
            db.queue_change(row['photo_id'], 'location', 'effective',
                            workspace_id=row['workspace_id'], _commit=False)
