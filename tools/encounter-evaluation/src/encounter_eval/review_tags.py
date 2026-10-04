"""Apply human review labels using Vireo's keyword and pending-XMP writers."""
from __future__ import annotations

import json
from pathlib import Path

from .common import configure_repo, digest
from .review import metadata, same_photo


def ensure_tag_sync(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS tag_sync(
        photo_id INTEGER PRIMARY KEY, revision INTEGER NOT NULL,
        status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', result TEXT)''')


def _apply_tags(db, photo, answer, marker):
    """One library transaction, including a receipt for crash-safe retries."""
    from services.pending_changes import queue_keyword_add, queue_keyword_remove

    pid = photo['id']
    with db.conn:
        db.conn.execute('BEGIN IMMEDIATE')
        now = db.conn.execute('''SELECT p.filename,p.file_hash,f.path AS folder
            FROM photos p JOIN folders f ON f.id=p.folder_id
            JOIN workspace_folders wf ON wf.folder_id=p.folder_id
            WHERE p.id=? AND wf.workspace_id=?''', (pid, db._ws_id())).fetchone()
        if now is None or not same_photo(photo, dict(now)):
            raise ValueError('Photo no longer matches this library or workspace; no tags were changed')
        fingerprint = digest([answer['revision'], answer['taxa'], answer['status']])
        receipt = db.get_meta(marker)
        if receipt:
            receipt = json.loads(receipt)
            if receipt['revision'] >= answer['revision']:
                if receipt['fingerprint'] != fingerprint:
                    raise ValueError('A newer or different review was already applied; reconcile this review copy')
                return receipt['result']

        rows = [dict(r) for r in db.conn.execute('''SELECT k.* FROM keywords k
            JOIN photo_keywords pk ON pk.keyword_id=k.id WHERE pk.photo_id=?''', (pid,))]
        desired = set()
        for key in answer['taxa']:
            prefix, _, value = key.partition(':')
            if prefix not in {'inat', 'taxon'} or not value.isdigit():
                raise ValueError('Choose a catalog species with a resolved identity before updating Vireo tags')
            column = 'inat_id' if prefix == 'inat' else 'id'
            taxon = db.conn.execute(f"SELECT * FROM taxa WHERE {column}=? AND rank='species'", (int(value),)).fetchone()
            if taxon is None:
                raise ValueError(f'Species {key} is no longer in the live catalog; choose its current identity')
            # Keep existing hierarchical tags and aliases for the selected identity.
            matching = [r for r in rows if db.is_keyword_species(r['id']) and
                        (r['source_taxon_id'] == taxon['inat_id'] if r['source_taxon_id'] is not None
                         else r['taxon_id'] == taxon['id'])]
            if matching:
                desired.update(r['id'] for r in matching)
                continue
            if taxon['inat_id']:
                kid = db.add_keyword(taxon['common_name'] or taxon['name'], is_species=True,
                                     source_taxon_id=taxon['inat_id'], _commit=False)
            else:
                # Scientific names avoid common-name homonyms for local-only taxa.
                kid = db.add_keyword(taxon['name'], is_species=True, _commit=False)
                linked = db.conn.execute('SELECT taxon_id FROM keywords WHERE id=?', (kid,)).fetchone()
                if linked['taxon_id'] != taxon['id']:
                    raise ValueError(f'Could not safely resolve local species {key}; no tags were changed')
            desired.add(kid)

        existing = {r['id'] for r in rows}
        removed = [r for r in rows if r['id'] not in desired and db.is_keyword_species(r['id'])] if answer['status'] == 'complete' else []
        for row in removed:
            db.untag_photo(pid, row['id'], _commit=False)
            queue_keyword_remove(db, pid, row['name'], _commit=False)
            db.record_edit('keyword_remove', f'Species review: removed "{row["name"]}"', str(row['id']),
                           [{'photo_id': pid, 'old_value': str(row['id']), 'new_value': ''}], _commit=False)
        for kid in sorted(desired):
            db.tag_photo(pid, kid, source='manual', _commit=False)
            if kid not in existing:
                name = db.conn.execute('SELECT name FROM keywords WHERE id=?', (kid,)).fetchone()['name']
                queue_keyword_add(db, pid, name, _commit=False)
                db.record_edit('keyword_add', f'Species review: added "{name}"', str(kid),
                               [{'photo_id': pid, 'old_value': '', 'new_value': str(kid)}], _commit=False)
        result = {'added': len(desired - existing), 'removed': len(removed)}
        db.set_meta(marker, json.dumps({'revision': answer['revision'], 'fingerprint': fingerprint, 'result': result}), _commit=False)
    return result


def sync_review_tags(conn, photo_id):
    """Keep a durable pending state until the library confirms this revision.

    A library receipt and tag mutations commit together. If the process stops
    before updating this separate review DB, retry acknowledges that receipt
    without overwriting later edits or duplicating history / XMP work.
    """
    configure_repo()
    from db import Database

    with conn:
        conn.execute('BEGIN IMMEDIATE')
        sync = conn.execute('SELECT * FROM tag_sync WHERE photo_id=?', (photo_id,)).fetchone()
        if sync is None or sync['status'] == 'applied':
            return dict(sync) if sync else None
        review = conn.execute('SELECT * FROM reviews WHERE photo_id=?', (photo_id,)).fetchone()
        row = conn.execute('SELECT partition,data FROM photos WHERE id=?', (photo_id,)).fetchone()
        db = None
        try:
            if not row or row['partition'] not in {'train', 'development'} or review['revision'] != sync['revision']:
                raise ValueError('Review is outside the eligible pool or has changed revision')
            meta = metadata(conn)
            if not Path(meta['library']).is_file():
                raise ValueError('The Vireo library is unavailable')
            db = Database(meta['library'], initialize_schema=False)
            db.set_active_workspace(meta['workspace'])
            answer = dict(review)
            answer['taxa'] = json.loads(answer['taxa'])
            marker = 'encounter_review:' + digest([meta['created_at'], meta['source_data_digest'], photo_id])
            result = _apply_tags(db, json.loads(row['data']), answer, marker)
            conn.execute("UPDATE tag_sync SET status='applied',error='',result=? WHERE photo_id=?",
                         (json.dumps(result), photo_id))
        except Exception as exc:
            conn.execute('UPDATE tag_sync SET error=? WHERE photo_id=?', (str(exc), photo_id))
        finally:
            if db:
                db.close()
        return dict(conn.execute('SELECT * FROM tag_sync WHERE photo_id=?', (photo_id,)).fetchone())
