"""Loopback-only review UI; saves reference labels and optionally updates Vireo tags."""
from __future__ import annotations

import argparse
import json
import secrets
import sqlite3
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .review import KINDS, build_queue, connect, metadata, same_photo, save_review
from .review_tags import ensure_tag_sync, sync_review_tags


def species_rows(conn, keys):
    result = []
    for key in keys or []:
        row = conn.execute('SELECT * FROM species WHERE key=?', (key,)).fetchone()
        result.append(dict(row) if row else {'key': key, 'name': key.removeprefix('name:'), 'scientific': ''})
    return result


def photo_payload(conn, pid):
    row = conn.execute('SELECT data FROM photos WHERE id=?', (pid,)).fetchone()
    if not row:
        raise ValueError('Photo is outside this review pool')
    photo = json.loads(row['data'])
    review = conn.execute('SELECT * FROM reviews WHERE photo_id=?', (pid,)).fetchone()
    photo['review'] = dict(review) if review else None
    sync = conn.execute('SELECT * FROM tag_sync WHERE photo_id=?', (pid,)).fetchone()
    photo['tag_sync'] = dict(sync) if sync else None
    if review:
        photo['review']['taxa'] = json.loads(review['taxa'])
    photo['selected_species'] = species_rows(conn, photo['review']['taxa'] if review else (photo['reference'] or {}).get('taxa', []))
    for suggestion in photo['suggestions'].values():
        suggestion['species'] = species_rows(conn, suggestion['taxa']) if suggestion['taxa'] is not None else None
    photo['neighbors'] = [{'id': i, 'filename': json.loads(conn.execute('SELECT data FROM photos WHERE id=?', (i,)).fetchone()[0])['filename']}
                          for i in photo['neighbors']]
    return photo


def image_path(conn, pid, media_root, large):
    row = conn.execute('SELECT data FROM photos WHERE id=?', (pid,)).fetchone()
    if not row:
        return None
    photo = json.loads(row['data'])
    library = metadata(conn)['library']
    # IDs can be reused or files replaced after the comparison snapshot.
    with sqlite3.connect(Path(library).as_uri() + '?mode=ro', uri=True) as live:
        live.row_factory = sqlite3.Row
        now = live.execute('SELECT p.filename,p.file_hash,f.path AS folder FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?', (pid,)).fetchone()
        if now is None or not same_photo(photo, dict(now)):
            return None
    root = Path(media_root).resolve()
    paths = [root / 'previews' / f'{pid}_{size}.jpg' for size in ([3840, 1920, 960] if large else [960, 1920])]
    thumb = photo.get('thumbnail')
    if thumb:
        p = Path(thumb)
        paths.append(p if p.is_absolute() else root / 'thumbnails' / p)
    paths.append(root / 'thumbnails' / f'{pid}.jpg')
    for path in paths:
        if path.resolve().is_relative_to(root) and path.is_file():
            return path
    return None


def make_server(queue, *, port=0, media_root=None, update_vireo_tags=False):
    queue = Path(queue).resolve()
    if not queue.is_file():
        raise ValueError('Review database does not exist')
    with connect(queue) as conn:
        meta = metadata(conn)
        ensure_tag_sync(conn)
        if meta.get('format_version') != 1:
            raise ValueError('Unsupported review database')
    token = secrets.token_urlsafe(32)
    media_root = Path(media_root or Path.home() / '.vireo').resolve()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass  # Do not log access tokens or private filenames.

        def send(self, status, body, content_type='application/json'):
            if not isinstance(body, bytes):
                body = json.dumps(body).encode()
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Referrer-Policy', 'no-referrer')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(body)

        def authorized(self, query):
            supplied = self.headers.get('Authorization', '').removeprefix('Bearer ') or query.get('token', [''])[0]
            return secrets.compare_digest(supplied, token)

        def do_GET(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            if not self.authorized(query):
                self.send(403, {'error': 'Open the review link printed by the server'})
                return
            conn = connect(queue)
            try:
                if parsed.path == '/':
                    self.send(200, Path(__file__).with_name('review.html').read_bytes(), 'text/html; charset=utf-8')
                elif parsed.path == '/api/status':
                    counts = {r[0]: r[1] for r in conn.execute('SELECT kind,COUNT(*) FROM items GROUP BY kind')}
                    reviewed = dict(conn.execute('SELECT status,COUNT(*) FROM reviews GROUP BY status').fetchall())
                    self.send(200, {'counts': counts, 'reviewed': reviewed, 'stats': meta['stats'], 'workspace': meta['workspace'],
                                    'agreement_sample': meta['agreement_sample'], 'capture_date': meta.get('capture_date'),
                                    'update_vireo_tags': update_vireo_tags,
                                    'tag_updates_pending': conn.execute("SELECT COUNT(*) FROM tag_sync WHERE status='pending'").fetchone()[0]})
                elif parsed.path == '/api/items':
                    kind = query.get('kind', [''])[0]
                    if kind and kind not in KINDS:
                        raise ValueError('Unknown review category')
                    status = query.get('status', ['pending'])[0]
                    if status not in {'pending', 'reviewed', 'all'}:
                        raise ValueError('Unknown review status')
                    where, params = ["i.kind != 'identity'" if not kind else 'i.kind=?'], [kind] if kind else []
                    if status == 'pending':
                        where.append("(r.photo_id IS NULL OR EXISTS (SELECT 1 FROM tag_sync t WHERE t.photo_id=i.photo_id AND t.status='pending'))")
                    elif status == 'reviewed':
                        where.append('r.photo_id IS NOT NULL')
                    offset = max(0, int(query.get('offset', ['0'])[0]))
                    sql = ' FROM items i LEFT JOIN reviews r ON r.photo_id=i.photo_id WHERE ' + ' AND '.join(where)
                    count = conn.execute('SELECT COUNT(*)' + sql, params).fetchone()[0]
                    rows = [dict(r) for r in conn.execute('SELECT i.*,r.status' + sql + ' ORDER BY i.position LIMIT 30 OFFSET ?', [*params, offset])]
                    self.send(200, {'total': count, 'items': rows})
                elif parsed.path.startswith('/api/photo/'):
                    self.send(200, photo_payload(conn, int(parsed.path.rsplit('/', 1)[-1])))
                elif parsed.path == '/api/species':
                    term = query.get('q', [''])[0].strip()
                    if len(term) < 2 or len(term) > 120:
                        self.send(200, [])
                        return
                    # Escape LIKE wildcards so typing a name never becomes a pattern.
                    escaped = term.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
                    rows = conn.execute("SELECT * FROM species WHERE name LIKE ? ESCAPE '\\' OR scientific LIKE ? ESCAPE '\\' ORDER BY length(name),name LIMIT 30", (escaped+'%', escaped+'%')).fetchall()
                    if not rows:
                        rows = conn.execute("SELECT * FROM species WHERE name LIKE ? ESCAPE '\\' OR scientific LIKE ? ESCAPE '\\' ORDER BY length(name),name LIMIT 30", ('%'+escaped+'%', '%'+escaped+'%')).fetchall()
                    self.send(200, [dict(r) for r in rows])
                elif parsed.path.startswith('/image/'):
                    path = image_path(conn, int(parsed.path.rsplit('/', 1)[-1]), media_root, query.get('large') == ['1'])
                    if path:
                        self.send(200, path.read_bytes(), 'image/jpeg')
                    else:
                        self.send(404, {'error': 'No matching cached preview is available'})
                else:
                    self.send(404, {'error': 'Not found'})
            except (ValueError, sqlite3.Error, OSError) as exc:
                self.send(400, {'error': str(exc)})
            finally:
                conn.close()

        def do_POST(self):
            parsed = urlparse(self.path)
            origin = self.headers.get('Origin')
            if (not self.authorized(parse_qs(parsed.query)) or
                    (origin and origin != f'http://127.0.0.1:{self.server.server_port}')):
                self.send(403, {'error': 'Unauthorized review request'})
                return
            if parsed.path not in {'/api/review', '/api/retry-tags'}:
                self.send(404, {'error': 'Not found'})
                return
            conn = connect(queue)
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 16384 or self.headers.get('Content-Type') != 'application/json':
                    raise ValueError('Send a small JSON review')
                body = json.loads(self.rfile.read(length))
                if parsed.path == '/api/retry-tags':
                    if not update_vireo_tags:
                        raise ValueError('Restart with --update-vireo-tags to retry tag updates')
                    result = {'tag_sync': sync_review_tags(conn, int(body['photo_id']))}
                else:
                    result = save_review(conn, int(body['photo_id']), taxa=body['taxa'], complete=body['complete'],
                                         notes=body.get('notes', ''), revision=body.get('revision', 0),
                                         update_vireo_tags=update_vireo_tags)
                    if update_vireo_tags:
                        result['tag_sync'] = sync_review_tags(conn, int(body['photo_id']))
                self.send(200, result)
            except (ValueError, KeyError, TypeError, sqlite3.Error) as exc:
                self.send(400, {'error': str(exc)})
            finally:
                conn.close()

    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    server.review_url = f'http://127.0.0.1:{server.server_port}/?token={token}'
    return server


def main(argv=None):
    p = argparse.ArgumentParser(description='Build or open a local encounter disagreement review queue')
    sub = p.add_subparsers(dest='command', required=True)
    build = sub.add_parser('build')
    build.add_argument('--run', type=Path, required=True)
    build.add_argument('--output', type=Path, required=True)
    build.add_argument('--db', type=Path, default=Path.home()/'.vireo/vireo.db')
    build.add_argument('--agreement-sample', type=int, default=200)
    build.add_argument('--seed', type=int, default=42)
    serve = sub.add_parser('serve')
    serve.add_argument('--queue', type=Path, required=True)
    serve.add_argument('--port', type=int, default=0)
    serve.add_argument('--media-root', type=Path)
    serve.add_argument('--open', action='store_true')
    serve.add_argument('--update-vireo-tags', action='store_true',
                       help='Also apply saved reviews to Vireo species tags and queue normal XMP sync')
    args = p.parse_args(argv)
    if args.command == 'build':
        print(json.dumps(build_queue(args.run, args.output, args.db, agreement_sample=args.agreement_sample, seed=args.seed), indent=2))
    else:
        server = make_server(args.queue, port=args.port, media_root=args.media_root,
                             update_vireo_tags=args.update_vireo_tags)
        print(server.review_url, flush=True)
        if args.open:
            webbrowser.open(server.review_url)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()


if __name__ == '__main__':
    main()
