"""Canonical, idempotent DDL for photo-specific workspace visibility."""

def create_photo_visibility_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS workspace_photos (
        workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
        photo_id INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
        PRIMARY KEY (workspace_id, photo_id)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workspace_photos_photo ON workspace_photos(photo_id)")
    conn.execute("""CREATE VIEW IF NOT EXISTS photo_workspace_visibility AS
        SELECT p.id AS photo_id, p.folder_id, wf.workspace_id
        FROM photos p JOIN workspace_folders wf ON wf.folder_id = p.folder_id
        UNION
        SELECT p.id, p.folder_id, wp.workspace_id
        FROM photos p JOIN workspace_photos wp ON wp.photo_id = p.id
    """)
    conn.execute("""CREATE VIEW IF NOT EXISTS workspace_visible_folders AS
        SELECT folder_id, workspace_id, is_root FROM workspace_folders
        UNION
        SELECT DISTINCT p.folder_id, wp.workspace_id, 1 AS is_root
        FROM workspace_photos wp JOIN photos p ON p.id = wp.photo_id
        WHERE NOT EXISTS (SELECT 1 FROM workspace_folders wf
            WHERE wf.folder_id = p.folder_id AND wf.workspace_id = wp.workspace_id)
    """)

