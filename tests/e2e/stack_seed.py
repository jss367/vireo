"""Seed photos that Browse will actually collapse into one stack.

Browse stopped keying stacks on ``photos.burst_id`` in PR #1659: that column
holds EXIF ImageUniqueID, which most cameras never write and the rest reuse
for the life of the body. A stack is now a run of consecutive frames from one
folder whose capture times are no further apart than
``browse_stack_time_gap`` seconds and which carry the same species and
location keywords, so a test that wants a stack has to seed those two
properties instead of a shared id.
"""

import datetime

# One second between frames: well inside the 3s default browse_stack_time_gap,
# and ascending, so the run reads in the order the caller listed the photos.
STACK_STEP_SECONDS = 1

# The keyword folds that make up a burst's identity (``_STACK_KEYWORD_SET_CTES``
# in db.py): species/taxonomy and location. A general keyword is deliberately
# not part of it, so a test can still single one member out with one.
_STACK_KEYWORDS_SQL = """
    SELECT DISTINCT pk.keyword_id
    FROM photo_keywords pk
    JOIN keywords k ON k.id = pk.keyword_id
    WHERE pk.photo_id IN ({placeholders})
      AND (k.type IN ('taxonomy', 'location') OR k.is_species = 1)
"""


def seed_browse_stack(db, photo_ids, start=None):
    """Make ``photo_ids`` collapse into a single Browse stack.

    Writes consecutive capture times one second apart, in the order given,
    and spreads the union of the members' species and location keywords over
    all of them — the two signals the grid's burst-run query keys on. By
    default the run starts at the earliest capture time the photos already
    have, so the stack keeps its place among the rest of the folder.

    Returns the timestamps written, oldest first.
    """
    ids = list(photo_ids)
    if len(ids) < 2:
        raise ValueError("a stack needs at least two photos")
    placeholders = ",".join("?" * len(ids))
    rows = db.conn.execute(
        f"SELECT id, folder_id, timestamp FROM photos WHERE id IN ({placeholders})",
        ids,
    ).fetchall()
    if len(rows) != len(ids):
        raise ValueError(f"no such photos: {sorted(set(ids) - {r['id'] for r in rows})}")
    folders = {row["folder_id"] for row in rows}
    if len(folders) != 1:
        raise ValueError(f"a burst never spans folders, got {sorted(folders)}")

    base = datetime.datetime.fromisoformat(
        start or min(row["timestamp"] for row in rows)
    )
    timestamps = [
        (base + datetime.timedelta(seconds=i * STACK_STEP_SECONDS)).isoformat()
        for i in range(len(ids))
    ]
    keyword_ids = [
        row[0]
        for row in db.conn.execute(
            _STACK_KEYWORDS_SQL.format(placeholders=placeholders), ids
        )
    ]
    with db.conn:
        for photo_id, timestamp in zip(ids, timestamps, strict=True):
            db.conn.execute(
                "UPDATE photos SET timestamp = ? WHERE id = ?", (timestamp, photo_id)
            )
            for keyword_id in keyword_ids:
                db.tag_photo(photo_id, keyword_id, _commit=False)
    return timestamps
