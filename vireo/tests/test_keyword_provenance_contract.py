"""Structural guard for the ``photo_keywords.source`` provenance contract.

A retirement pass deletes keyword associations it reads as generated (the
one-shot Wildlife genre retirement, since removed, was the first). The only
thing standing between a hand-added keyword and that delete is
``photo_keywords.source = 'manual'``, so the contract cannot rest on every
future author remembering to stamp it.

Three mechanisms enforce it, and this module pins all three:

1. ``services.keyword_tagging.tag_photo``, the entry point every caller
   outside the data layer tags through, defaults ``source`` to
   ``'manual'``. The failure
   modes are asymmetric — a wrongly-manual generated tag is a stale keyword
   the user can delete, while a wrongly-unknown manual tag is silent,
   unrecoverable data loss — so the default is the fail-safe one and a call
   site that forgets provenance degrades to "do not delete this".
2. Declining manual authorship is therefore an explicit, reviewable act. Only
   the sidecar readers may do it, and the allowlist below is the whole
   inventory. Adding a new provenance-neutral writer fails this test.
3. Provenance is created in few places but *converges* in many: a duplicate
   merge folds losers onto a winner, a keyword rename/curation-merge repoints
   rows onto a canonical keyword, companion pairing copies a RAW's keywords
   onto its JPEG, and every re-tag lands on a row that may already exist.
   Each is a place where the surviving row can silently take the weaker of
   two claims. The fold rule — lattice max, never a downgrade — lives once in
   ``repositories/keyword_source.py`` (``keyword_source_max`` /
   ``keyword_source_max_sql`` / ``KEYWORD_SOURCE_CONFLICT_SQL``, re-exported
   by ``db``), and the convergence guard below requires every site that
   writes a ``photo_keywords`` row to be a listed, reviewed site that uses
   it. The writers live in ``repositories/keyword_provenance.py``; the
   guards below scan every production module and accept a fold symbol
   either as a bare name or as an attribute, so the contract holds wherever
   a writer lives.

The source scan reads every call of the entry point (``tag_photo(db,
photo_id, keyword_id, source)``) and every call of the repository writer
behind it (``<...>.keyword_provenance.tag(..., source=...)``,
``KeywordProvenanceRepository(...).tag(...)``, a local bound to either, and
``self.tag(...)`` inside the provenance repository), so a writer that skips
the entry point still has to declare its provenance.

Sites are keyed by ``(module relative to vireo/ in POSIX form, enclosing
function)``, so a writer that moves between modules has to be re-listed here.
"""

import ast
import inspect
import re
from pathlib import Path

import pytest
from services import keyword_tagging, photo_ingest, workspace_folders

VIREO_DIR = Path(__file__).resolve().parent.parent

# (module relative to vireo/, enclosing function) → why this writer cannot
# claim manual authorship. A sidecar term is genuinely ambiguous: the user may
# have typed it in Lightroom, or Vireo may have written it out itself.
PROVENANCE_NEUTRAL_WRITERS = {
    ("sync.py", "sync_from_xmp"): "XMP reconcile cannot tell user terms from Vireo's own",
    ("scanner/keywords.py", "_import_keyword_lists"): "scanned sidecar and embedded terms have ambiguous authorship",
}

# Writers that neither claim nor decline authorship: they move an association
# between rows and pass through whatever provenance it already carried. The
# value is computed, so this test can't read it statically — the allowlist is
# the review gate instead.
PROVENANCE_CARRYING_WRITERS = {
    ("repositories/moves_merge.py", "carry_keyword_associations_for_merge"): "forwards the winning source association's existing stamp",
    ("services/identity_alternatives.py", "replay_identity_edit"): "identity history restores original candidate association stamps",
    ("db.py", "restore_photo_location_state"): "history restores the original location stamps",
    ("db.py", "_apply_winner_loser_merge"): "duplicate merge carries the losers' stamps",
}

# The fold rule, named. A convergence point must reference one of these; none
# of them may be reimplemented locally, because a local copy is exactly how
# one site ends up with a different idea of which claim is stronger.
PROVENANCE_FOLD_SYMBOLS = {
    "keyword_source_max",
    "keyword_source_max_sql",
    "KEYWORD_SOURCE_CONFLICT_SQL",
}

# Every production site that writes a ``photo_keywords`` row — INSERT (which
# may hit an existing row) or UPDATE of its keyword_id/source (which moves a
# row onto one that may already exist). Each is a convergence point: two
# claims about the same (photo, keyword) meet and one survives.
#
# ``keyword_tagging.tag_photo`` and the merges reach their writes in
# ``KeywordProvenanceRepository`` under the names keyed here
# (``link_keyword_to_place``'s in its ``_absorb_into_canonical_place``
# helper).
_PROVENANCE_MODULE = "repositories/keyword_provenance.py"
PROVENANCE_CONVERGENCE_POINTS = {
    ("repositories/identity_alternatives.py", "remap_identity_for_photo_merge"): "identity collision carries candidate stamps through the shared fold",
    (_PROVENANCE_MODULE, "tag"): "re-tag folds against the row already there",
    (_PROVENANCE_MODULE, "merge_keyword_into"): "keyword merge repoints rows onto dst",
    (_PROVENANCE_MODULE, "_absorb_into_canonical_place"): "place link repoints rows onto canonical",
    (_PROVENANCE_MODULE, "carry_companion_keywords"): "pairing copies companion keywords",
    ("repositories/folders.py", "carry_merged_photo_keywords"): "folder merge carries loser's keywords onto the survivor",
    ("repositories/moves_merge.py", "_carry_embedded_keyword_associations"): "staged merge preserves embedded association stamps",
}


def _production_modules():
    """Every shipped .py under vireo/, excluding tests and test helpers."""
    for path in sorted(VIREO_DIR.rglob("*.py")):
        rel = path.relative_to(VIREO_DIR)
        if rel.parts[0] in {"tests", "testing"}:
            continue
        yield rel, path


def _enclosing_function_name(tree, target):
    """Return the innermost function enclosing ``target``, or '<module>'."""
    name = "<module>"
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for child in ast.walk(node):
            if child is target:
                # Deeper matches overwrite shallower ones because ast.walk
                # visits outer functions first.
                name = node.name
    return name


_PROVENANCE_REPOSITORY_CLASS = "KeywordProvenanceRepository"

# The entry point forwards its caller's declared ``source`` to the repository
# writer; ``test_tag_photo_defaults_to_manual_provenance`` pins its default.
_ENTRY_POINT = ("services/keyword_tagging.py", "tag_photo")


def _builds_provenance_repository(node):
    """``<...>.keyword_provenance`` or ``KeywordProvenanceRepository(...)``."""
    if isinstance(node, ast.Attribute) and node.attr == "keyword_provenance":
        return True
    return (
        isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id == _PROVENANCE_REPOSITORY_CLASS
    )


def _writer_calls(rel, tree):
    """Yield ``(call, source_node)`` for every association write in ``tree``.

    The entry point ``tag_photo(db, photo_id, keyword_id, source)`` (bare or
    as ``<module>.tag_photo``) declares ``source`` by keyword or as its
    fourth positional argument. The repository writer ``tag`` takes it
    keyword-only, through any receiver that is a provenance repository.
    """
    aliases = {
        target.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and _builds_provenance_repository(node.value)
        for target in node.targets if isinstance(target, ast.Name)
    }
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        keyword = next((kw.value for kw in node.keywords if kw.arg == "source"), None)
        is_entry = (
            (isinstance(func, ast.Attribute) and func.attr == "tag_photo")
            or (isinstance(func, ast.Name) and func.id == "tag_photo")
        )
        if is_entry:
            if keyword is None and len(node.args) >= 4:
                keyword = node.args[3]
            yield node, keyword
            continue
        if not (isinstance(func, ast.Attribute) and func.attr == "tag"):
            continue
        receiver = func.value
        on_repository = (
            _builds_provenance_repository(receiver)
            or (isinstance(receiver, ast.Name) and receiver.id in aliases)
            or (rel == _PROVENANCE_MODULE
                and isinstance(receiver, ast.Name) and receiver.id == "self")
        )
        if on_repository:
            yield node, keyword if keyword is not None else _REQUIRED_SOURCE


# A repository-writer call with no ``source=``: the writer has no default, so
# the call cannot even run; reported as unrecognized.
_REQUIRED_SOURCE = ast.Constant(value=object())


def _tag_photo_calls():
    """Yield (relpath, function, lineno, source_node_or_None) for every write."""
    for rel, path in _production_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node, source in _writer_calls(rel.as_posix(), tree):
            function = _enclosing_function_name(tree, node)
            if (rel.as_posix(), function) == _ENTRY_POINT:
                continue
            yield rel.as_posix(), function, node.lineno, source


def _declared_source(node):
    """Resolve a ``source=`` argument to 'manual', None, or 'unrecognized'."""
    if node is None:
        return "<omitted>"
    if isinstance(node, ast.Constant):
        if not isinstance(node.value, (str, type(None))):
            return "<unrecognized>"
        return node.value
    if isinstance(node, ast.Name):
        return {
            "KEYWORD_SOURCE_MANUAL": "manual",
            "KEYWORD_SOURCE_UNKNOWN": None,
        }.get(node.id, "<unrecognized>")
    if isinstance(node, ast.Attribute):
        return {
            "KEYWORD_SOURCE_MANUAL": "manual",
            "KEYWORD_SOURCE_UNKNOWN": None,
        }.get(node.attr, "<unrecognized>")
    return "<unrecognized>"


def test_tag_photo_defaults_to_manual_provenance():
    """The unstamped path must degrade to 'never delete this', not the reverse."""
    from db import KEYWORD_SOURCE_MANUAL, KEYWORD_SOURCE_UNKNOWN, Database
    from repositories.keyword_provenance import KeywordProvenanceRepository
    from services import keyword_tagging

    assert KEYWORD_SOURCE_MANUAL == "manual"
    assert KEYWORD_SOURCE_UNKNOWN is None
    # One entry point, outside the data layer; the repository writer behind
    # it has no default of its own to fall back on.
    assert not hasattr(Database, "tag_photo")
    writer = inspect.signature(KeywordProvenanceRepository.tag).parameters["source"]
    assert writer.default is inspect.Parameter.empty
    assert writer.kind is inspect.Parameter.KEYWORD_ONLY

    sig = inspect.signature(keyword_tagging.tag_photo)
    assert sig.parameters["source"].default == KEYWORD_SOURCE_MANUAL, (
        "tag_photo's source default is the fail-safe for every call site that "
        "forgets to declare provenance; it must stay 'manual'."
    )


def test_only_sidecar_readers_decline_manual_provenance():
    """A new provenance-neutral writer call site must be a deliberate act."""
    neutral = set()
    carrying = set()
    for rel, func, lineno, node in _tag_photo_calls():
        declared = _declared_source(node)
        if declared == "<unrecognized>":
            assert (rel, func) in PROVENANCE_CARRYING_WRITERS, (
                f"{rel}:{lineno} ({func}) passes a source= value this contract "
                f"cannot read statically. Use KEYWORD_SOURCE_MANUAL or "
                f"KEYWORD_SOURCE_UNKNOWN, or — if it forwards an existing "
                f"row's stamp — add it to PROVENANCE_CARRYING_WRITERS."
            )
            carrying.add((rel, func))
            continue
        assert declared in ("manual", None, "<omitted>"), (
            f"{rel}:{lineno} ({func}) passes source={declared!r}, which is not "
            f"a known provenance value."
        )
        if declared is None:
            neutral.add((rel, func))

    assert carrying == set(PROVENANCE_CARRYING_WRITERS), (
        "A listed provenance-carrying writer no longer computes its source. "
        "Drop it from PROVENANCE_CARRYING_WRITERS so the static check applies "
        f"again.\n  found:    {sorted(carrying)}\n"
        f"  expected: {sorted(PROVENANCE_CARRYING_WRITERS)}"
    )

    assert neutral == set(PROVENANCE_NEUTRAL_WRITERS), (
        "Provenance-neutral tagging is limited to the sidecar readers. A "
        "writer that leaves source NULL produces associations that "
        "a retirement pass reads as generated and deletes. If "
        "the new site really cannot know authorship, add it to "
        "PROVENANCE_NEUTRAL_WRITERS with a reason; otherwise let it take the "
        "manual default.\n"
        f"  found:    {sorted(neutral)}\n"
        f"  expected: {sorted(PROVENANCE_NEUTRAL_WRITERS)}"
    )


def test_raw_photo_keywords_inserts_carry_a_source_column():
    """Bypassing tag_photo() must not bypass the provenance stamp."""
    offenders = []
    for rel, path in _production_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            lowered = node.value.lower()
            # Check the INSERT's own column list, not the whole statement — an
            # ``ON CONFLICT ... SET source = ...`` clause in the same literal
            # would otherwise mask a column list that omits it.
            for columns in re.findall(
                r"into\s+photo_keywords\s*\(([^)]*)\)", lowered,
            ):
                if "source" not in columns:
                    offenders.append(f"{rel}:{node.lineno}")
    assert not offenders, (
        "These raw INSERTs write photo_keywords rows without a source column, "
        "so the associations land as NULL ('unknown') no matter who asked for "
        "them. Route them through tag_photo() or carry the source explicitly: "
        + ", ".join(offenders)
    )


_PHOTO_KEYWORDS_WRITE_PATTERNS = (
    re.compile(r"insert\s+(or\s+\w+\s+)?into\s+photo_keywords"),
    re.compile(r"update\s+(or\s+\w+\s+)?photo_keywords\b"),
)


def _sql_texts(node):
    """Yield SQL-ish string text under ``node``, f-strings included.

    An f-string's interpolations are replaced with a placeholder so the
    statement keywords around them still match — several of these statements
    build their SET clause by calling the shared fold helper.
    """
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            yield child.lineno, child.value
        elif isinstance(child, ast.JoinedStr):
            parts = [
                piece.value if isinstance(piece, ast.Constant) else " <expr> "
                for piece in child.values
            ]
            yield child.lineno, "".join(
                part for part in parts if isinstance(part, str)
            )


def _references_fold_helper(node):
    """True if ``node``'s subtree names one of the shared fold symbols."""
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and child.id in PROVENANCE_FOLD_SYMBOLS:
            return True
        if isinstance(child, ast.Attribute) and child.attr in PROVENANCE_FOLD_SYMBOLS:
            return True
    return False


def _production_functions():
    """Yield (relpath, function name, ast node) for every production function."""
    for rel, path in _production_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                yield rel.as_posix(), node.name, node


# These updates never create or converge associations. Evidence editing leaves
# keyword authorship untouched; undo restores the exact historical stamp (even
# unknown), rather than upgrading it to manual. Pin the complete SQL so this
# exception cannot admit an INSERT or a future association move.
SPECIES_EVIDENCE_UPDATES = {
    ("repositories/species_provenance.py", "set"):
        "update photo_keywords set species_provenance=? where photo_id=? and keyword_id=?",
    ("repositories/species_provenance.py", "restore"):
        "update photo_keywords set source=?, species_provenance=? where photo_id=? and keyword_id=?",
}


def test_photo_keywords_writers_use_the_shared_provenance_fold():
    """A new convergence point must be listed, and must use the shared fold.

    This is the structural half of the "no write moves down the lattice"
    rule. The reviewed cases were each found the same way — a path that moved
    or collapsed an association and decided its provenance locally — so the
    guard is not "did you remember to fold" (nobody remembers) but "is this
    site one of the ones we looked at, and does it call the one fold".
    """
    found = {}
    evidence_updates = set()
    unfolded = []
    for rel, name, node in _production_functions():
        for lineno, text in _sql_texts(node):
            lowered = text.lower()
            if not any(p.search(lowered) for p in _PHOTO_KEYWORDS_WRITE_PATTERNS):
                continue
            point = (rel, name)
            if point in SPECIES_EVIDENCE_UPDATES:
                assert " ".join(lowered.split()) == SPECIES_EVIDENCE_UPDATES[point]
                evidence_updates.add(point)
                continue
            found[point] = lineno
            if not _references_fold_helper(node):
                unfolded.append(f"{rel}:{lineno} ({name})")

    assert evidence_updates == set(SPECIES_EVIDENCE_UPDATES)
    assert not unfolded, (
        "These functions write photo_keywords rows without referencing the "
        "shared provenance fold (" + ", ".join(sorted(PROVENANCE_FOLD_SYMBOLS))
        + "). A row that lands on an existing association and keeps the "
        "weaker of the two stamps is how a hand-added keyword becomes one "
        "that retirement deletes: " + ", ".join(sorted(set(unfolded)))
    )
    assert set(found) == set(PROVENANCE_CONVERGENCE_POINTS), (
        "The set of places where two claims about the same (photo, keyword) "
        "can meet has changed. Add the new site to "
        "PROVENANCE_CONVERGENCE_POINTS with a one-line reason once you have "
        "checked it folds provenance instead of picking a side.\n"
        f"  found:    {sorted(found)}\n"
        f"  expected: {sorted(PROVENANCE_CONVERGENCE_POINTS)}"
    )


def test_provenance_carrying_writers_use_the_shared_fold():
    """The Python-side folds must not reimplement the ordering either."""
    for rel, name, node in _production_functions():
        if (rel, name) not in PROVENANCE_CARRYING_WRITERS:
            continue
        assert _references_fold_helper(node), (
            f"{rel} ({name}) computes an association's provenance without "
            f"the shared fold. Use keyword_source_max() so it cannot "
            f"disagree with the SQL paths about which claim is stronger."
        )


def test_no_sql_folds_provenance_with_coalesce():
    """COALESCE is not the fold — it downgrades as soon as 'accept' exists."""
    offenders = []
    for rel, path in _production_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for lineno, text in _sql_texts(tree):
            if re.search(r"source\s*=\s*coalesce\s*\(", text.lower()):
                offenders.append(f"{rel}:{lineno}")
    assert not offenders, (
        "COALESCE(new, old) only looks like the provenance fold while the "
        "column is set-or-NULL: with a third value it silently rewrites "
        "'manual' to the weaker incoming stamp. Use keyword_source_max_sql() "
        "or KEYWORD_SOURCE_CONFLICT_SQL: " + ", ".join(offenders)
    )


def test_provenance_lattice_orders_manual_above_accept_above_unknown():
    """The ordering itself, pinned — PR #1479 Phase 4 writes to this lattice."""
    from db import (
        KEYWORD_SOURCE_ACCEPT,
        KEYWORD_SOURCE_MANUAL,
        KEYWORD_SOURCE_PRECEDENCE,
        KEYWORD_SOURCE_UNKNOWN,
        keyword_source_max,
        keyword_source_rank,
    )

    assert KEYWORD_SOURCE_PRECEDENCE == (
        KEYWORD_SOURCE_UNKNOWN,
        KEYWORD_SOURCE_ACCEPT,
        KEYWORD_SOURCE_MANUAL,
    )
    assert (
        keyword_source_rank(KEYWORD_SOURCE_MANUAL)
        > keyword_source_rank(KEYWORD_SOURCE_ACCEPT)
        > keyword_source_rank(KEYWORD_SOURCE_UNKNOWN)
    )
    # The fold, in both argument orders — convergence has no natural side.
    for a, b in (
        (KEYWORD_SOURCE_MANUAL, KEYWORD_SOURCE_ACCEPT),
        (KEYWORD_SOURCE_MANUAL, KEYWORD_SOURCE_UNKNOWN),
    ):
        assert keyword_source_max(a, b) == a
        assert keyword_source_max(b, a) == a
    assert keyword_source_max(KEYWORD_SOURCE_ACCEPT, KEYWORD_SOURCE_UNKNOWN) == (
        KEYWORD_SOURCE_ACCEPT
    )
    assert keyword_source_max() is KEYWORD_SOURCE_UNKNOWN
    # A stamp from a newer schema outranks "no stamp", never 'manual'.
    assert keyword_source_max("from-the-future", KEYWORD_SOURCE_UNKNOWN) == (
        "from-the-future"
    )
    assert keyword_source_max("from-the-future", KEYWORD_SOURCE_MANUAL) == (
        KEYWORD_SOURCE_MANUAL
    )


@pytest.fixture()
def prov_db(tmp_path):
    from db import Database

    db = Database(str(tmp_path / "prov.db"))
    try:
        yield db
    finally:
        db.close()


def test_unstamped_tag_photo_is_recorded_as_manual(prov_db):
    """A call site that forgets provenance still produces a protected row."""
    fid = workspace_folders.add_folder(prov_db, "/photos", name="photos")
    pid = photo_ingest.add_photo(
        prov_db, folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=10, file_mtime=1.0,
    )
    kid = prov_db.keywords.add("Wildlife", kw_type="genre")

    keyword_tagging.tag_photo(prov_db, pid, kid)
    prov_db.commit()

    row = prov_db.conn.execute(
        "SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (pid, kid),
    ).fetchone()
    assert row["source"] == "manual"


def test_sidecar_import_leaves_provenance_unknown(prov_db):
    """The allowlisted neutral writers really do write NULL, not 'manual'."""
    from db import KEYWORD_SOURCE_UNKNOWN

    fid = workspace_folders.add_folder(prov_db, "/photos", name="photos")
    pid = photo_ingest.add_photo(
        prov_db, folder_id=fid, filename="b.jpg", extension=".jpg",
        file_size=10, file_mtime=1.0,
    )
    kid = prov_db.keywords.add("Wildlife", kw_type="genre")

    keyword_tagging.tag_photo(prov_db, pid, kid, source=KEYWORD_SOURCE_UNKNOWN)
    prov_db.commit()

    row = prov_db.conn.execute(
        "SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (pid, kid),
    ).fetchone()
    assert row["source"] is None


def test_duplicate_merge_carries_manual_provenance_to_the_winner(prov_db):
    """Moving an association between photo rows must move its authorship too."""
    from db import KEYWORD_SOURCE_UNKNOWN

    fid = workspace_folders.add_folder(prov_db, "/photos", name="photos")
    winner = photo_ingest.add_photo(
        prov_db, folder_id=fid, filename="w.jpg", extension=".jpg",
        file_size=10, file_mtime=1.0,
    )
    loser = photo_ingest.add_photo(
        prov_db, folder_id=fid, filename="l.jpg", extension=".jpg",
        file_size=10, file_mtime=1.0,
    )
    hand_added = prov_db.keywords.add("Backlit", kw_type="general")
    scanned = prov_db.keywords.add("Imported", kw_type="general")
    shared = prov_db.keywords.add("Shared", kw_type="general")
    owned = prov_db.keywords.add("Owned", kw_type="general")
    keyword_tagging.tag_photo(prov_db, winner, shared, source=KEYWORD_SOURCE_UNKNOWN)
    keyword_tagging.tag_photo(prov_db, winner, owned)  # manual
    keyword_tagging.tag_photo(prov_db, loser, hand_added)
    keyword_tagging.tag_photo(prov_db, loser, scanned, source=KEYWORD_SOURCE_UNKNOWN)
    keyword_tagging.tag_photo(prov_db, loser, shared)
    keyword_tagging.tag_photo(prov_db, loser, owned, source=KEYWORD_SOURCE_UNKNOWN)
    prov_db.commit()

    prov_db._apply_winner_loser_merge(winner, [loser])

    stamps = {
        row["keyword_id"]: row["source"]
        for row in prov_db.conn.execute(
            "SELECT keyword_id, source FROM photo_keywords WHERE photo_id = ?",
            (winner,),
        ).fetchall()
    }
    assert stamps[hand_added] == "manual", (
        "A duplicate merge must not launder a hand-added keyword into an "
        "unattributed association that retirement reads as generated."
    )
    assert stamps[scanned] is None
    assert stamps[shared] == "manual", (
        "A manual loser must upgrade an overlapping unknown association on "
        "the winner, even though the keyword does not need to be added."
    )
    assert stamps[owned] == "manual", (
        "The fold runs one way only. Now that every loser keyword is "
        "re-tagged, an unattributed loser must not pull the winner's own "
        "hand-added stamp back down to unknown."
    )


def test_keyword_merge_folds_provenance_on_overlapping_photos(prov_db):
    """Collapsing two keywords onto one must keep the stronger claim.

    ``UPDATE OR IGNORE`` leaves the destination row untouched when a photo
    carries both keywords, and the source row is then deleted — so without a
    fold, whichever keyword happened to be the merge destination decides the
    surviving provenance.
    """
    from db import KEYWORD_SOURCE_ACCEPT, KEYWORD_SOURCE_UNKNOWN

    fid = workspace_folders.add_folder(prov_db, "/photos", name="photos")

    def _photo(name):
        return photo_ingest.add_photo(
            prov_db, folder_id=fid, filename=name, extension=".jpg",
            file_size=10, file_mtime=1.0,
        )

    # Each photo carries both keywords, differing only in which side is
    # stronger. The merge collapses them onto ``dst``.
    weak_dst = _photo("weak_dst.jpg")
    strong_dst = _photo("strong_dst.jpg")
    mid_dst = _photo("mid_dst.jpg")
    src = prov_db.keywords.add("Heron", kw_type="general")
    dst = prov_db.keywords.add("Grey Heron", kw_type="general")

    keyword_tagging.tag_photo(prov_db, weak_dst, dst, source=KEYWORD_SOURCE_UNKNOWN)
    keyword_tagging.tag_photo(prov_db, weak_dst, src)  # manual
    keyword_tagging.tag_photo(prov_db, strong_dst, dst)  # manual
    keyword_tagging.tag_photo(prov_db, strong_dst, src, source=KEYWORD_SOURCE_ACCEPT)
    keyword_tagging.tag_photo(prov_db, mid_dst, dst, source=KEYWORD_SOURCE_UNKNOWN)
    keyword_tagging.tag_photo(prov_db, mid_dst, src, source=KEYWORD_SOURCE_ACCEPT)
    prov_db.commit()

    prov_db.keyword_provenance.merge_keyword_into(src, dst)

    stamps = {
        row["photo_id"]: row["source"]
        for row in prov_db.conn.execute(
            "SELECT photo_id, source FROM photo_keywords WHERE keyword_id = ?",
            (dst,),
        ).fetchall()
    }
    assert stamps[weak_dst] == "manual", (
        "The user's hand-added 'Heron' must survive being merged into an "
        "unattributed 'Grey Heron', or retirement reads the result as "
        "generated and deletes it."
    )
    assert stamps[strong_dst] == "manual", (
        "The fold must not run downhill either: a weaker source may not "
        "overwrite a manual destination."
    )
    assert stamps[mid_dst] == KEYWORD_SOURCE_ACCEPT


def test_tag_photo_never_moves_provenance_down_the_lattice(prov_db):
    """Re-tagging stores the lattice max, in every direction."""
    from db import (
        KEYWORD_SOURCE_ACCEPT,
        KEYWORD_SOURCE_MANUAL,
        KEYWORD_SOURCE_UNKNOWN,
    )

    fid = workspace_folders.add_folder(prov_db, "/photos", name="photos")
    kid = prov_db.keywords.add("Wildlife", kw_type="genre")
    prov_db.commit()

    def _stamp(first, second):
        pid = photo_ingest.add_photo(
            prov_db, folder_id=fid, filename=f"{first}-{second}.jpg", extension=".jpg",
            file_size=10, file_mtime=1.0,
        )
        keyword_tagging.tag_photo(prov_db, pid, kid, source=first)
        keyword_tagging.tag_photo(prov_db, pid, kid, source=second)
        prov_db.commit()
        return prov_db.conn.execute(
            "SELECT source FROM photo_keywords "
            "WHERE photo_id = ? AND keyword_id = ?",
            (pid, kid),
        ).fetchone()["source"]

    assert _stamp(KEYWORD_SOURCE_MANUAL, KEYWORD_SOURCE_ACCEPT) == "manual"
    assert _stamp(KEYWORD_SOURCE_MANUAL, KEYWORD_SOURCE_UNKNOWN) == "manual"
    assert _stamp(KEYWORD_SOURCE_ACCEPT, KEYWORD_SOURCE_UNKNOWN) == "accept"
    assert _stamp(KEYWORD_SOURCE_ACCEPT, KEYWORD_SOURCE_MANUAL) == "manual"
    assert _stamp(KEYWORD_SOURCE_UNKNOWN, KEYWORD_SOURCE_ACCEPT) == "accept"
    assert _stamp(KEYWORD_SOURCE_UNKNOWN, KEYWORD_SOURCE_MANUAL) == "manual"


def test_setting_a_photo_location_records_manual_provenance(prov_db):
    """Assigning a place is a person's explicit act, not generated metadata."""
    fid = workspace_folders.add_folder(prov_db, "/photos", name="photos")
    pid = photo_ingest.add_photo(
        prov_db, folder_id=fid, filename="loc.jpg", extension=".jpg",
        file_size=10, file_mtime=1.0,
    )
    leaf = prov_db.keywords.add("Yosemite", kw_type="location")
    prov_db.commit()

    prov_db.set_photo_location(pid, leaf)

    row = prov_db.conn.execute(
        "SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (pid, leaf),
    ).fetchone()
    assert row["source"] == "manual"


def test_manual_stamp_survives_a_later_neutral_retag(prov_db):
    """A sidecar rescan must not downgrade authorship the user established."""
    from db import KEYWORD_SOURCE_MANUAL, KEYWORD_SOURCE_UNKNOWN

    fid = workspace_folders.add_folder(prov_db, "/photos", name="photos")
    pid = photo_ingest.add_photo(
        prov_db, folder_id=fid, filename="c.jpg", extension=".jpg",
        file_size=10, file_mtime=1.0,
    )
    kid = prov_db.keywords.add("Wildlife", kw_type="genre")

    keyword_tagging.tag_photo(prov_db, pid, kid, source=KEYWORD_SOURCE_MANUAL)
    keyword_tagging.tag_photo(prov_db, pid, kid, source=KEYWORD_SOURCE_UNKNOWN)
    prov_db.commit()

    row = prov_db.conn.execute(
        "SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (pid, kid),
    ).fetchone()
    assert row["source"] == "manual"


def test_writer_call_scan_finds_every_way_to_reach_the_writer():
    """Pin the scan: the entry point and each route to the repository writer."""
    tree = ast.parse(
        "def write(db, conn, ws):\n"
        "    keyword_tagging.tag_photo(db, 1, 2)\n"
        "    keyword_tagging.tag_photo(db, 1, 2, None)\n"
        "    tag_photo(db, 1, 2, source='manual')\n"
        "    db.keyword_provenance.tag(1, 2, source=None)\n"
        "    KeywordProvenanceRepository(conn).tag(1, 2, source='accept')\n"
        "    repo = db.keyword_provenance\n"
        "    repo.tag(1, 2)\n"
        "    db.tags.tag(1)\n"
    )
    found = [
        (node.lineno, _declared_source(source))
        for node, source in _writer_calls("services/example.py", tree)
    ]
    assert found == [
        (2, "<omitted>"), (3, None), (4, "manual"), (5, None), (6, "accept"),
        (8, "<unrecognized>"),
    ]
    self_tree = ast.parse("def f(self):\n    self.tag(1, 2, source=None)\n")
    assert [_declared_source(src) for _, src in _writer_calls(_PROVENANCE_MODULE, self_tree)] == [None]
    assert list(_writer_calls("repositories/other.py", self_tree)) == []
