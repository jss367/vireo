"""The provenance lattice of ``photo_keywords.source``, defined once.

Every ``photo_keywords`` writer that can land on an existing association
folds the two claims with these (``keyword_source_max`` in Python,
``keyword_source_max_sql`` / ``KEYWORD_SOURCE_CONFLICT_SQL`` in SQL), never
a local copy: ``test_keyword_provenance_contract`` requires each convergence
point to reference one of them. ``db`` re-exports every name here.
"""

# Durable provenance values for ``photo_keywords.source``.
#
# ``KEYWORD_SOURCE_MANUAL`` means "a person explicitly asked for this
# association"; nothing that prunes generated metadata may delete it.
# ``KEYWORD_SOURCE_UNKNOWN`` (NULL) means "we genuinely cannot tell" — the
# only writers allowed to use it are the sidecar readers (scanner, XMP
# reconcile), where a term may equally have come from the user's Lightroom
# catalog or from a keyword Vireo itself wrote out.
#
# ``tag_photo`` defaults to MANUAL on purpose. The failure mode of a
# mis-stamped association is asymmetric: guessing "manual" for a generated
# tag leaves a stale keyword the user can delete, while guessing "unknown"
# for a hand-added tag lets retirement passes erase user metadata silently
# and unrecoverably. So the default is the fail-safe value and every
# provenance-neutral writer has to say so explicitly. See
# ``test_keyword_provenance_contract.py``, which pins that inventory.
KEYWORD_SOURCE_MANUAL = "manual"
# Written by the prediction-accept path (PR #1479 Phase 4). Nothing in this
# module writes it yet; it is declared here so the lattice below has its
# middle element and both PRs share one ordering instead of two.
KEYWORD_SOURCE_ACCEPT = "accept"
KEYWORD_SOURCE_UNKNOWN = None

# Provenance is a lattice, weakest first: NULL < 'accept' < 'manual'.
#
# The rule the lattice exists to state once: **when two associations for the
# same (photo, keyword) converge, the survivor takes the stronger claim,
# never the weaker.** Associations converge in more places than they are
# created — a duplicate merge folds losers onto a winner, a keyword
# rename/curation-merge repoints rows onto a canonical keyword, RAW/JPEG
# companion pairing copies a companion's keywords onto the primary, and every
# ``tag_photo`` re-tag lands on a row that may already exist. Each of those is
# a place where a hand-added keyword can quietly decay into an unattributed
# row that a retirement pass reads as generated and deletes (the one-shot
# Wildlife genre retirement, since removed, worked that way).
# ``COALESCE(new, old)`` is not the rule — it is only accidentally
# equal to it while the column is set-or-NULL, and it downgrades 'manual' to
# 'accept' the moment a third value exists.
#
# So the fold is expressed once, here, and every convergence point uses it:
# ``keyword_source_max`` in Python, ``keyword_source_max_sql`` /
# ``KEYWORD_SOURCE_CONFLICT_SQL`` in SQL.
# ``test_keyword_provenance_contract.py`` enumerates the call sites and fails
# on a new one that does not.
KEYWORD_SOURCE_PRECEDENCE = (
    KEYWORD_SOURCE_UNKNOWN,
    KEYWORD_SOURCE_ACCEPT,
    KEYWORD_SOURCE_MANUAL,
)
_KEYWORD_SOURCE_RANK = {
    value: rank for rank, value in enumerate(KEYWORD_SOURCE_PRECEDENCE)
}
# A stamp this build does not recognise still means "somebody deliberately
# claimed this row", so it must outrank "no stamp at all" — but it never
# outranks an explicit 'manual', which is the one value with a delete-safety
# guarantee attached.
_KEYWORD_SOURCE_UNRECOGNIZED_RANK = 1


def keyword_source_rank(source: str | None) -> int:
    """Position of ``source`` in the provenance lattice (higher = stronger)."""
    if source is None:
        return _KEYWORD_SOURCE_RANK[KEYWORD_SOURCE_UNKNOWN]
    return _KEYWORD_SOURCE_RANK.get(source, _KEYWORD_SOURCE_UNRECOGNIZED_RANK)


def keyword_source_max(*sources: str | None) -> str | None:
    """Return the strongest of ``sources``: the fold for converging rows.

    Ties keep the first argument, so callers pass the incoming/stronger
    candidate first when they want it to win a same-rank tie.
    """
    best = KEYWORD_SOURCE_UNKNOWN
    best_rank = keyword_source_rank(KEYWORD_SOURCE_UNKNOWN)
    for source in sources:
        rank = keyword_source_rank(source)
        if rank > best_rank:
            best, best_rank = source, rank
    return best


def keyword_source_rank_sql(expr: str) -> str:
    """SQL for ``keyword_source_rank(expr)``, from the same ordering."""
    whens = " ".join(
        f"WHEN {expr} = '{value}' THEN {rank}"
        for rank, value in enumerate(KEYWORD_SOURCE_PRECEDENCE)
        if value is not None
    )
    return (
        f"(CASE WHEN {expr} IS NULL THEN "
        f"{_KEYWORD_SOURCE_RANK[KEYWORD_SOURCE_UNKNOWN]} {whens} "
        f"ELSE {_KEYWORD_SOURCE_UNRECOGNIZED_RANK} END)"
    )


def keyword_source_max_sql(left: str, right: str) -> str:
    """SQL for ``keyword_source_max(left, right)``.

    ``left`` and ``right`` are SQL expressions and are each substituted twice
    (once to rank, once to yield the value), so pass column references,
    literals, or scalar subqueries bound with *named* parameters — positional
    ``?`` would have to be supplied twice in an order the caller cannot see.
    """
    return (
        f"(CASE WHEN {keyword_source_rank_sql(left)} >= "
        f"{keyword_source_rank_sql(right)} THEN {left} ELSE {right} END)"
    )


# The upsert clause every ``INSERT INTO photo_keywords`` must end with: an
# existing row keeps its provenance unless the incoming row's is stronger,
# and keeps its species_provenance unless it has none. Rows that INSERT only
# three columns leave ``excluded.species_provenance`` NULL, so COALESCE
# leaves the surviving row's evidence intact; merge paths that INSERT four
# columns carry the loser's evidence onto the survivor when the survivor had
# none, matching the ``_fold_history_state`` rule that keeps the survivor's
# own stamp over the incoming one.
KEYWORD_SOURCE_CONFLICT_SQL = (
    "ON CONFLICT(photo_id, keyword_id) DO UPDATE SET "
    "source = "
    + keyword_source_max_sql("excluded.source", "photo_keywords.source")
    + ", species_provenance = COALESCE("
    "photo_keywords.species_provenance, excluded.species_provenance)"
)
