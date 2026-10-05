"""Keyword normalization helpers.

These helpers are intentionally conservative: normalize comparison keys and
trim stray edge quote marks without removing meaningful punctuation inside a
keyword such as "Hawai'i" or "Smith's Longspur".
"""

import re
import unicodedata

# U+02BB..U+02BF (spacing modifier letters -- Hawaiian okina, Semitic
# hamza/ayin, Greek breathings, etc.) are intentionally NOT included:
# they are Unicode letters (category Lm) used inside legitimate keyword
# names such as species names starting with U+02BB (okina), so stripping
# them at the edges would rewrite the taxonomy rather than remove a stray
# quote.
_EDGE_QUOTES = (
    "\"'`"
    "´"
    "‘’‚‛"
    "“”„‟"
    "′″"
    "❛❜❝❞"
)

# ASCII-only lowercase table. SQLite's built-in ``LOWER()``/``COLLATE
# NOCASE`` only folds A-Z, leaving non-ASCII letters such as ``É`` alone.
# ``add_keyword()`` relies on that behavior, so ``keyword_match_key`` uses
# the same fold to avoid the dedupe/merge path folding distinct non-ASCII
# case pairs that the DB would treat as different keywords.
_ASCII_LOWER_TABLE = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "abcdefghijklmnopqrstuvwxyz",
)

# Typographic single quotes that are unambiguously PUNCTUATION (Unicode
# categories Pi/Pf/Po) fold to ASCII U+0027 so one species does not split
# into two keywords. macOS smart-quote substitution, pasted iNaturalist
# common names, and a handful of label-file lines all yield ``Say’s
# phoebe``; stored verbatim it is a distinct row from ``Say's phoebe``
# under SQLite ``COLLATE NOCASE``, which is how one bird ended up with
# two Life List cards and two lifer numbers.
#
# The fold happens in the DISPLAY/STORAGE form rather than only in
# ``keyword_match_key`` on purpose: ``add_keyword`` dedupes with SQL
# ``COLLATE NOCASE``, which cannot fold U+2019. Folding only the match key
# would let the merge pass collapse rows that the SQL layer still treats
# as distinct, so ``add_keyword`` would immediately re-insert the curly
# variant and the duplicate would grow back. Normalizing on write keeps
# the two layers in lockstep — see ``keyword_match_key``'s docstring.
#
# Deliberately EXCLUDED:
#   * U+02BB / U+02BC / U+02B9 — spacing modifier LETTERS (category Lm).
#     U+02BB is the Hawaiian okina in ``ʻApapane`` and ``Hawaiʻi
#     ʻamakihi``; folding it would rewrite the taxonomy.
#   * U+00B4 ACUTE ACCENT — preserved internally by
#     ``_nfkc_preserving_internal_acute`` for names like ``O´Brien``.
#   * U+2032 PRIME — the semantic prime symbol used for feet, arcminutes,
#     and similar measurements. ``normalize_keyword_display`` runs on every
#     keyword (not only species names), so a folded-in-place `10′ waterfall`
#     would silently rewrite existing DB and queued XMP values. Species
#     labels with a stray prime are rare enough that the case-collision
#     merger below can handle them; folding them here would break the
#     legitimate measurement case with no way for the user to opt out.
# Edge occurrences of the folded characters are already removed by
# ``_EDGE_QUOTES``, so in practice this only rewrites INTERNAL ones.
_APOSTROPHE_FOLD = {
    ord("‘"): "'",  # LEFT SINGLE QUOTATION MARK
    ord("’"): "'",  # RIGHT SINGLE QUOTATION MARK
    ord("‛"): "'",  # SINGLE HIGH-REVERSED-9 QUOTATION MARK
}


def _nfkc_preserving_internal_acute(value: str) -> str:
    """Apply NFKC while leaving internal U+00B4 ACUTE ACCENT intact.

    NFKC decomposes U+00B4 into space + combining acute (U+0020 U+0301),
    which corrupts internal punctuation in names like ``O´Brien``. A
    prior implementation reserved U+E000 as a temporary sentinel, but
    U+E000 is a valid Private Use Area code point that users may include
    in a keyword; a lone ```` would then round-trip as ``´``.
    Splitting the string at U+00B4 boundaries, NFKC-normalizing each
    segment, and rejoining with U+00B4 avoids any sentinel collision
    while preserving the acute wherever it survived edge stripping.
    """
    if "´" not in value:
        return unicodedata.normalize("NFKC", value)
    return "´".join(
        unicodedata.normalize("NFKC", seg) for seg in value.split("´")
    )


def normalize_keyword_display(name: str) -> str:
    """Return a cleaned display/storage form for a keyword name."""
    value = str(name or "")
    # Trim whitespace BEFORE stripping edge quotes so a leading/trailing
    # space doesn't shield an edge-quote character from the pre-NFKC
    # strip below. Without this, an imported XMP value like
    # ` ´apapane` (space + U+00B4 ACUTE ACCENT) leaves the acute in
    # place; NFKC then decomposes it to a leading combining mark
    # (U+0301) that is not in _EDGE_QUOTES, and the result is
    # `́apapane` -- a nearly invisible variant that no longer matches
    # `apapane`.
    value = value.strip()
    # Strip edge quotes BEFORE NFKC so characters that decompose into a
    # spacing char plus a combining mark (e.g. U+00B4 ACUTE ACCENT ->
    # U+0020 U+0301) get removed while they are still a single quote-ish
    # code point at the edge. Skipping this pre-strip lets `´apapane`
    # normalize to `́apapane`, an invisible variant that survives the
    # post-NFKC strip below because U+0301 is not in _EDGE_QUOTES.
    value = value.strip(_EDGE_QUOTES)
    # Preserve any internal U+00B4 across NFKC -- see helper for rationale
    # (segment-and-rejoin, no sentinel character that could collide with
    # legitimate input such as U+E000).
    value = _nfkc_preserving_internal_acute(value)
    value = "".join(" " if ch.isspace() else ch for ch in value)
    value = re.sub(r" +", " ", value).strip()
    value = value.strip(_EDGE_QUOTES)
    value = re.sub(r" +", " ", value).strip()
    # Fold internal typographic apostrophes LAST, after the edge strips have
    # had their chance to delete them outright. Doing it earlier would turn a
    # leading `’` into `'` and still strip it, but running last keeps the
    # edge-case behavior identical to before this fold existed.
    value = value.translate(_APOSTROPHE_FOLD)
    return value


def keyword_match_key(name: str) -> str:
    """Return the key used when comparing keyword names for equivalence.

    Applies an ASCII-only case fold that matches SQLite's built-in
    ``LOWER(name)``/``COLLATE NOCASE`` used by ``add_keyword()`` and the
    ``keywords`` table constraints. Python's ``str.lower()`` and
    ``str.casefold()`` are both more aggressive than SQLite's ASCII
    ``LOWER``: ``"Éclair".lower() == "éclair"`` and
    ``"Maße".casefold() == "masse"``, so grouping duplicates by either of
    those keys would let ``merge_duplicate_keywords()`` retag and delete
    one of two distinct keywords that the DB constraint layer treats as
    separate. Restricting the fold to A-Z leaves non-ASCII letters such
    as ``É`` / ``é`` and ``ß`` alone, keeping the equivalence class in
    lockstep with the SQL side.
    """
    return normalize_keyword_display(name).translate(_ASCII_LOWER_TABLE)


def folded_species_key(species):
    """Return the string ``Database.add_prediction`` keys ``species`` by.

    Mirrors the normalization branch in ``add_prediction``: fold when the
    result is non-empty, otherwise keep the original. Callers use this to
    dedupe alternatives against a primary before writing prediction_review
    rows, so the key must match exactly what ends up in the UNIQUE column.

    ``None`` passes through so callers can distinguish "no species" from a
    species that normalizes to the empty string.
    """
    if species is None:
        return None
    folded = normalize_keyword_display(species)
    return folded if folded else species


def species_match_key(species):
    """Return the equivalence key used to decide whether two species agree.

    This is the rule that decides whether a burst is unanimous. It differs
    from ``keyword_match_key`` only for names that normalize away entirely
    (``folded_species_key`` keeps the original in that case), and it is the
    fold that ``classify_job._store_grouped_predictions`` applies when it
    computes ``group_reviewable``.

    ``str.lower()``/``str.casefold()`` and SQLite's ``lower()`` are all
    wrong substitutes: the first two fold non-ASCII pairs SQLite treats as
    distinct, and the last does not apply the apostrophe/edge-quote folding
    in ``normalize_keyword_display``, so ``Hawai'i 'Amakihi`` and
    ``Hawai’i ’Amakihi`` would read as two species.
    """
    return (folded_species_key(species) or "").strip().translate(
        _ASCII_LOWER_TABLE,
    )
