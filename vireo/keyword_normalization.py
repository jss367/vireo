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
