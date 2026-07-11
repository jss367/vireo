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
    "\u00b4"
    "\u2018\u2019\u201a\u201b"
    "\u201c\u201d\u201e\u201f"
    "\u2032\u2033"
    "\u275b\u275c\u275d\u275e"
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
    # `́apapane` — a nearly invisible variant that no longer matches
    # `apapane`.
    value = value.strip()
    # Strip edge quotes BEFORE NFKC so characters that decompose into a
    # spacing char plus a combining mark (e.g. U+00B4 ACUTE ACCENT ->
    # U+0020 U+0301) get removed while they are still a single quote-ish
    # code point at the edge. Skipping this pre-strip lets `´apapane`
    # normalize to `́apapane`, an invisible variant that survives the
    # post-NFKC strip below because U+0301 is not in _EDGE_QUOTES.
    value = value.strip(_EDGE_QUOTES)
    value = unicodedata.normalize("NFKC", value)
    value = "".join(" " if ch.isspace() else ch for ch in value)
    value = re.sub(r" +", " ", value).strip()
    value = value.strip(_EDGE_QUOTES)
    value = re.sub(r" +", " ", value).strip()
    return value


def keyword_match_key(name: str) -> str:
    """Return the key used when comparing keyword names for equivalence.

    Uses ``str.lower()`` rather than ``str.casefold()`` so distinct user
    keywords are not silently folded together on the merge/dedupe path.
    ``casefold`` is more aggressive than the previous SQLite
    ``LOWER(name)``/``COLLATE NOCASE`` behavior: for example
    ``"Maße".casefold() == "Masse".casefold() == "masse"``, so grouping
    duplicates by that key would let ``merge_duplicate_keywords()`` retag
    and delete one of two unrelated German keywords even though
    ``add_keyword()`` and the table constraints would treat them as
    distinct. ``str.lower()`` keeps ``ß`` as ``ß`` (its lowercase form),
    which lines up with SQLite's ASCII-only ``COLLATE NOCASE`` comparison
    used everywhere else in this codebase.
    """
    return normalize_keyword_display(name).lower()
