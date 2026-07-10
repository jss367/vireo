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
    value = unicodedata.normalize("NFKC", str(name or ""))
    value = "".join(" " if ch.isspace() else ch for ch in value)
    value = re.sub(r" +", " ", value).strip()
    value = value.strip(_EDGE_QUOTES)
    value = re.sub(r" +", " ", value).strip()
    return value


def keyword_match_key(name: str) -> str:
    """Return the key used when comparing keyword names for equivalence."""
    return normalize_keyword_display(name).casefold()
