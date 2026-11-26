"""Comparing a copy with its source, byte for byte.

A move deletes the source once the copy is verified, so a matching size is
not enough: a copy can have the right length and the wrong bytes (a write
the share dropped, a zero-filled range, a file replaced after it landed).
Every local verifier in this package reads both files through
``files_match`` before the source is removed.
"""

_CHUNK = 1024 * 1024


def files_match(source, destination):
    """Whether ``source`` and ``destination`` hold the same bytes.

    Both files are read on every call. ``filecmp`` remembers results by stat
    signature, so it would pass a destination rewritten in place with the
    same size and timestamp on the strength of an earlier comparison.
    ``OSError`` from either read propagates; an unreadable file is never a
    match.
    """
    with open(source, "rb") as src, open(destination, "rb") as dst:
        while chunk := src.read(_CHUNK):
            if chunk != dst.read(len(chunk)):
                return False
        return not dst.read(1)
