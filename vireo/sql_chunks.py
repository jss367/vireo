"""SQLite bound-parameter chunking shared by routes and services.

SQLite's ``SQLITE_MAX_VARIABLE_NUMBER`` defaults to 32766 on builds since
3.32 but remains 999 on older builds (and on some packagers' default builds).
Bulk actions can hand us thousands of photo ids at once, so every IN-clause
query over a caller-sized list is chunked under this cap to stay portable
across SQLite versions. Sized below 999 to leave headroom for additional
bound parameters in joined statements.
"""

SQL_PARAM_CHUNK = 900


def chunked(seq, size=SQL_PARAM_CHUNK):
    """Yield ``seq`` in successive lists of at most ``size`` items."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]
