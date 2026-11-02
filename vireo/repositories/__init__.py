"""Domain-focused SQLite repositories used behind the Database façade."""

# Sentinel for "argument not provided" vs an explicit None. ``db`` re-exports
# it as ``_UNSET`` so façade signatures and repository signatures share one
# object and a wrapper can forward its arguments unchanged.
UNSET = object()

# SQLite's historical bound-parameter limit is 999; 800 leaves room for the
# other parameters a chunked statement binds. ``db`` re-exports it as
# ``_SQLITE_PARAM_CHUNK_SIZE`` for the factories that pass a chunk size in.
SQLITE_PARAM_CHUNK_SIZE = 800


def chunks(values, size=SQLITE_PARAM_CHUNK_SIZE):
    """Yield ``values`` (any iterable) in lists of at most ``size`` items."""
    values = list(values)
    for idx in range(0, len(values), size):
        yield values[idx:idx + size]
