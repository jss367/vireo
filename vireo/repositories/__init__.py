"""Domain-focused SQLite repositories used behind the Database façade."""

# Sentinel for "argument not provided" vs an explicit None. ``db`` re-exports
# it as ``_UNSET`` so façade signatures and repository signatures share one
# object and a wrapper can forward its arguments unchanged.
UNSET = object()
