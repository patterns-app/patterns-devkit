from __future__ import annotations

from snapflow.core.sql.sql_snap import SqlSnap


@SqlSnap(
    namespace="core",
    display_name="Dedupe Table (keep latest)",
    file=__file__,
    required_storage_engines=["postgres"],
    # TODO: requires postgres StorageEngine
)
def dedupe_keep_latest_sql():
    return "dedupe_keep_latest_sql.sql"

