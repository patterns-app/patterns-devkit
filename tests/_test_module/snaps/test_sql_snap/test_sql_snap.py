from __future__ import annotations
from snapflow import SqlSnap, SnapContext


@SqlSnap(namespace="_test_module", file=__file__)
def test_sql_snap():
    return "test_sql_snap.sql"
