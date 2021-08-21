from basis.utils.ulid import generate_ulid_as_base32
from datetime import datetime, timedelta
from time import time

from basis.helpers.connectors.connection import HttpApiConnection
from basis.helpers.connectors.mock_data_source import GenericJsonObjectDataSource


def test_http_conn():
    period = 1
    conn = HttpApiConnection(ratelimit_params=dict(calls=1, period=period))
    params = {"dt": datetime(2000, 1, 1)}
    t = time()
    conn.get("http://example.com", params)
    conn.get("http://example.com", params)
    dur = time() - t
    assert dur > period


def test_data_source():
    start = datetime(2000, 1, 1)
    ds = GenericJsonObjectDataSource(
        first_created_at=start,
        objects_per_period_modulo=10,
        update_frequency_period=100,
        update_modulo=10,
        random_seed=123456789,
        period_seconds=60,
    )

    assert len(ds.build_all_objects(start)) == 0
    objs = ds.build_all_objects(start + timedelta(minutes=10))
    assert len(objs) == 10 * 9 / 2
    objs = ds.build_all_objects(start + timedelta(minutes=100))
    assert len(objs) == 100 * 9 / 2
    assert len(set([o.id for o in objs])) == 100 * 9 / 2
    assert len(set([str(o.updated_at) for o in objs])) == 90


def test_data_source_get():
    start = datetime(2000, 1, 1)
    ds = GenericJsonObjectDataSource(
        first_created_at=start,
        objects_per_period_modulo=10,
        update_frequency_period=100,
        update_modulo=10,
        random_seed=123456789,
        period_seconds=60,
    )
    n = 10
    objs = ds.get_by_page(start + timedelta(minutes=10), limit=n, page=2)
    assert len(objs) == n
    objs = ds.get_by_page(start + timedelta(minutes=10), limit=n, page=5)
    assert len(objs) == 5  # only 5 left
    # order by
    objs = ds.get_by_page(
        start + timedelta(minutes=10), limit=n, page=5, order_direction="desc"
    )
    assert objs[-1].created_at == start + timedelta(minutes=1)
    # Last is first
    assert objs[-1].id, 1  # Last is first
    objs = ds.get_by_page(
        start + timedelta(minutes=10),
        limit=n,
        page=1,
        order_by="updated_at",
        order_direction="desc",
        min_updated_at=start + timedelta(minutes=9),
    )
    assert len(objs) == 9
    assert objs[-1].updated_at == start + timedelta(minutes=9)


def test_ulid():
    ids = [generate_ulid_as_base32() for _ in range(10000)]
    assert sorted(ids) == ids
