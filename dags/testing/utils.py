from __future__ import annotations

import tempfile

from sqlalchemy.orm import Session

from dags.core.node import DataBlockLog, PipeLog
from dags.utils.common import rand_str


def get_tmp_sqlite_db_url(dbname=None):
    if dbname is None:
        dbname = rand_str(10)
    dir = tempfile.mkdtemp()
    return f"sqlite:///{dir}/{dbname}.db"


def display_pipe_log(sess: Session):
    for dbl in sess.query(DataBlockLog).order_by(DataBlockLog.created_at):
        print(f"{dbl.pipe_log.pipe_key:30} {dbl.data_block_id:4} {dbl.direction}")
