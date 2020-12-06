import os

from click.testing import CliRunner

from snapflow.cli import app
from snapflow.project.project import SNAPFLOW_PROJECT_FILE_NAME
from snapflow.testing.utils import get_tmp_sqlite_db_url


def test_app():
    db_url = get_tmp_sqlite_db_url()
    runner = CliRunner()
    result = runner.invoke(app, ["-m", db_url, "log"])
    assert result.exit_code == 0
    result = runner.invoke(app, ["-m", db_url, "list", "pipes"])
    assert result.exit_code == 0
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["-m", db_url, "init"])
        assert result.exit_code == 0
        pth = os.path.join(os.getcwd(), SNAPFLOW_PROJECT_FILE_NAME)
        assert os.path.exists(pth)
