import os

from click.testing import CliRunner

from dags.cli import app
from dags.project.project import DAGS_PROJECT_FILE_NAME
from dags.testing.utils import get_tmp_sqlite_db_url


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
        pth = os.path.join(os.getcwd(), DAGS_PROJECT_FILE_NAME)
        assert os.path.exists(pth)
