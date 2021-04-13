import os

from click.testing import CliRunner
from dcp.storage.database.utils import get_tmp_sqlite_db_url
from snapflow.cli import app
from snapflow.project.project import SNAPFLOW_PROJECT_FILE_NAME


def test_app():
    db_url = get_tmp_sqlite_db_url()
    runner = CliRunner()
    result = runner.invoke(app, ["-m", db_url, "logs"])
    assert result.exit_code == 0
    result = runner.invoke(app, ["-m", db_url, "nodes"])
    assert result.exit_code == 0
    result = runner.invoke(app, ["-m", db_url, "blocks"])
    assert result.exit_code == 0
    result = runner.invoke(
        app, ["-m", db_url, "generate", "schema"], input='{"f1": 1, "f2": "hi"}'
    )
    assert result.exit_code == 0
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["-m", db_url, "init"])
        assert result.exit_code == 0
        pth = os.path.join(os.getcwd(), SNAPFLOW_PROJECT_FILE_NAME)
        assert os.path.exists(pth)
