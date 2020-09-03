import os

from click.testing import CliRunner

from dags.cli import app
from dags.project.project import DAGS_PROJECT_FILE_NAME


def test_app():
    runner = CliRunner()
    result = runner.invoke(app, ["log"])
    assert result.exit_code == 0
    result = runner.invoke(app, ["list", "pipes"])
    assert result.exit_code == 0
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["init"])
        assert result.exit_code == 0
        pth = os.path.join(os.getcwd(), DAGS_PROJECT_FILE_NAME)
        assert os.path.exists(pth)
