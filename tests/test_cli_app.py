import os

from click.testing import CliRunner

from basis.cli import app
from basis.project.project import BASIS_PROJECT_FILE_NAME


def test_app():
    runner = CliRunner()
    result = runner.invoke(app, ["log"])
    assert result.exit_code == 0
    result = runner.invoke(app, ["list", "functions"])
    assert result.exit_code == 0
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["init"])
        assert result.exit_code == 0
        pth = os.path.join(os.getcwd(), BASIS_PROJECT_FILE_NAME)
        assert os.path.exists(pth)
