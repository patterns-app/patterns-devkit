from click.testing import CliRunner

from basis.cli import app


def test_app():
    runner = CliRunner()
    result = runner.invoke(app, ["log"])
    assert result.exit_code == 0
    result = runner.invoke(app, ["list", "functions"])
    assert result.exit_code == 0
