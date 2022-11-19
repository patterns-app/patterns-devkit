from pathlib import Path

from patterns.cli.helpers import _all_files_not_ignored


def test_ignore_file(tmp_path: Path):
    f1 = tmp_path / "__pycache__" / "settings.xml"
    f1.parent.mkdir()
    f1.write_text("<>")

    f2 = tmp_path / ".DS_Store" / "foo"
    f2.parent.mkdir()
    f2.write_text("foo")

    f3 = tmp_path / "my.venv" / "foo.txt"
    f3.parent.mkdir()
    f3.write_text("foo")

    f4 = tmp_path / "p.pyc"
    f4.write_text("foo")

    f5 = tmp_path / "my.pycx"
    f5.write_text("foo")

    assert sorted(_all_files_not_ignored(tmp_path)) == sorted([f3, f5])
