import re
from pathlib import Path
import patterns


def test_pyproject_and_package_versions_are_in_sync():
    path = (Path(__file__).parent.parent / "pyproject.toml").resolve()

    pyproject_version = re.findall(
        r'^version = "(\d+\.\d+\.\d+)"$', path.read_text(), re.M
    )
    assert patterns.__version__ == pyproject_version[0]
