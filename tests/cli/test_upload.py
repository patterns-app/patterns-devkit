from basis.cli.api import DEFAULT_BASE_URL
from pathlib import Path
import requests_mock

from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_upload():
    if not IS_CI:
        dr = set_tmp_dir(create_basis_config=True)
        proj_path = Path(dr) / "proj"
        # Create dataspace
        get_test_command("generate").execute(f"new {proj_path}", inputs="\n")
        command_tester = get_test_command("upload")
        with requests_mock.Mocker() as m:
            m.post(
                DEFAULT_BASE_URL + "dataspace-version/upload",
                json={"dataspace_version_id": 1},
            )
            command_tester.execute(f"{proj_path / 'dataspace.yml'}")

