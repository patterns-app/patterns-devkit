import os
import shutil
import base64
from basis.cli.api import DEFAULT_BASE_URL
from basis.cli.helpers import compress_directory
from pathlib import Path
import requests_mock

from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_clone():
    if not IS_CI:
        dr = set_tmp_dir(create_basis_config=True)
        proj_path = Path(dr) / "proj"
        # Create dataspace
        get_test_command("generate").execute(f"new {proj_path}", inputs="\n")
        zipf = compress_directory(proj_path)
        shutil.rmtree(proj_path)
        assert not os.path.exists(proj_path / "dataspace.yml")
        b64_zipf = base64.b64encode(zipf.read())
        command_tester = get_test_command("clone")
        with requests_mock.Mocker() as m:
            m.post(
                DEFAULT_BASE_URL + "dataspace-version/download",
                json={"zip": b64_zipf.decode()},
            )
            command_tester.execute(f"mock_name")
        assert os.path.exists(proj_path / "dataspace.yml")

