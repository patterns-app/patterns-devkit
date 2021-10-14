import requests_mock
from basis.cli.api import DEFAULT_BASE_URL
from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_logs():
    if not IS_CI:
        dr = set_tmp_dir(create_basis_config=True)
        command_tester = get_test_command("logs")
        with requests_mock.Mocker() as m:
            m.post(
                DEFAULT_BASE_URL + "dataspace/logs",
                json={"name": "name"},
            )
            m.post(
                DEFAULT_BASE_URL + "app/logs",
                json={"name": "name"},
            )
            m.post(
                DEFAULT_BASE_URL + "node/logs",
                json={"name": "name"},
            )
            command_tester.execute(f"dataspace name")
            assert "name" in command_tester.io.fetch_output()
            command_tester.execute(f"app name")
            assert "name" in command_tester.io.fetch_output()
            command_tester.execute(f"node name")
            assert "name" in command_tester.io.fetch_output()
