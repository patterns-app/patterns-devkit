import requests_mock
from basis.cli.api import DEFAULT_BASE_URL
from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_info():
    if not IS_CI:
        dr = set_tmp_dir(create_basis_config=True)
        command_tester = get_test_command("list")
        with requests_mock.Mocker() as m:
            m.post(
                DEFAULT_BASE_URL + "environments", json={"name": "name"},
            )
            m.post(
                DEFAULT_BASE_URL + "graphs", json={"name": "name"},
            )
            m.post(
                DEFAULT_BASE_URL + "nodes", json={"name": "name"},
            )
            command_tester.execute(f"env")
            assert "name" in command_tester.io.fetch_output()
            command_tester.execute(f"graph")
            assert "name" in command_tester.io.fetch_output()
            command_tester.execute(f"node")
            assert "name" in command_tester.io.fetch_output()
