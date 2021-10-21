import requests_mock
from basis.cli.api import DEFAULT_BASE_URL
from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_info():
    set_tmp_dir(create_basis_config=True)
    command_tester = get_test_command("info")
    with requests_mock.Mocker() as m:
        m.post(
            DEFAULT_BASE_URL + "environments/info",
            json={"name": "name"},
        )
        m.post(
            DEFAULT_BASE_URL + "graphs/info",
            json={"name": "name"},
        )
        m.post(
            DEFAULT_BASE_URL + "nodes/info",
            json={"name": "name"},
        )
        command_tester.execute(f"env name")
        assert "name" in command_tester.io.fetch_output()
        command_tester.execute(f"graph name")
        assert "name" in command_tester.io.fetch_output()
        command_tester.execute(f"node name")
        assert "name" in command_tester.io.fetch_output()
