import requests_mock
from basis.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import IS_CI, get_test_command, reqest_mocker, set_tmp_dir

def test_list():
    set_tmp_dir(create_basis_config=True)
    command_tester = get_test_command("list")
    with reqest_mocker() as m:
        for e in [
            Endpoints.ENVIRONMENTS_LIST,
            Endpoints.GRAPHS_LIST,
            Endpoints.NODES_LIST,
        ]:
            obj_name = e.split("/")[-2]
            m.get(
                API_BASE_URL + e, json={"results": [{"name": "name"}]},
            )
        command_tester.execute(f"env")
        assert "name" in command_tester.io.fetch_output()
        command_tester.execute(f"graph")
        assert "name" in command_tester.io.fetch_output()
        command_tester.execute(f"node")
        assert "name" in command_tester.io.fetch_output()
