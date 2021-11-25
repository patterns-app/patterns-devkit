from pathlib import Path

from basis.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import get_test_command, reqest_mocker


def test_upload():
    p = (Path(__file__).parent.parent / "graph" / "flat_graph" / "graph.yml").resolve()
    command_tester = get_test_command("upload")
    with reqest_mocker() as m:
        m.post(
            API_BASE_URL + Endpoints.GRAPH_VERSIONS_CREATE, json={"uid": 1},
        )
        command_tester.execute(str(p))
