from pathlib import Path

import requests_mock
from basis.cli.services.api import API_BASE_URL
from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_cli_new_user():
    dr = set_tmp_dir(create_basis_config=True)
    proj_path = Path(dr) / "proj"
    # Create graphs
    subgraph = "subgraph1"
    # Root graph
    get_test_command("generate").execute(f"graph {proj_path}", inputs="\n")
    get_test_command("generate").execute(
        f"node {proj_path}/node1.py", inputs="\npython\n"
    )
    get_test_command("generate").execute(
        f"node {proj_path}/node2.sql", inputs="\nsql\n"
    )
    # Sub graph
    get_test_command("generate").execute(f"graph {proj_path}/{subgraph}", inputs="\n")
    # Sub node
    get_test_command("generate").execute(
        f"node {proj_path}/{subgraph}/node1.py", inputs="\npython\n"
    )

    # TODO:
    # - Login
    # - Edit graph.yml
    # - Deploy graph
    # - Run node
