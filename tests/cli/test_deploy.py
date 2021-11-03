# from pathlib import Path
#
# from basis.cli.services.api import API_BASE_URL, Endpoints
# from tests.cli.base import IS_CI, get_test_command, reqest_mocker, set_tmp_dir
#
#
# def test_deploy():
#     dr = set_tmp_dir(create_basis_config=True)
#     proj_path = Path(dr) / "proj"
#     # Create graph
#     get_test_command("generate").execute(f"graph {proj_path}", inputs="\n")
#     command_tester = get_test_command("upload")
#     with reqest_mocker() as m:
#         m.post(
#             API_BASE_URL + Endpoints.GRAPH_VERSIONS_CREATE,
#             json={"uid": 1},
#         )
#         command_tester.execute(f"{proj_path / 'graph.yml'}")
#         m.post(
#             API_BASE_URL + Endpoints.GRAPH_VERSIONS_CREATE,
#             json={"uid": 1},
#             )
#         command_tester.execute(f"{proj_path / 'graph.yml'}")
