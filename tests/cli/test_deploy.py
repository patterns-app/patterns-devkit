from pathlib import Path

from basis.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import request_mocker, set_tmp_dir, run_cli


def test_deploy(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    path = '/'.join((dr / "name").parts)

    with request_mocker() as m:
        for e in [
            Endpoints.GRAPH_VERSIONS_CREATE,
            Endpoints.DEPLOYMENTS_DEPLOY,
        ]:
            m.post(
                API_BASE_URL + e, json={"uid": 1, "graph_name": "name"},
            )
        m.get(
            API_BASE_URL + Endpoints.GRAPH_VERSIONS_LIST, json={'results': [{'uid': 1}]}
        )

        run_cli(f"create graph {path}")

        result = run_cli(f"upload --no-deploy {path}")
        assert 'Uploaded new graph' in result.output
        assert 'Graph deployed' not in result.output

        result = run_cli(f"deploy --graph={path}")
        assert 'deployed' in result.output
