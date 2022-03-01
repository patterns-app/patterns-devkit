from pathlib import Path

from basis.cli.config import read_local_basis_config
from basis.cli.services.api import Endpoints, API_BASE_URL
from tests.cli.base import set_tmp_dir, run_cli, request_mocker


def test_config_org_and_env(tmp_path: Path):
    set_tmp_dir(tmp_path)
    old_cfg = read_local_basis_config()
    assert old_cfg.environment_id == "test-env-uid"
    assert old_cfg.organization_id == "test-org-uid"

    with request_mocker() as m:
        m.get(
            API_BASE_URL + Endpoints.organization_by_name("org"),
            json={"uid": "org-uid"},
        )
        m.get(
            API_BASE_URL + Endpoints.environment_by_name("org-uid", "env"),
            json={"uid": "env-uid"},
        )
        run_cli("config -o org -e env")
    new_cfg = read_local_basis_config()
    assert new_cfg.organization_id == "org-uid"
    assert new_cfg.environment_id == "env-uid"
