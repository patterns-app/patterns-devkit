from pathlib import Path

from patterns.cli.config import read_devkit_config
from patterns.cli.services.api import Endpoints
from tests.cli.base import set_tmp_dir, run_cli, request_mocker


def test_config_org_and_env(tmp_path: Path):
    set_tmp_dir(tmp_path)
    old_cfg = read_devkit_config()
    assert old_cfg.organization_id == "test-org-uid"

    with request_mocker() as m:
        m.get(Endpoints.organization_by_slug("org"), json={"uid": "org-uid"})
        run_cli("config -o org")
    new_cfg = read_devkit_config()
    assert new_cfg.organization_id == "org-uid"
