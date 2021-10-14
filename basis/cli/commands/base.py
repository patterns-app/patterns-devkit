from __future__ import annotations

from basis.cli.config import read_local_basis_config
from basis.configuration.dataspace import DataspaceCfg


class BasisCommandBase:
    def is_authenticated(self):
        cfg = read_local_basis_config()
        return bool(cfg.get("token"))

    def ensure_authenticated(self):
        if not self.is_authenticated():
            self.line(
                f"<error>You must login before running this command (`basis login`)</error>"
            )
            exit(1)

    # def get_current_project_yaml(self, config_file: str = "basis.yml") -> Dict:
    #     return load_yaml(config_file)

    # def get_current_project(self, config_file: str = "basis.yml") -> ProjectCfg:
    #     return ProjectCfg.parse_file(config_file)

    # def write_current_project(
    #     self, project: ProjectCfg, config_file: str = "basis.yml",
    # ):
    #     with open(config_file, "w") as f:
    #         f.write(dump_yaml(project.dict(exclude_unset=True)))

