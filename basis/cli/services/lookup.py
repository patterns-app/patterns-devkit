from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

from basis.cli.config import (
    read_local_basis_config,
    CliConfig,
    update_local_basis_config,
)
from basis.cli.services.environments import (
    get_environment_by_name,
    paginated_environments,
)
from basis.cli.services.graph import find_graph_file, resolve_graph_path
from basis.cli.services.graph_versions import (
    get_graph_by_name,
    get_active_graph_version,
    get_graph_version_by_id,
)
from basis.cli.services.organizations import (
    get_organization_by_name,
    paginated_organizations,
)
from basis.cli.services.output import prompt_choices
from basis.configuration.base import load_yaml


@dataclass
class IdLookup:
    """A cached lookup of various ids from whatever info you can give it."""

    environment_name: str = None
    organization_name: str = None
    explicit_graph_path: Path = None
    node_file_path: Path = None
    explicit_graph_version_id: str = None
    ignore_local_cfg: bool = False
    explicit_graph_name: str = None
    find_nearest_graph: bool = False

    @cached_property
    def organization_id(self) -> str:
        if self.organization_name:
            return get_organization_by_name(self.organization_name)["uid"]
        if self.cfg.organization_id:
            return self.cfg.organization_id
        organizations = list(paginated_organizations())
        orgs_by_name = {org["name"]: org for org in organizations}
        if len(organizations) == 1:
            org = organizations[0]
            update_local_basis_config(organization_id=org["uid"])
        else:
            org_name = prompt_choices(
                "Available organizations",
                "Select an organization",
                choices=orgs_by_name.keys(),
            )
            org = orgs_by_name[org_name]
        self.organization_name = org["name"]
        return org["uid"]

    @cached_property
    def environment_id(self) -> str:
        if self.environment_name:
            env = get_environment_by_name(self.organization_id, self.environment_name)
            return env["uid"]
        if self.cfg.environment_id:
            return self.cfg.environment_id

        environments = list(paginated_environments(self.organization_id))
        envs_by_name = {env["name"]: env for env in environments}

        if len(environments) == 1:
            uid = environments[0]["uid"]
            update_local_basis_config(environment_id=uid)
            return uid

        env_name = prompt_choices(
            "Available environments",
            "Select an environment",
            choices=envs_by_name.keys(),
        )
        return envs_by_name[env_name]["uid"]

    @cached_property
    def graph_id(self) -> str:
        return get_graph_by_name(self.organization_id, self.graph_name)["uid"]

    @cached_property
    def graph_version_id(self):
        if self.explicit_graph_version_id:
            return self.explicit_graph_version_id
        return get_active_graph_version(self.graph_id)["uid"]

    @cached_property
    def node_id(self) -> str:
        graph = self.graph_file_path
        node = self.node_file_path
        if not graph or not node:
            raise ValueError("Must specify a node")
        err_msg = f"Node {node} is not part of the graph at {graph}"

        try:
            node_path = node.absolute().relative_to(graph.parent)
        except Exception as e:
            raise Exception(err_msg) from e
        cfg = load_yaml(graph) or {}
        for node in cfg.get("nodes", []):
            if node.get("node_file") == node_path.as_posix():
                id = node.get("id")
                if id:
                    return id
                raise Exception("Node does not have an id. Run `basis upload` first.")
        raise Exception(err_msg)

    @cached_property
    def graph_file_path(self) -> Path:
        if self.explicit_graph_path:
            return resolve_graph_path(self.explicit_graph_path, exists=True)
        if self.node_file_path:
            return find_graph_file(
                self.node_file_path.parent,
                prompt=False,
                nearest=self.find_nearest_graph,
            )
        return find_graph_file(path=None, prompt=True, nearest=self.find_nearest_graph)

    @cached_property
    def graph_directory(self) -> Path:
        return self.graph_file_path.parent

    @cached_property
    def root_graph_file(self) -> Path:
        return find_graph_file(self.graph_directory, nearest=False)

    @cached_property
    def cfg(self) -> CliConfig:
        if self.ignore_local_cfg:
            return CliConfig()
        return read_local_basis_config()

    @cached_property
    def graph_name(self) -> str:
        def from_yaml():
            yaml = load_yaml(self.root_graph_file)
            return yaml.get("name", self.root_graph_file.parent.name)

        if self.explicit_graph_name:
            return self.explicit_graph_name
        if self.explicit_graph_path or self.node_file_path:
            return from_yaml()
        if self.explicit_graph_version_id:
            vid = self.explicit_graph_version_id
            return get_graph_version_by_id(vid)["graph"]["name"]
        return from_yaml()
