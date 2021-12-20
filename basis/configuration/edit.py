import re
from io import StringIO
from pathlib import Path
from typing import List, Dict, Any, Optional

import ruyaml

from basis.configuration.graph import NodeCfg, ExposingCfg, GraphDefinitionCfg
from basis.configuration.path import NodeId
from basis.graph.builder import graph_manifest_from_yaml, _GraphBuilder, _ParsedGraph
from basis.graph.configured_node import GraphManifest


class GraphConfigEditor:
    """Edit a graph.yml file, preserving comments

    By default, constructing an instance of this class will raise an exception if the
    yaml file doesn't exist or can't be parsed.

    If you pass `read=False` to the constructor, the file won't be read, and you'll
    start with an empty config.

    You can also pass `path_to_graph_yml=None` if you aren't going to write back to
    disk.
    """

    def __init__(self, path_to_graph_yml: Optional[Path], read: bool = True):
        self._yaml = ruyaml.YAML()
        self._path_to_graph_yml = path_to_graph_yml
        self._yaml.indent(mapping=2, sequence=4, offset=2)
        # read text manually instead of loading the Path directly to normalize line
        # breaks. Ruyaml opens files in binary mode (bypassing universal newline
        # support), then proceeds to behave incorrectly in the presence of \r\n, adding
        # extra line breaks in the output.
        if read:
            with self._path_to_graph_yml.open() as f:
                text = f.read()
            self._cfg = self._yaml.load(text) or {}
            # ruyaml doesn't provide a way to preserve indentation,
            # so pick a value that matches the first list item we see
            if m := re.search(r"^( *)-", text, re.MULTILINE):
                indent = len(m.group(1)) + 2
            else:
                indent = 4
        else:
            self._cfg = {}
            indent = 4

        self._yaml.indent(
            mapping=int(indent / 2), sequence=indent, offset=max(0, indent - 2)
        )

    def write(self):
        """Write the config back to the file"""
        self._yaml.dump(self._cfg, self._path_to_graph_yml)

    def dump(self) -> str:
        """Return the edited config as a yaml string"""
        s = StringIO()
        self._yaml.dump(self._cfg, s)
        return s.getvalue()

    def parse_to_cfg(self) -> GraphDefinitionCfg:
        """Parse the data to a GraphDefinitionCfg without writing it to disk"""
        return GraphDefinitionCfg(**self._cfg)

    def parse_to_manifest(self) -> GraphManifest:
        """Parse the data to a GraphManifest without writing it to disk"""
        parse_to_cfg = self.parse_to_cfg
        path_to_graph_yml = self._path_to_graph_yml

        class MemBuilder(_GraphBuilder):
            def _read_graph_yml(self, path: Path) -> GraphDefinitionCfg:
                if path_to_graph_yml and path != path_to_graph_yml:
                    return super()._read_graph_yml(path)
                if path != self.root_graph_location:
                    raise NotImplementedError("Cannot construct subgraphs in-memory")
                return parse_to_cfg()

        if path_to_graph_yml:
            p = path_to_graph_yml
        else:
            p = Path(self.get_name() or "graph") / "graph.yml"
        return MemBuilder(p).build()

    def set_name(self, name: str) -> "GraphConfigEditor":
        self._cfg["name"] = name
        return self

    def get_name(self) -> Optional[str]:
        return self._cfg.get("name")

    def get_exposing_cfg(self) -> Optional[ExposingCfg]:
        if "exposes" in self._cfg:
            return ExposingCfg(**self._cfg["exposes"])
        return None

    def set_exposing_cfg(self, exposing: Optional[ExposingCfg]) -> "GraphConfigEditor":
        if exposing is None:
            del self._cfg["exposes"]
        else:
            self._cfg["exposes"] = exposing.dict(exclude_none=True)
        return self

    def add_node_cfg(self, node: NodeCfg) -> "GraphConfigEditor":
        d = node.dict(exclude_none=True)

        for k in ("node_file", "id", "webhook"):
            if (
                k in d
                and d[k]
                and any(it.get(k) == d[k] for it in self._cfg.get("nodes", []))
            ):
                raise ValueError(
                    f"{k} '{d[k]}' already defined in the graph configuration"
                )

        # ruyaml refuses to dump anything that isn't a built-in type, even subclasses of
        # them, so we have to map all the inputs and outputs to strings
        for k in ("inputs", "outputs"):
            p = d.get(k, None)
            if p is None:
                continue
            d[k] = [str(v) for v in p]

        if "nodes" not in self._cfg:
            self._cfg["nodes"] = []
        self._cfg["nodes"].append(d)
        return self

    def add_node(
        self,
        node_file: str,
        schedule: str = None,
        inputs: List[str] = None,
        outputs: List[str] = None,
        parameters: Dict[str, Any] = None,
        name: str = None,
        id: str = None,
        description: str = None,
    ) -> "GraphConfigEditor":
        self.add_node_cfg(
            NodeCfg(
                node_file=node_file,
                schedule=schedule,
                inputs=inputs,
                outputs=outputs,
                parameters=parameters,
                name=name,
                id=id,
                description=description,
            )
        )
        return self

    def add_webhook(
        self, webhook: str, name: str = None, id: str = None, description: str = None
    ) -> "GraphConfigEditor":
        self.add_node_cfg(
            NodeCfg(webhook=webhook, name=name, id=id, description=description)
        )
        return self
