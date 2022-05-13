from __future__ import annotations

import functools
import io
import re
from io import StringIO
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, IO, Iterator, Callable
from zipfile import ZipFile, ZipInfo

import ruyaml

from patterns.cli.helpers import compress_directory, random_node_id
from patterns.cli.services.graph import resolve_graph_path

MISSING = object()


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
            # so pick a value that matches the least-indented list item we see.
            matches = (
                len(m.group(1)) for m in re.finditer(r"^( *)-", text, re.MULTILINE)
            )
            # ruyaml's indent number includes the length  of "- " for some reason
            indent = min(matches, default=2) + 2
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

    def set_name(self, name: str) -> GraphConfigEditor:
        self._cfg["title"] = name
        return self

    def get_title(self) -> Optional[str]:
        return self._cfg.get("title")

    def get_slug(self) -> Optional[str]:
        """Return the slug value listed in the yaml, or None if it's not present"""
        return self._cfg.get("slug")

    def add_function_node_dict(self, node: dict) -> GraphConfigEditor:
        d = {k: v for (k, v) in node.items() if v is not None}
        for k in ("node_file", "id", "webhook"):
            if (
                k in d
                and d[k]
                and any(it.get(k) == d[k] for it in self._cfg.get("functions", []))
            ):
                raise ValueError(
                    f"{k} '{d[k]}' already defined in the graph configuration"
                )

        if "functions" not in self._cfg:
            self._cfg["functions"] = []
        self._cfg["functions"].append(d)
        return self

    def add_node(
        self,
        node_file: str,
        schedule: str = None,
        inputs: Dict[str, str] = None,
        outputs: Dict[str, str] = None,
        parameters: Dict[str, Any] = None,
        title: str = None,
        id: Optional[str] = MISSING,
        description: str = None,
        description_file: str = None,
    ) -> GraphConfigEditor:
        if id is MISSING:
            id = random_node_id()
        self.add_function_node_dict(
            {
                "node_file": node_file,
                "schedule": schedule,
                "inputs": inputs,
                "outputs": outputs,
                "parameters": parameters,
                "title": title,
                "id": str(id) if id else id,
                "description": description,
                "description_file": description_file,
            }
        )
        return self

    def add_store(
        self,
        name: str,
        table: bool,
        title: str = None,
        id: Optional[str] = MISSING,
        schema: str = None,
    ):
        if id is MISSING:
            id = random_node_id()
        d = {
            "table" if table else "stream": name,
            "title": title,
            "id": str(id) if id else id,
            "schema": schema,
        }
        d = {k: v for (k, v) in d.items() if v is not None}

        for k in ("table", "stream", "id"):
            if d.get(k) and any(
                it.get(k) == d[k] for it in self._cfg.get("stores", [])
            ):
                raise ValueError(
                    f"{k} '{d[k]}' already defined in the graph configuration"
                )

        if "stores" not in self._cfg:
            self._cfg["stores"] = []
        self._cfg["stores"].append(d)
        return self

    def add_webhook(
        self,
        webhook: str,
        title: str = None,
        id: Optional[str] = MISSING,
        description: str = None,
        description_file: str = None,
    ) -> GraphConfigEditor:
        if id is MISSING:
            id = random_node_id()
        self.add_function_node_dict(
            {
                "webhook": webhook,
                "title": title,
                "id": str(id) if id else id,
                "description": description,
                "description_file": description_file,
            }
        )
        return self

    def add_component_uses(
        self,
        component_key: str,
        schedule: str = None,
        inputs: Dict[str, str] = None,
        outputs: Dict[str, str] = None,
        parameters: Dict[str, Any] = None,
        title: str = None,
        id: Optional[str] = MISSING,
        description: str = None,
        description_file: str = None,
    ) -> GraphConfigEditor:
        if id is MISSING:
            id = random_node_id()
        self.add_function_node_dict(
            {
                "uses": component_key,
                "schedule": schedule,
                "inputs": inputs,
                "outputs": outputs,
                "parameters": parameters,
                "title": title,
                "id": str(id) if id else id,
                "description": description,
                "description_file": description_file,
            }
        )
        return self

    def add_missing_node_ids(self) -> GraphConfigEditor:
        """Add a random id to any node entry that doesn't specify one"""
        for node in self.all_nodes():
            if "id" not in node:
                node["id"] = random_node_id()
        return self

    def all_nodes(self) -> Iterator[dict]:
        """Return an iterator over all function and store node declarations"""
        yield from self.function_nodes()
        yield from self.store_nodes()

    def function_nodes(self) -> Iterator[dict]:
        nodes = self._cfg.get("functions")
        if not isinstance(nodes, list):
            return

        for node in nodes:
            if not isinstance(node, dict):
                continue
            yield node

    def store_nodes(self) -> Iterator[dict]:
        nodes = self._cfg.get("stores")
        if not isinstance(nodes, list):
            return

        for node in nodes:
            if not isinstance(node, dict):
                continue
            yield node


class GraphDirectoryEditor:
    def __init__(self, graph_path: Path, overwrite: bool = False):
        """
        :param graph_path: The path to a graph.yml file, or a directory containing one
        :param overwrite: If False, raise an exception in add_node_from_zip if a node
            exists and differs from the extracted content.
        """
        try:
            self.yml_path = resolve_graph_path(graph_path, exists=True)
        except ValueError:
            self.yml_path = resolve_graph_path(graph_path, exists=False)
        self.dir = self.yml_path.parent
        self.overwrite = overwrite
        if self.yml_path.is_file():
            self._cfg = self._editor(self.yml_path)
        else:
            self._cfg: Optional[GraphConfigEditor] = None

    def graph_slug(self) -> str:
        """Return the graph name slug based on the graph directory name"""
        if self._cfg and self._cfg.get_slug():
            name = self._cfg.get_slug()
        else:
            name = self.yml_path.parent.name
        return re.sub(r"[^a-zA-Z0-9]", "-", name)

    def compress_directory(self) -> io.BytesIO:
        """Return an in-memory zip file containing the compressed graph directory"""
        return compress_directory(self.dir)

    def add_node_from_file(self, dst_path: Union[Path, str], file: IO[bytes]):
        """Write the content of a file to dst_path

       :param dst_path: Path relative to the output graph directory
       :param file: A file-like object open in read mode
       """
        dst_path = Path(dst_path)
        self._write_file(dst_path, file)
        self._add_cfg_node(dst_path)

    def add_node_from_zip(
        self,
        src_path: Union[Path, str],
        dst_path: Union[Path, str],
        zf: Union[ZipFile, Path, IO[bytes]],
    ) -> GraphDirectoryEditor:
        """Copy the node or subgraph located at src_path in zipfile to dst_path

        :param src_path: Path relative to the root of zipfile
        :param dst_path: Path relative to the output graph directory
        :param zf: A ZipFile open in read mode, or a path to a zip file to open
        """
        src_path = Path(src_path)
        dst_path = Path(dst_path)
        if isinstance(zf, ZipFile):
            self._add(src_path, dst_path, zf)
        else:
            with ZipFile(zf, "r") as f:
                self._add(src_path, dst_path, f)
        return self

    def add_missing_node_ids(self) -> GraphDirectoryEditor:
        """Add a random id to any node entry that doesn't specify one

        This will update all graph.yml files in the directory
        """
        for editor in self._graph_editors():
            editor.add_missing_node_ids()
            editor.write()
        return self

    def _add(self, src_path: Path, dst_path: Path, zf: ZipFile):
        if src_path.name == "graph.yml":

            def dirname(p):
                if len(p.parts) <= 1:
                    return ""
                return _zip_name(p.parent) + "/"

            src_dir = dirname(src_path)
            dst_dir = dirname(dst_path)

            for info in zf.infolist():
                if info.filename.startswith(src_dir) and not info.is_dir():
                    new_name = dst_dir + info.filename[len(src_dir) :]
                    self._extract_file(info, Path(new_name), zf)
        else:
            self._extract_file(zf.getinfo(_zip_name(src_path)), dst_path, zf)
        self._add_cfg_node(dst_path)

    def _add_cfg_node(self, dst_path: Path):
        if not self._cfg or str(dst_path) == "graph.yml":
            return
        try:
            self._cfg.add_node(_zip_name(dst_path)).write()
        except ValueError:
            pass  # node already exists, leave it unchanged

    def _extract_file(self, member: ZipInfo, dst_path: Path, zf: ZipFile):
        full_dst_path = self.dir / dst_path
        if full_dst_path.is_dir():
            raise ValueError(
                f"Cannot extract {dst_path}: a directory by that name exists"
            )
        if self.overwrite or not full_dst_path.is_file():
            member.filename = _zip_name(dst_path)
            zf.extract(member, self.dir)
        else:
            with zf.open(member, "r") as f:
                self._write_file(dst_path, f)

    def _write_file(self, dst_path: Path, file: IO[bytes]):
        full_dst_path = self.dir / dst_path
        new_content = io.TextIOWrapper(file).read()

        if not self.overwrite:
            try:
                old_content = full_dst_path.read_text()
            except FileNotFoundError:
                pass
            else:
                if new_content != old_content:
                    raise FileOverwriteError(
                        full_dst_path,
                        f"Cannot extract {dst_path}: would overwrite existing file",
                    )
        full_dst_path.write_text(new_content)

    def _graph_editors(self) -> Iterator[GraphConfigEditor]:
        for p in self.dir.rglob("graph.yml"):
            yield self._editor(p)

    @functools.lru_cache(maxsize=None)
    def _editor(self, yaml_path: Path) -> GraphConfigEditor:
        return GraphConfigEditor(yaml_path)


class FileOverwriteError(Exception):
    def __init__(self, file_path: Path, message: str) -> None:
        super().__init__(message)
        self.file_path = file_path


def _zip_name(p: Path):
    return "/".join(p.parts)
