import io
import re
import textwrap
import zipfile
from pathlib import Path
from typing import Dict

import pytest

from basis.configuration.edit import GraphDirectoryEditor, FileOverwriteError
from tests.graph.utils import setup_manifest


def test_add_new_node(tmp_path: Path):
    do_add_zip_test(
        tmp_path,
        before={"graph.yml": "nodes: []"},
        zip={"graph.yml": 'nodes: [{"node_file": "node.py"}]', "node.py": "foo"},
        src="node.py",
        dst="new.py",
        after={
            "graph.yml": """
        nodes:
          - node_file: new.py
            id: <id>
        """,
            "new.py": "foo",
        },
    )


def test_add_unchanged_node(tmp_path: Path):
    do_add_zip_test(
        tmp_path,
        before={"graph.yml": 'nodes: [{"node_file": "old.sql"}]', "old.sql": "foo"},
        zip={"graph.yml": 'nodes: [{"d/node_file": "node.sql"}]', "d/node.sql": "foo"},
        src="d/node.sql",
        dst="old.sql",
        after={"graph.yml": 'nodes: [{"node_file": "old.sql"}]', "old.sql": "foo"},
    )


def test_err_add_changed_node(tmp_path: Path):
    with pytest.raises(FileOverwriteError) as exc_info:
        do_add_zip_test(
            tmp_path,
            before={"graph.yml": 'nodes: [{"node_file": "old.sql"}]', "old.sql": "foo"},
            zip={"graph.yml": 'nodes: [{"node_file": "node.sql"}]', "node.sql": "bar"},
            src="node.sql",
            dst="old.sql",
        )
    assert exc_info.value.file_path == tmp_path / "old.sql"


def test_overwrite_node(tmp_path: Path):
    do_add_zip_test(
        tmp_path,
        before={"graph.yml": 'nodes: [{"node_file": "old.sql"}]', "old.sql": "foo"},
        zip={"graph.yml": 'nodes: [{"node_file": "node.sql"}]', "node.sql": "bar"},
        src="node.sql",
        dst="old.sql",
        after={"graph.yml": 'nodes: [{"node_file": "old.sql"}]', "old.sql": "bar"},
        overwrite=True,
    )


def test_full_clone(tmp_path: Path):
    do_add_zip_test(
        tmp_path,
        before={},
        zip={"graph.yml": 'nodes: [{"node_file": "node.sql"}]', "node.sql": "bar"},
        src="graph.yml",
        dst="graph.yml",
        after={"graph.yml": 'nodes: [{"node_file": "node.sql"}]', "node.sql": "bar"},
        overwrite=True,
    )


def test_add_subgraph(tmp_path: Path):
    do_add_zip_test(
        tmp_path,
        before={
            "graph.yml": """
            nodes:
              - node_file: s.sql
            """,
            "s.sql": "foo",
        },
        zip={
            "graph.yml": 'nodes: [{"node_file": "sub/graph.yml"}]',
            "sub/graph.yml": 'nodes: [{"node_file": "s.sql"}]',
            "sub/s.sql": "bar",
        },
        src="sub/graph.yml",
        dst="new/graph.yml",
        after={
            "graph.yml": """
            nodes:
              - node_file: s.sql
              - node_file: new/graph.yml
                id: <id>
            """,
            "s.sql": "foo",
            "new/graph.yml": 'nodes: [{"node_file": "s.sql"}]',
            "new/s.sql": "bar",
        },
    )


def test_add_single_file(tmp_path: Path):
    before = {
        "graph.yml": """
        nodes:
          - node_file: s.sql
        """,
        "s.sql": "foo",
    }
    after = {
        "graph.yml": """
         nodes:
           - node_file: s.sql
           - node_file: new.sql
             id: <id>
         """,
        "s.sql": "foo",
        "new.sql": "bar",
    }
    setup_manifest(tmp_path, before)
    editor = GraphDirectoryEditor(tmp_path, overwrite=False)
    content = "bar"
    editor.add_node_from_file("new.sql", io.BytesIO(content.encode()))
    assert_files(tmp_path, after)
    manifest = editor.build_manifest()
    assert {n.file_path_to_node_script_relative_to_root for n in manifest.nodes} == {
        "s.sql",
        "new.sql",
    }


def do_add_zip_test(
    tmp_path: Path,
    before: Dict[str, str],
    zip: Dict[str, str],
    src: str,
    dst: str,
    after: Dict[str, str] = None,
    overwrite: bool = False,
):
    if before:
        setup_manifest(tmp_path, before)
    editor = GraphDirectoryEditor(tmp_path, overwrite=overwrite)
    with create_zip(zip) as z:
        editor.add_node_from_zip(src, dst, z)
    if after:
        assert_files(tmp_path, after)


def assert_files(root: Path, files: Dict[str, str]):
    for path, content in files.items():
        p = root / path
        assert p.is_file(), f"{p} does not exist"
        content = textwrap.dedent(content).strip()
        actual = p.read_text().strip()
        if path.endswith(".yml"):
            actual = re.sub(r"id: \w+", "id: <id>", actual)
        assert actual == content
    for p in root.rglob("*"):
        name = "/".join(p.relative_to(root).parts)
        if p.is_file():
            assert name in files, f"extra file {p}"


def create_zip(files: Dict[str, str]) -> zipfile.ZipFile:
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w") as f:
        for path, content in files.items():
            content = textwrap.dedent(content).strip()
            f.writestr(path, content)
    b.seek(0)
    return zipfile.ZipFile(b, "r")
