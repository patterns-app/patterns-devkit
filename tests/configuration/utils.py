import textwrap
from pathlib import Path
from typing import Dict


def setup_graph_files(root: Path, files: Dict[str, str]):
    for path, content in files.items():
        content = textwrap.dedent(content).strip()
        if path.endswith(".py"):
            if not content.startswith("@node"):
                content = f"@node\ndef generated_node(\n{content}\n):\n    pass"
            content = "from patterns import *\n\n" + content
        abspath = root / path
        if len(Path(path).parts) > 1:
            abspath.parent.mkdir(parents=True, exist_ok=True)
        abspath.write_text(content)
