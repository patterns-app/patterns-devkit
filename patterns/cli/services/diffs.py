import difflib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Dict
from zipfile import ZipFile

from rich.markdown import Markdown

from patterns.cli.services.output import sprint


@dataclass
class DiffResult:
    added: List[str]
    removed: List[str]
    changed: Dict[str, Iterator[str]]

    @property
    def is_not_empty(self) -> bool:
        return bool(self.added or self.removed or self.changed)

    @property
    def is_empty(self) -> bool:
        return not self.is_not_empty


def get_diffs_between_zip_and_dir(zf: ZipFile, root: Path) -> DiffResult:
    """Return a map of {filename: diff} where the contents differ between zf and root"""
    result = DiffResult([], [], {})
    all_in_zip = set()
    for zipinfo in zf.infolist():
        dst = root / zipinfo.filename
        if zipinfo.is_dir():
            continue
        all_in_zip.add(zipinfo.filename)
        if not dst.is_file():
            result.removed.append(zipinfo.filename)
            continue
        zip_content = zf.read(zipinfo).decode().splitlines(keepends=False)
        fs_content = dst.read_text().splitlines(keepends=False)
        if zip_content != fs_content:
            diff = difflib.unified_diff(
                zip_content,
                fs_content,
                fromfile=f"<remote> {zipinfo.filename}",
                tofile=f"<local>  {zipinfo.filename}",
                lineterm="",
            )
            result.changed[zipinfo.filename] = diff
    for base, _, names in os.walk(root):
        for name in names:
            fname = (Path(base) / name).relative_to(root).as_posix()
            if fname not in all_in_zip:
                result.added.append(fname)

    return result


def print_diffs(diffs: DiffResult, context: bool, full: bool):
    if full:
        if diffs.added:
            sprint("Added:")
            sprint(Markdown("\n".join(f"- {a}" for a in diffs.added), style="success"))
            print()
        if diffs.removed:
            sprint("Deleted:")
            sprint(Markdown("\n".join(f"- {a}" for a in diffs.removed), style="error"))
            print()
    if not diffs.changed:
        return
    sprint("Modified:")
    if not context:
        sprint(Markdown("\n".join(f"- {a}" for a in diffs.changed), style="info"))
        return

    print()
    diff = "\n\n".join("\n".join(d) for d in diffs.changed.values())
    sprint(
        Markdown(
            f"""
```diff
{diff}
```
""",
            code_theme="vim",
        )
    )
