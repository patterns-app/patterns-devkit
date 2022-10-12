import difflib
from pathlib import Path
from typing import Iterator
from zipfile import ZipFile

from rich.markdown import Markdown

from patterns.cli.services.output import sprint


def get_diffs_between_zip_and_dir(zf: ZipFile, root: Path) -> dict[str, Iterator[str]]:
    """Return a map of {filename: diff} where the contents differ between zf and root"""
    conflicts = {}
    for zipinfo in zf.infolist():
        dst = root / zipinfo.filename
        if zipinfo.is_dir() or not dst.is_file():
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
            conflicts[zipinfo.filename] = diff
    return conflicts


def print_diffs(diffs: dict[str, Iterator[str]], full: bool):
    if not full:
        for diff in diffs.keys():
            sprint(f"\t[error]{diff}")
        return

    diff = "\n\n".join("\n".join(d) for d in diffs.values())
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
