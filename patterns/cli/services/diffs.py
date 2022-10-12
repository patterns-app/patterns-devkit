from pathlib import Path
from zipfile import ZipFile


def get_conflicts_between_zip_and_dir(zf: ZipFile, root: Path) -> list[str]:
    """Return a list of filenames where the contents differ between zf and root"""
    conflicts = []
    for zipinfo in zf.infolist():
        dst = root / zipinfo.filename
        if zipinfo.is_dir() or not dst.exists():
            continue
        if dst.is_file():
            # normalize line endings
            new = zf.read(zipinfo).decode().replace("\r\n", "\n")
            if dst.read_text().replace("\r\n", "\n") != new:
                conflicts.append(zipinfo.filename)
        else:
            conflicts.append(zipinfo.filename)
    return conflicts
