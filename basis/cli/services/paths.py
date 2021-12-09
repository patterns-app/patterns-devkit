from pathlib import Path


def is_relative_to(self: Path, other: Path) -> bool:
    """Backport of Path.is_relative_to, which was added in python3.9"""
    try:
        self.relative_to(other)
        return True
    except ValueError:
        return False
