import io
from pathlib import Path
from zipfile import ZipFile

from patterns.cli.services.diffs import get_diffs_between_zip_and_dir


def test_diffs(tmp_path: Path):
    txt = tmp_path / "t.txt"
    txt2 = tmp_path / "t2.txt"
    bin = tmp_path / "b.bin"
    txt.write_text("foo\nbar\nbaz")
    txt2.write_text("foo\nbar\nbaz")
    bin.write_bytes(b"\xf1\xf2\xf3")
    zfbytes = io.BytesIO()
    with ZipFile(zfbytes, "w") as zf:
        zf.write(txt, "t.txt")
        zf.write(txt2, "t2.txt")
        zf.write(bin, "b.bin")

        diffs = get_diffs_between_zip_and_dir(zf, tmp_path, False)
        assert diffs.added == []
        assert diffs.removed == []
        assert diffs.changed == {}

        txt2.unlink()
        (tmp_path / "t3.txt").write_text("t3")
        txt.write_text("foo\nbar2\nbaz\nqux")
        bin.write_bytes(b"\xf1\xff")

        diffs = get_diffs_between_zip_and_dir(zf, tmp_path, False)
        assert diffs.added == ["t3.txt"]
        assert diffs.removed == ["t2.txt"]
        changed = {k: list(v) for k, v in diffs.changed.items()}
        assert changed == {
            "b.bin": [
                "--- <remote> b.bin",
                "+++ <local>  b.bin",
                "Binary contents differ",
            ],
            "t.txt": [
                "--- <remote> t.txt",
                "+++ <local>  t.txt",
                "@@ -1,3 +1,4 @@",
                " foo",
                "-bar",
                "+bar2",
                " baz",
                "+qux",
            ],
        }

        diffs = get_diffs_between_zip_and_dir(zf, tmp_path, True)
        assert diffs.added == ["t2.txt"]
        assert diffs.removed == ["t3.txt"]
        changed = {k: list(v) for k, v in diffs.changed.items()}
        assert changed == {
            "b.bin": [
                "--- <remote> b.bin",
                "+++ <local>  b.bin",
                "Binary contents differ",
            ],
            "t.txt": [
                "--- <remote> t.txt",
                "+++ <local>  t.txt",
                "@@ -1,4 +1,3 @@",
                " foo",
                "-bar2",
                "+bar",
                " baz",
                "-qux",
            ],
        }
