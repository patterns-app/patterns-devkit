import os
import tempfile
from pathlib import Path
from typing import Tuple

import pytest
from basis.cli.app import app
from basis.cli.commands.generate import GenerateCommand
from cleo import Application, CommandTester
from cleo.testers import command_tester
from dcp.utils.common import rand_str

IS_CI = os.environ.get("CI")


def set_tmp_dir():
    dr = tempfile.mkdtemp()
    os.chdir(dr)
    return dr


def get_test_command(name: str, dirpath: str) -> CommandTester:
    command = app.find(name)
    command_tester = CommandTester(command)
    return command_tester


def test_generate():
    if not IS_CI:
        dr = set_tmp_dir()
        command_tester = get_test_command("new", dr)
        name = f"testspace_{rand_str(4)}".lower()
        command_tester.execute(f"dataspace {name}")
        assert os.path.exists(Path(dr) / "basis.yml")
        assert os.path.exists(Path(dr) / name)
        assert os.path.exists(Path(dr) / name / "__init__.py")
        assert os.path.exists(Path(dr) / name / "functions")
        assert os.path.exists(Path(dr) / name / "schemas")
        assert os.path.exists(Path(dr) / name / "flows")

        fn_name = "function1"
        command_tester.execute(f"function {fn_name}")
        assert os.path.exists(Path(dr) / name / "functions" / fn_name)
        assert os.path.exists(Path(dr) / name / "functions" / fn_name / f"{fn_name}.py")


def test_run():
    if not IS_CI:
        dr = set_tmp_dir()
        ds = (
            """
        storages:
          - sqlite:///%s/.basis.db
        graph:
          nodes:
            - key: import_records
              function: core.import_records
              params:
                records: '[{"f1":1, "f2":2}]'
        """
            % dr
        )
        with open(Path(dr) / "basis.yml", "w") as f:
            f.write(ds)
        command_tester = get_test_command("run", dr)
        command_tester.execute()
        command_tester = get_test_command("output", dr)
        command_tester.execute("import_records")
        # out = command_tester.io.fetch_output()
        # print(out)
        # assert out == '[{"f1":1, "f2":2}]'
