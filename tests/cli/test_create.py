import os
import random
import tempfile
from pathlib import Path
from typing import Tuple

import pytest
from basis.cli.app import app
from basis.cli.commands.generate import GenerateCommand
from cleo import Application, CommandTester
from cleo.testers import command_tester
from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_generate_project():
    # Certain file actions not possible in CI env now
    if not IS_CI:
        dr = set_tmp_dir()
        command_tester = get_test_command("create")
        name = f"test_{random.randint(0,10000)}".lower()
        inputs = "\n".join([name]) + "\n"
        command_tester.execute(f"project", inputs=inputs)
        assert os.path.exists(Path(dr) / name / "basis.yml")
        assert os.path.exists(Path(dr) / name / "README.md")
        assert os.path.exists(Path(dr) / name / "requirements.txt")

        # With path
        path = "pth/projname"
        inputs = "\n"
        command_tester.execute(f"project {path}", inputs=inputs)
        assert os.path.exists(Path(dr) / path / "basis.yml")


def test_generate_app():
    # Certain file actions not possible in CI env now
    if not IS_CI:
        dr = set_tmp_dir()
        command_tester = get_test_command("create")
        name = f"test_{random.randint(0,10000)}".lower()
        pth = "proj/app1/" + name
        inputs = "\n"
        command_tester.execute(f"app {pth}", inputs=inputs)
        assert os.path.exists(Path(dr) / pth / "app.yml")


def test_generate_component():
    # Certain file actions not possible in CI env now
    if not IS_CI:
        dr = set_tmp_dir()
        command_tester = get_test_command("create")
        name = f"test_{random.randint(0,10000)}.py".lower()
        pth = "proj/app1/" + name
        inputs = "\n".join(["\n", "python"]) + "\n"
        command_tester.execute(f"component {pth}", inputs=inputs)
        assert os.path.exists(Path(dr) / pth)

