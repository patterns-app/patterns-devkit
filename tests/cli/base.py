import os
import tempfile

from basis.cli.app import app
from cleo import CommandTester


IS_CI = os.environ.get("CI")


def set_tmp_dir():
    dr = tempfile.mkdtemp()
    os.chdir(dr)
    return dr


def get_test_command(name: str) -> CommandTester:
    command = app.find(name)
    command_tester = CommandTester(command)
    return command_tester

