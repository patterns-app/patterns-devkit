from snapflow.cli.commands.generate import GenerateCommand
import pytest

from cleo import Application
from cleo import CommandTester


def test_generate():
    application = Application()
    application.add(GenerateCommand())
    command = application.find("new")
    command_tester = CommandTester(command)
    # command_tester.execute("module")

