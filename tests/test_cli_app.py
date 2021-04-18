import pytest
from cleo import Application, CommandTester
from snapflow.cli.commands.generate import GenerateCommand


def test_generate():
    application = Application()
    application.add(GenerateCommand())
    command = application.find("new")
    command_tester = CommandTester(command)
    assert command_tester is not None
    # TODO
    # command_tester.execute("module")
