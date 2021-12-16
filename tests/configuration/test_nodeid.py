import random

from basis.configuration.path import NodeId


def test_random():
    random.seed(0)
    assert NodeId.random() == "yxlrjbhy"


def test_from_name():
    assert NodeId.from_name("x", None) == "zywrijde"
    assert NodeId.from_name("x", NodeId("12345678")) == "a4lxkkmh"
