from dataclasses import dataclass
from dataclasses import dataclass
from typing import Callable, Any, TypeVar, Type

from patterns.node._methods import (
    InputTableMethods,
    OutputTableMethods,
    InputStreamMethods,
    OutputStreamMethods,
    StateMethods,
)


def _mixin_attrs():
    def init(self, param_name):
        self.param_name = param_name

    return {"__init__": init}


# In order to support type inference with both `Parameter` and `Parameter()` forms, we
# need to use the metaclass mechanisms to return a type rather than a value when the
# classes are invoked. We add __init__ to the generated classes so that invoking them
# generates new class objects with the description etc. set. This does mean that
# invoking the class will recursively produce more classes and never an actual instance,
# but that fine for how we're using them.
class _InputMeta(type):
    def __new__(
        mcs,
        description: str = None,
        schema: str = None,
        required: bool = True,
    ):
        return super().__new__(mcs, mcs.__name__, (mcs,), _mixin_attrs())

    # noinspection PyMissingConstructor
    def __init__(
        cls,
        description: str = None,
        schema: str = None,
        required: bool = True,
    ):
        cls.description = description
        cls.schema = schema
        cls.required = required


class _OutputMeta(type):
    def __new__(
        mcs,
        description: str = None,
        schema: str = None,
    ):
        return super().__new__(mcs, mcs.__name__, (mcs,), _mixin_attrs())

    # noinspection PyMissingConstructor
    def __init__(
        cls,
        description: str = None,
        schema: str = None,
    ):
        cls.description = description
        cls.schema = schema


class _StateMeta(type):
    def __new__(mcs):
        return super().__new__(mcs, mcs.__name__, (mcs,), _mixin_attrs())

    # noinspection PyMissingConstructor
    def __init__(cls):
        pass


class InputTable(_InputMeta, InputTableMethods):
    pass


class OutputTable(_OutputMeta, OutputTableMethods):
    pass


class InputStream(_InputMeta, InputStreamMethods):
    pass


class OutputStream(_OutputMeta, OutputStreamMethods):
    pass


class Table(InputTableMethods, OutputTableMethods):
    def __init__(
        self,
        name: str,
        mode: str = "r",
        description: str = None,
        schema: str = None,
        required: bool = True,
    ):
        pass


class Stream(InputStreamMethods, OutputStreamMethods):
    def __init__(
        self,
        name: str,
        mode: str = "r",
        description: str = None,
        schema: str = None,
        required: bool = True,
    ):
        pass


class State(_StateMeta, StateMethods):
    pass


T = TypeVar("T")


class _Parameter(str):
    def __call__(
        self,
        description: str = None,
        type: Type[T] = str,
        default: Any = None,
    ) -> T:
        pass


Parameter = _Parameter()


@dataclass(frozen=True)
class NodeFunction:
    function: Callable


def node(function: Callable):
    """A decorator that registers a function to execute when a node runs"""
    return NodeFunction(function)
