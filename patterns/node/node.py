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
# but that's fine for how we're using them.
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
        cls._schema = schema
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
        cls._schema = schema


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
        """Table is a thin abstraction over a database table that
        provides a stable reference across versions of the table.
        
        Args:
            name: The Patterns name for the table. The actual database table
                on disk will include this name and a hash.
            mode: Whether to use the table in "read" mode ("r") or "write" mode ("w")
            description: An optional short description of this table
            schema: An optional explicit Schema for this table. If not provided the
                schema will be inferred, or can be set with the table's `init` method.
            required: Whether this table is a required table for the operation of the
                node, or is optional.
        """
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
    """
    State is a wrapper around a Table that supports quickly storing
    and retrieving single values from the database.
    """
    pass


T = TypeVar("T")


class _Parameter(str):
    def __call__(
        self,
        description: str = None,
        type: Type[T] = str,
        default: Any = "MISSING",
    ) -> T:
        """Parameters let a python script take values from the end user / UI.
        
        Allowed parameter types:
        * str
        * int
        * float
        * bool
        * datetime
        * date
        * list
        * Connection
        
        Args:
            description: Description / help text
            type: should be the actual python type, e.g. `type=str` or `type=datetiem`
            default: default value. If not set explicitly, the parameter is assumed to be required.
                May be set to None
        """
        pass


Parameter = _Parameter()


class Connection(dict):
    def __init__(self, connection_type: str):
        super().__init__()


@dataclass(frozen=True)
class NodeFunction:
    function: Callable


def node(function: Callable):
    """A decorator that registers a function to execute when a node runs"""
    return NodeFunction(function)