import abc
import inspect
from dataclasses import dataclass
from typing import Callable, List, Union, Any

from commonmodel import Schema

from basis.node._methods import (
    InputTableMethods,
    OutputTableMethods,
    ParameterMethods,
    InputStreamMethods,
    OutputStreamMethods,
    StateMethods,
)


class _NodeInterfaceEntry:
    param_name: str


def _mixin_attrs():
    def init(self, param_name):
        self.param_name = param_name

    return {
        "__init__": init,
    }


# In order to support type inference with both `Parameter` and `Parameter()` forms, we need to use the metaclass
# mechanisms to return a type rather than a value when the classes are invoked. We add __init__ to the generated classes
# so that invoking them generates new class objects with the description etc. set. This does mean that invoking the
# class will recursively produce more classes and never an actual instance, but that fine for how we're using them.
class _InputMeta(type, _NodeInterfaceEntry):
    def __new__(
        mcs,
        description: str = None,
        schema: Union[str, Schema] = None,
        required: bool = True,
    ):
        return super().__new__(mcs, mcs.__name__, (mcs,), _mixin_attrs())

    # noinspection PyMissingConstructor
    def __init__(
        cls,
        description: str = None,
        schema: Union[str, Schema] = None,
        required: bool = True,
    ):
        cls.description = description
        cls.schema = schema
        cls.required = required


class _OutputMeta(type, _NodeInterfaceEntry):
    def __new__(
        mcs, description: str = None, schema: Union[str, Schema] = None,
    ):
        return super().__new__(mcs, mcs.__name__, (mcs,), _mixin_attrs())

    # noinspection PyMissingConstructor
    def __init__(
        cls, description: str = None, schema: Union[str, Schema] = None,
    ):
        cls.description = description
        cls.schema = schema


class _ParameterMeta(type, _NodeInterfaceEntry):
    def __new__(
        mcs, description: str = None, type: str = None, default: Any = None,
    ):
        return super().__new__(mcs, mcs.__name__, (mcs,), _mixin_attrs())

    # noinspection PyMissingConstructor
    def __init__(
        cls, description: str = None, type: str = None, default: Any = None,
    ):
        cls.description = description
        cls.type = type
        cls.default = default


class InputTable(_InputMeta, InputTableMethods):
    pass


class OutputTable(_OutputMeta, OutputTableMethods):
    pass


class InputStream(_InputMeta, InputStreamMethods):
    pass


class OutputStream(_OutputMeta, OutputStreamMethods):
    pass


class Parameter(_ParameterMeta, ParameterMethods):
    pass


class State(_ParameterMeta, StateMethods):
    pass


@dataclass(frozen=True)
class NodeFunction:
    function: Callable
    args: List[_NodeInterfaceEntry]


def node(function: Callable):
    """A decorator that registers a function to execute when a node runs"""

    sig = inspect.signature(function)

    args = []
    for (name, param) in sig.parameters.items():
        value = param.default
        if value is inspect.Parameter.empty:
            raise TypeError(f"{name} must have a type (e.g. {name}=InputTable)")

        if inspect.isclass(value) and issubclass(value, _NodeInterfaceEntry):
            if value.__class__ in (type, abc.ABCMeta):
                value = value()
            # noinspection PyCallingNonCallable
            args.append(value(name))
        else:
            raise TypeError(f"{name} is not a valid node parameter type")
    return NodeFunction(function, args)
