from types import ModuleType
from typing import List, Type, TypeVar

T = TypeVar("T")


def find_all_of_type_in_module(module: ModuleType, typ: Type[T]) -> List[T]:
    return [
        getattr(module, n) for n in dir(module) if isinstance(getattr(module, n), typ)
    ]
