from __future__ import annotations

from collections import OrderedDict
from typing import Generic, Iterable, Optional, Type, TypeVar

from sqlalchemy import types


class ClassBasedEnum:
    pass


ClassBasedEnumType = Type[ClassBasedEnum]


class ClassRegistry:
    def __init__(self):
        self._registry: OrderedDict[str, ClassBasedEnumType] = OrderedDict()

    def register(self, cls: ClassBasedEnumType):
        self._registry[self.get_key(cls)] = cls

    def get_key(self, cls: ClassBasedEnumType) -> str:
        return cls.__name__

    def get(self, key: str) -> Optional[ClassBasedEnumType]:
        return self._registry.get(key)

    def has(self, cls: ClassBasedEnumType) -> bool:
        return self.get_key(cls) in self._registry

    def all(
        self, base: Optional[ClassBasedEnumType] = None
    ) -> Iterable[ClassBasedEnumType]:
        a = self._registry.values()
        if base is not None:
            a = (v for v in a if issubclass(v, base))
        return a

    def __getitem__(self, item: str) -> ClassBasedEnumType:
        return self._registry[item]


global_registry = ClassRegistry()


class ClassBasedEnumSqlalchemyType(types.TypeDecorator):
    impl = types.Unicode

    def __init__(self, length: int = 128):
        super().__init__(length)

    def process_bind_param(self, value, dialect):
        # if not issubclass(value, ClassBasedEnum):
        #     raise TypeError(value)
        return global_registry.get_key(value)

    def process_result_value(self, value, dialect) -> ClassBasedEnumType:
        return global_registry[value]
