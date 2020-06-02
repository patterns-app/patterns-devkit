from __future__ import annotations

import logging
from collections import defaultdict
from typing import DefaultDict, Dict, Generic, Iterable, List, TypeVar, cast

logger = logging.getLogger(__name__)


class RegistryError(Exception):
    pass


T = TypeVar("T")
U = TypeVar("U")


class Registry(Generic[T]):
    def __init__(
        self, object_key_attr: str = "key", error_on_duplicate: bool = True,
    ):
        self.object_key_attr = object_key_attr
        self.error_on_duplicate = error_on_duplicate
        self._registry: Dict[str, T] = {}

    def __repr__(self):
        return repr(self._registry)

    def __len__(self):
        return len(self._registry)

    def __getattr__(self, item) -> T:
        return self.get(item)

    def __dir__(self) -> List[str]:
        d = super().__dir__()
        return list(set(d) | set(self._registry))

    def merge(self, other: Registry):
        self._registry.update(other._registry)

    def get_key(self, obj):
        key = getattr(obj, self.object_key_attr, None)
        if not key:
            raise RegistryError(
                f"`{self.object_key_attr}` attribute required to register {obj}"
            )
        return key

    def register(self, obj: T):
        key = self.get_key(obj)
        if key in self._registry:
            if self.error_on_duplicate:
                raise RegistryError(f"{key} already registered")
            else:
                logger.info(f"{key} already registered")
        self._registry[key] = obj

    def register_all(self, objs: Iterable[T]):
        for obj in objs:
            self.register(obj)

    def get(self, key: str) -> T:
        return self._registry[key]

    def all(self) -> Iterable[T]:
        return self._registry.values()


class UriRegistry(Registry, Generic[T]):
    def __init__(
        self, object_key_attr: str = "uri", error_on_duplicate: bool = True,
    ):
        super().__init__(object_key_attr, error_on_duplicate)
        self._module_key_registry: DefaultDict[str, List[str]] = defaultdict(list)

    def get_uri(self, obj):
        uri = getattr(obj, self.object_key_attr, None)
        if not uri:
            raise RegistryError(
                f"`{self.object_key_attr}` attribute required to register {obj}"
            )
        return uri

    def register(self, obj: T):
        uri = self.get_uri(obj)
        if uri in self._registry:
            if self.error_on_duplicate:
                raise RegistryError(f"{uri} already registered")
            else:
                logger.info(f"{uri} already registered")
        self._registry[uri] = obj
        try:
            module, key = uri.split(".")
        except ValueError:
            raise RegistryError(f"Invalid URI '{uri}'")
        self._module_key_registry[key].append(module)

    def get(self, uri_or_key: str, module_order: List[str] = None) -> T:
        if self.is_qualified(uri_or_key):
            return self._registry[uri_or_key]
        module_keys = self._module_key_registry[uri_or_key]
        if len(module_keys) == 1:
            module_key = module_keys[0]
        else:
            if not module_order:
                raise RegistryError(
                    f"Ambiguous URI {uri_or_key} and no module order specified"
                )
            try:
                module_key = [m for m in module_order if m in module_keys][0]
            except (IndexError, TypeError):
                raise RegistryError(
                    f"Ambiguous {self.object_key_attr} in registry lookup: {uri_or_key} in {module_keys} (ordered {module_order})"
                )
        return self.get(module_key + "." + uri_or_key)

    def is_qualified(self, key: str) -> bool:
        return "." in key

    def merge(self, other: Registry):
        other = cast(UriRegistry, other)
        self._registry.update(other._registry)
        for k, v in other._module_key_registry.items():
            self._module_key_registry[k].extend(v)
