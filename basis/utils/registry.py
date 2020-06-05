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

    def merge(self, other: Registry, overwrite: bool = False):
        if overwrite:
            self._registry.update(other._registry)
        else:
            for k, v in other._registry.items():
                if k not in self._registry:
                    self._registry[k] = v

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

    def delete(self, key: str):
        del self._registry[key]

    def all(self) -> Iterable[T]:
        return self._registry.values()


class UriRegistry(Registry, Generic[T]):
    def __init__(
        self, object_key_attr: str = "uri", error_on_duplicate: bool = True,
    ):
        super().__init__(object_key_attr, error_on_duplicate)
        self._module_key_registry: DefaultDict[str, List[str]] = defaultdict(list)

    def register(self, obj: T):
        uri = self.get_key(obj)
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
        self.register_module_key(key, module)

    def register_module_key(self, key: str, module: str):
        if module not in self._module_key_registry[key]:
            self._module_key_registry[key].append(module)

    def normalize_uri(
        self, uri_or_key: str, module_precedence: List[str] = None
    ) -> str:
        if self.is_qualified(uri_or_key):
            return uri_or_key
        key = uri_or_key
        module_keys = self._module_key_registry[key]
        if len(module_keys) == 1:
            module_key = module_keys[0]
        else:
            if not module_precedence:
                raise RegistryError(
                    f"Ambiguous key {key} and no module precedence specified ({module_keys})"
                )
            try:
                module_key = [m for m in module_precedence if m in module_keys][0]
            except (IndexError, TypeError):
                raise RegistryError(
                    f"Ambiguous/missing key in registry lookup: {key} in modules {module_keys} (modules checked: {module_precedence})"
                )
        return module_key + "." + uri_or_key

    def get(self, uri_or_key: str, module_precedence: List[str] = None) -> T:
        key = self.normalize_uri(uri_or_key, module_precedence)
        return self._registry[key]

    def delete(self, uri_or_key: str, module_precedence: List[str] = None):
        key = self.normalize_uri(uri_or_key, module_precedence)
        del self._registry[key]

    def is_qualified(self, key: str) -> bool:
        return "." in key

    def merge(self, other: Registry, overwrite: bool = False):
        other = cast(UriRegistry, other)
        for other_obj in other.all():
            try:
                self.register(other_obj)
            except RegistryError:
                if overwrite:
                    self.delete(other_obj.uri)
                    self.register(other_obj)
        for k, v in other._module_key_registry.items():
            for m in v:
                self.register_module_key(k, m)
