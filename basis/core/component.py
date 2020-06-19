from __future__ import annotations

import os
import re
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass, fields, replace
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    DefaultDict,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
)

from basis.utils.common import StringEnum

if TYPE_CHECKING:
    from basis.core.data_function import (
        DataFunctionLike,
        DataFunction,
    )
    from basis.core.module import BasisModule, DEFAULT_LOCAL_MODULE
    from basis.core.typing.object_type import ObjectTypeLike, ObjectType
    from basis.core.external import ExternalProvider, ExternalResource

VERSIONED_URIS = (
    False  # TODO: think about when we would want to use versioned URIs (never?)
)

re_comp_id = re.compile(
    r"((?P<component_type>\w+):)?((?P<module_name>\w+)\.)?(?P<name>\w+)(@(?P<version>.+))?"
)


class ComponentBase:
    ctype: ComponentType


class ComponentType(StringEnum):
    DataFunction = "dfn"
    ObjectType = "otype"
    External = "ext"
    # ExternalResource = "ext"


class OverwriteBehavior(StringEnum):
    OVERWRITE = "overwrite"
    ERROR = "error"
    IGNORE = "ignore"
    MERGE = "merge"


@dataclass(frozen=True)
class UriMixin:
    name: str
    module_name: Optional[str]
    version: Optional[str]


@dataclass(frozen=True)
class ComponentUri(ComponentBase):
    component_type: ComponentType
    name: str
    module_name: Optional[str]
    version: Optional[str]

    @classmethod
    def from_str(cls, uri: str, **kwargs: Any) -> ComponentUri:
        m = re_comp_id.match(uri)
        if not m:
            raise
        d = m.groupdict()
        d.update(kwargs)
        if isinstance(d["component_type"], str):
            d["component_type"] = ComponentType(d["component_type"])
        return ComponentUri(**d)

    @property
    def unversioned_uri(self):
        from basis.core.module import DEFAULT_LOCAL_MODULE

        module = self.module_name
        if not module:
            module = DEFAULT_LOCAL_MODULE.name
        k = module + "." + self.name
        return k

    @property
    def versioned_uri(self):
        uri = self.unversioned_uri
        if self.version:
            uri += "@" + str(self.version)
        return uri

    @property
    def uri(self):
        if VERSIONED_URIS:
            return self.versioned_uri
        return self.unversioned_uri

    @property
    def full_uri(self):
        return self.component_type.value + ":" + self.versioned_uri

    def clone(self, **kwargs: Any) -> ComponentUri:
        d = {f.name: getattr(self, f.name) for f in fields(self)}
        d.update(kwargs)
        return self.__class__(**d)

    def associate_with_module(self, module: BasisModule) -> ComponentUri:
        return self.clone(module_name=module.name)

    def merge(self, other: ComponentUri):
        # TODO: should this raise an error or be a no op by default? Probably error
        pass
        # raise NotImplementedError


UriLike = Union[ComponentUri, str]


def ensure_uri(uri_like: UriLike, **kwargs: Any) -> ComponentUri:
    if isinstance(uri_like, ComponentUri):
        return uri_like
    if isinstance(uri_like, str):
        return ComponentUri.from_str(uri_like, **kwargs)
    raise TypeError(uri_like)


C = TypeVar("C", bound=ComponentUri)


class RegistryError(Exception):
    pass


class ComponentRegistry:
    def __init__(
        self,
        overwrite_behavior: Union[OverwriteBehavior, str] = OverwriteBehavior.MERGE,
        key_attrs: List[str] = ["component_type", "name", "module_name"],
    ):
        self._uri_to_component: Dict[str, ComponentUri] = {}
        self._name_to_modules: DefaultDict[str, List[str]] = defaultdict(list)
        if isinstance(overwrite_behavior, str):
            overwrite_behavior = OverwriteBehavior(overwrite_behavior)
        self._overwrite_behavior = overwrite_behavior
        self._key_attrs = key_attrs

    def __repr__(self):
        return repr(self._uri_to_component)

    def __len__(self):
        return len(self._uri_to_component)

    def __getattr__(self, name: str) -> ComponentUri:
        """Allows for direct name based access to registry items. eg reg.TestType1 """
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __dir__(self) -> List[str]:
        d = super().__dir__()
        return list(set(d) | set(self._uri_to_component.keys()))

    def get_key(self, uri: ComponentUri) -> str:
        attrs = [getattr(uri, a) for a in self._key_attrs]
        if any([a is None for a in attrs]):
            raise RegistryError(f"Key attrs contains None {attrs}")
        return "|".join(str(getattr(uri, a)) for a in self._key_attrs)

    def register(self, c: ComponentUri):
        assert c.module_name  # TODO
        key = self.get_key(c)
        if key in self._uri_to_component:
            if self._overwrite_behavior == OverwriteBehavior.ERROR:
                raise RegistryError(key)
            if self._overwrite_behavior == OverwriteBehavior.IGNORE:
                return
            if self._overwrite_behavior == OverwriteBehavior.MERGE:
                self._uri_to_component[key].merge(c)
                return
            if self._overwrite_behavior == OverwriteBehavior.OVERWRITE:
                pass
        self._uri_to_component[key] = c
        self.register_name_to_module(c)

    def register_name_to_module(self, uri: ComponentUri):
        assert uri.module_name
        # Only add to list if not already there
        if uri.module_name not in self._name_to_modules[uri.name]:
            self._name_to_modules[uri.name].append(uri.module_name)

    def qualify_uri(
        self, uri: ComponentUri, module_precedence: List[str] = None
    ) -> ComponentUri:
        if uri.module_name:
            return uri
        eligible_module_names = self._name_to_modules[uri.name]
        if len(eligible_module_names) == 1:
            module_name = eligible_module_names[0]
        else:
            if not module_precedence:
                # TODO: make two separate errors...
                raise RegistryError(
                    f"Ambiguous or missing key {uri.name} and no module precedence specified ({eligible_module_names})"
                )
            try:
                module_name = [
                    m for m in module_precedence if m in eligible_module_names
                ][0]
            except (IndexError, TypeError):
                raise RegistryError(
                    f"Ambiguous or missing key in registry lookup: {uri.name} in modules {eligible_module_names} (modules checked: {module_precedence})"
                )
        return uri.clone(module_name=module_name)

    def __getitem__(self, uri: UriLike) -> ComponentUri:
        uri = ensure_uri(uri)
        uri = self.qualify_uri(uri)
        key = self.get_key(uri)
        return self._uri_to_component[key]

    def get_key_from_uri_like(
        self, uri: UriLike, module_precedence: List[str] = None, **kwargs: Any
    ) -> str:
        uri = ensure_uri(uri, **kwargs)
        uri = self.qualify_uri(uri, module_precedence)
        return self.get_key(uri)

    def get(
        self,
        uri: UriLike,
        default: ComponentUri = None,
        module_precedence: List[str] = None,
        **kwargs: Any,
    ) -> Optional[ComponentUri]:
        k = self.get_key_from_uri_like(
            uri, module_precedence=module_precedence, **kwargs
        )
        return self._uri_to_component.get(k, default)

    def delete(self, uri: UriLike, **kwargs: Any):
        k = self.get_key_from_uri_like(uri, **kwargs)
        del self._uri_to_component[k]

    def all(self) -> Iterable[ComponentUri]:
        return self._uri_to_component.values()

    def merge(self, other: ComponentRegistry):
        for other_obj in other.all():
            self.register(other_obj)


class ComponentView(Generic[C]):
    components: Dict[str, C]

    def __init__(self, components: List[C]):
        self.components = {}
        for c in components:
            self.components[c.name] = c

    def __getattr__(self, name: str) -> C:
        """Allows for direct name based access to registry items. eg reg.TestType1 """
        try:
            return self.components[name]
        except KeyError:
            raise AttributeError(name)

    def __repr__(self):
        return repr(self.components)

    def __len__(self):
        return len(self.components)

    def __iter__(self):
        for c in self.components.values():
            yield c

    def __dir__(self) -> List[str]:
        d = super().__dir__()
        return list(set(d) | set(self.components.keys()))


class ComponentLibrary:
    registry: ComponentRegistry
    default_module: BasisModule
    modules: List[BasisModule]
    module_precedence: List[str]

    def __init__(self, default_module: BasisModule):
        self.default_module = default_module
        self.module_precedence = [self.default_module.name]
        self.registry = ComponentRegistry()

    def add_module(self, module: BasisModule):
        self.merge(module.library)

    def ensure_uri_module(self, c: ComponentUri) -> ComponentUri:
        if not c.module_name:
            return c.associate_with_module(self.default_module)
        return c

    def add_component(self, c: ComponentUri):
        c = self.ensure_uri_module(c)
        self.registry.register(c)

    def get_component(
        self, c: Union[ComponentUri, str], **kwargs: Any
    ) -> Optional[ComponentUri]:
        return self.registry.get(c, module_precedence=self.module_precedence, **kwargs)

    def get_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        from basis.core.typing.object_type import ObjectType

        otype = self.registry.get(
            otype_like,
            module_precedence=self.module_precedence,
            component_type=ComponentType.ObjectType,
        )
        if otype is None:
            # print({k: v.uri for k, v in self.registry._uri_to_component.items()})
            # print(self.registry._name_to_modules)
            raise KeyError(otype_like)
        return cast(ObjectType, otype)

    def get_function(self, df_like: DataFunctionLike) -> DataFunction:
        from basis.core.data_function import DataFunction

        fn = self.registry.get(
            df_like,
            module_precedence=self.module_precedence,
            component_type=ComponentType.DataFunction,
        )
        if fn is None:
            raise KeyError(df_like)
        return cast(DataFunction, fn)

    def get_external_resource(
        self, ext_like: Union[ExternalResource, str]
    ) -> ExternalResource:
        from basis.core.external import ExternalResource

        ext = self.registry.get(
            ext_like,
            module_precedence=self.module_precedence,
            component_type=ComponentType.External,
        )
        if ext is None:
            raise KeyError(ext_like)
        return cast(ExternalResource, ext)

    def all_otypes(self) -> List[ObjectType]:
        from basis.core.typing.object_type import ObjectType

        return [
            cast(ObjectType, c)
            for c in self.registry.all()
            if c.component_type == ComponentType.ObjectType
        ]

    def all_functions(self) -> List[DataFunction]:
        from basis.core.data_function import DataFunction

        return [
            cast(DataFunction, c)
            for c in self.registry.all()
            if c.component_type == ComponentType.DataFunction
        ]

    def all_external_resources(self) -> List[ExternalResource]:
        from basis.core.external import ExternalResource

        return [
            cast(ExternalResource, c)
            for c in self.registry.all()
            if c.component_type == ComponentType.External
        ]

    def all(self) -> Iterable[ComponentUri]:
        return self.registry.all()

    def component_view(self, ctype: ComponentType) -> ComponentView:
        comps = []
        for c in self.registry.all():
            if c.component_type == ctype:
                comps.append(c)
        return ComponentView(comps)

    def merge(self, other: ComponentLibrary):
        for c in other.registry.all():
            self.add_component(c)
        for m in other.module_precedence:
            if m not in self.module_precedence:
                self.module_precedence.append(m)
