from __future__ import annotations

from dataclasses import dataclass

import pytest

from dags.core.component import (
    ComponentRegistry,
    ComponentType,
    ComponentUri,
    RegistryError,
)


@dataclass(frozen=True)
class A(ComponentUri):
    extra_attr: str

    @classmethod
    def make(cls, **kwargs) -> A:
        defaults = dict(
            component_type=ComponentType.ObjectType,
            module_name="m1",
            version=None,
            extra_attr=None,
        )
        defaults.update(kwargs)
        return A(**defaults)


def test_registry_operations():
    r = ComponentRegistry(overwrite_behavior="error")
    a1 = A.make(name="a1", extra_attr="1")
    a2 = A.make(name="a2", extra_attr="2")
    r.register(a1)
    r.register(a2)
    assert r.get(a1.name, component_type=ComponentType.ObjectType) is a1
    assert r.get(a2.name, component_type=ComponentType.ObjectType) is a2
    with pytest.raises(RegistryError):
        # Can't register same name twice
        r.register(a1)
    with pytest.raises(RegistryError):
        r.get("k3")
    a12 = A.make(name="a1", module_name="m2", extra_attr="1")
    a13 = A.make(name="a1", module_name="m3", extra_attr="1")
    r.register(a12)
    r.register(a13)
    with pytest.raises(RegistryError):
        assert r.get(a1.name, component_type=ComponentType.ObjectType) is a1
    with pytest.raises(RegistryError):
        assert r.get(a12.name, component_type=ComponentType.ObjectType) is a1
    assert (
        r.get(
            a12.name,
            module_precedence=["m1", "m2"],
            component_type=ComponentType.ObjectType,
        )
        is a1
    )
    assert (
        r.get(
            a12.name,
            module_precedence=["m2", "m1"],
            component_type=ComponentType.ObjectType,
        )
        is a12
    )
    assert (
        r.get(
            a12.name, module_precedence=["m3"], component_type=ComponentType.ObjectType
        )
        is a13
    )
    with pytest.raises(RegistryError):
        assert r.get(a12.name, module_precedence=["m3"]) is a13
