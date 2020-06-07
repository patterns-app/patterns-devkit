from __future__ import annotations

from dataclasses import dataclass

import pytest

from basis.core.component import (
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


# def test_registry_merge():
#     r = Registry()
#     a1 = A("k1")
#     a2 = A("k2")
#     r.register(a1)
#     r.register(a2)
#     r2 = Registry()
#     a3 = A("k3")
#     r2.register(a3)
#     r.merge(r2)
#     assert r.get(a3.name) is a3
#     assert len(r) == 3
#     assert len(list(r.all())) == 3
#
#
# @dataclass
# class B:
#     uri: str
#
#
# def test_uri_registry_operations():
#     r = UriRegistry()
#     b1 = B("k1")  # Invalid URI
#     with pytest.raises(RegistryError):
#         r.register(b1)
#     b2 = B("m1.k2")
#     b3 = B("m2.k2")
#     r.register(b2)
#     r.register(b3)
#     assert r.get(b2.uri) is b2
#     assert r.get(b3.uri) is b3
#
#     # Unambiguous name
#     b4 = B("m1.k4")
#     r.register(b4)
#     assert r.get("k4") is b4
#
#     # Ambiguous name
#     with pytest.raises(RegistryError):
#         r.get("k2")
#     assert r.get("k2", module_precedence=["m1", "m2"]) is b2
#     assert r.get("k2", module_precedence=["m2"]) is b3
#     assert r.get("k2", module_precedence=["m2", "m1"]) is b3
#
#
# def test_uri_registry_merge():
#     r = UriRegistry()
#     b1 = B("m1.k1")
#     b2 = B("m1.k2")
#     r.register(b1)
#     r.register(b2)
#     r2 = UriRegistry()
#     b3 = B("m2.k2")
#     r2.register(b3)
#     r.merge(r2)
#     assert r.get(b3.uri) is b3
#     assert len(r) == 3
#     # Ambiguous name
#     with pytest.raises(RegistryError):
#         r.get("k2")
#     assert r.get("k2", module_precedence=["m1", "m2"]) is b2
