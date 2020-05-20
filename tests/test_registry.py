from __future__ import annotations

from dataclasses import dataclass

import pytest

from basis.utils.registry import Registry, RegistryError, UriRegistry


@dataclass
class A:
    key: str


def test_registry_operations():
    r = Registry()
    a1 = A("k1")
    a2 = A("k2")
    r.register(a1)
    r.register(a2)
    assert r.get(a1.key) is a1
    assert r.get(a2.key) is a2
    with pytest.raises(RegistryError):
        # Can't register same key twice
        r.register(a1)
    with pytest.raises(KeyError):
        r.get("k3")
    # Register all
    a4 = A("k4")
    a5 = A("k5")
    r.register_all([a4, a5])
    assert r.get(a4.key) is a4
    assert r.get(a5.key) is a5


def test_registry_merge():
    r = Registry()
    a1 = A("k1")
    a2 = A("k2")
    r.register(a1)
    r.register(a2)
    r2 = Registry()
    a3 = A("k3")
    r2.register(a3)
    r.merge(r2)
    assert r.get(a3.key) is a3
    assert len(r) == 3
    assert len(list(r)) == 3
    assert len(list(r.all())) == 3


@dataclass
class B:
    uri: str


def test_uri_registry_operations():
    r = UriRegistry()
    b1 = B("k1")  # Invalid URI
    with pytest.raises(RegistryError):
        r.register(b1)
    b2 = B("m1.k2")
    b3 = B("m2.k2")
    r.register(b2)
    r.register(b3)
    assert r.get(b2.uri) is b2
    assert r.get(b3.uri) is b3

    # Unambiguous key
    b4 = B("m1.k4")
    r.register(b4)
    assert r.get("k4") is b4

    # Ambiguous key
    with pytest.raises(RegistryError):
        r.get("k2")
    assert r.get("k2", module_order=["m1", "m2"]) is b2
    assert r.get("k2", module_order=["m2"]) is b3
    assert r.get("k2", module_order=["m2", "m1"]) is b3


def test_uri_registry_merge():
    r = UriRegistry()
    b1 = B("m1.k1")
    b2 = B("m1.k2")
    r.register(b1)
    r.register(b2)
    r2 = UriRegistry()
    b3 = B("m2.k2")
    r2.register(b3)
    r.merge(r2)
    assert r.get(b3.uri) is b3
    assert len(r) == 3
    # Ambiguous key
    with pytest.raises(RegistryError):
        r.get("k2")
    assert r.get("k2", module_order=["m1", "m2"]) is b2
