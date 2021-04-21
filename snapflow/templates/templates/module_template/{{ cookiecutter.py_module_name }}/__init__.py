from __future__ import annotations

from snapflow.core.module import SnapflowModule

name = "{{ cookiecutter.module_name }}"
namespace = name

module = SnapflowModule(
    name=name, namespace=namespace, py_module_path=__file__, py_module_name=__name__
)
