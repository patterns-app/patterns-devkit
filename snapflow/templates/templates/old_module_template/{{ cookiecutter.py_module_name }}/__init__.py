from __future__ import annotations

from snapflow.core.module import SnapflowModule

namespace = "{{ cookiecutter.module_name }}"

module = SnapflowModule(namespace=namespace, py_module_path=__file__,)
