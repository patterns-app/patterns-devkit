from __future__ import annotations

from basis.core.module import BasisModule

namespace = "{{ cookiecutter.module_name }}"

module = BasisModule(namespace=namespace, py_module_path=__file__,)
