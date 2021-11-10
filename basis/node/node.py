import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from basis.configuration.graph import ScriptType
from basis.graph.configured_node import ConfiguredNode
from basis.utils.modules import find_single_of_type_in_module


@dataclass(frozen=True)
class NodeFunction:
    function: Callable

    def __call__(self, *args, **kwargs):
        self.function(*args, **kwargs)


def get_node_function(node: ConfiguredNode) -> NodeFunction:
    assert node.node_definition is not None
    assert node.node_definition.script is not None
    assert node.node_definition.script.script_type == ScriptType.PYTHON
    assert (
        node.file_path_to_node_script_relative_to_root is not None
    ), "No script file specified"
    mod_path = '.'.join(Path(node.file_path_to_node_script_relative_to_root).with_suffix('').parts)
    # TODO: root must be on path...
    mod = importlib.import_module(mod_path)
    node_fn = find_single_of_type_in_module(mod, NodeFunction)
    return node_fn


node = NodeFunction  # decorator
