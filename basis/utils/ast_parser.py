import ast
from ast import Str
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Dict, Optional

from basis.graph.configured_node import (
    NodeInterface,
    InputDefinition,
    OutputDefinition,
    ParameterDefinition,
    PortType,
    StateDefinition,
)


def read_interface_from_py_node_file(path: Path) -> NodeInterface:
    tree = ast.parse(path.read_text())
    finder = _NodeFuncFinder()
    finder.visit(tree)
    if finder.found == 0:
        raise ValueError(f"Could not find node function at {path}")
    if finder.found > 1:
        raise ValueError(f"File contains more than one node function: {path}")
    return finder.interface()


@dataclass
class _Call:
    name: str
    kwargs: Dict[Str, Any]


class _NodeFuncFinder(ast.NodeVisitor):
    def __init__(self):
        self.i: List[InputDefinition] = []
        self.o: List[OutputDefinition] = []
        self.p: List[ParameterDefinition] = []
        self.s: Optional[StateDefinition] = None
        self.found = 0

    def interface(self):
        return NodeInterface(
            inputs=self.i, outputs=self.o, parameters=self.p, state=self.s
        )

    def visit_FunctionDef(self, node: ast.FunctionDef):
        name = node.name
        count = sum(1 for it in node.decorator_list if _is_basis_node_decorator(it))
        if count == 0:
            return
        if count > 1:
            raise ValueError(
                f"@node decorator cannot be used more than once on a function"
            )
        self.found += 1

        args = node.args
        if args.kwarg or args.vararg or args.posonlyargs or args.kwonlyargs:
            raise ValueError(
                f"Node function {name} cannot use varargs or keyword-only args"
            )
        if len(args.args) != len(args.defaults):
            raise ValueError(
                f"Node function {name} must specify an input, output, or parameter for all arguments."
            )
        for arg, default in zip(args.args, args.defaults):
            if not default:
                s = f"Node function {name} parameter {arg.arg} must specify an input, output, or parameter."
                raise ValueError(s)
            self._consume_call(arg.arg, _parse_call(default))

    def _consume_call(self, name: str, call: _Call):
        if call.name not in (
            "InputTable",
            "OutputTable",
            "InputStream",
            "OutputStream",
            "Parameter",
            "State",
        ):
            raise ValueError(
                f"node function parameter {name} must specify an input, output, or parameter, not {call.name}"
            )

        port_type = PortType.Table if call.name.endswith("Table") else PortType.Stream

        def get(k, t=None, d=None):
            v = call.kwargs.pop(k, d)
            if v is not None and t is not None and not isinstance(v, t):
                raise TypeError(f"argument must be {t}, not '{type(v)}'")
            return v

        if call.name.startswith("Input"):
            self.i.append(
                InputDefinition(
                    port_type=port_type,
                    name=name,
                    description=get("description", str),
                    schema_or_name=get("schema", str),
                    required=get("required", bool, True),
                )
            )
        elif call.name.startswith("Output"):
            self.o.append(
                OutputDefinition(
                    port_type=port_type,
                    name=name,
                    description=get("description", str),
                    schema_or_name=get("schema", str),
                )
            )
        elif call.name == "Parameter":
            self.p.append(
                ParameterDefinition(
                    name=name,
                    parameter_type=get("type", str),
                    description=get("description", str),
                    default=get("default"),
                )
            )
        elif call.name == "State":
            self.s = StateDefinition(name=name)
        else:
            raise RuntimeError("error in file parsing")  # unreachable
        if call.kwargs:
            raise ValueError(
                f"got an unexpected keyword argument {call.kwargs.popitem()[0]}"
            )


def _parse_call(node) -> _Call:
    if isinstance(node, ast.Name):
        return _Call(node.id, {})
    if not isinstance(node, ast.Call):
        raise ValueError(
            f"Node functions must declare inputs, outputs, or parameters: {node}"
        )
    if node.args:
        raise ValueError(f"Node functions only accept keyword arguments")
    return _Call(
        name=_get_qualified_name(node.func),
        kwargs={a.arg: _parse_port_arg(a.arg, a.value) for a in node.keywords},
    )


def _parse_port_arg(name: str, node):
    if not isinstance(node, ast.Constant):
        raise ValueError(
            f"Node function argument {name}: {ast.unparse(node)} in not a literal value"
        )
    return node.value


def _is_basis_node_decorator(node) -> bool:
    qname = _get_qualified_name(node)
    # We just check for the most common ways to import the decorator. If we need to be more accurate, we could parse the
    # module imports and actually resolve the name.
    return qname in ("node", "basis.node", "basis.node.node.node")


def _get_qualified_name(node) -> str:
    return ".".join(_get_qualified_name_parts(node))


def _get_qualified_name_parts(node) -> List[str]:
    parts = []
    while True:
        if isinstance(node, ast.Name):
            parts.append(node.id)
            break
        elif isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        else:
            raise RuntimeError(f"unexpected node type {node}")
    return list(reversed(parts))
