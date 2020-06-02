from __future__ import annotations

import enum
import inspect
import re
from dataclasses import asdict, dataclass
from functools import partial
from pprint import pprint
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

import networkx as nx
from pandas import DataFrame
from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, String, func
from sqlalchemy.orm import RelationshipProperty, relationship

from basis.core.data_block import DataBlock, DataBlockMetadata, DataSetMetadata
from basis.core.data_format import DatabaseTable, DictList
from basis.core.environment import Environment
from basis.core.metadata.orm import BaseModel
from basis.core.typing.object_type import (
    ObjectType,
    ObjectTypeLike,
    ObjectTypeUri,
    is_generic,
)
from basis.utils.common import printd

if TYPE_CHECKING:
    from basis.core.data_function import (
        FunctionNode,
        InputExhaustedException,
        DataFunctionCallable,
    )
    from basis.core.storage import Storage
    from basis.core.runnable import ExecutionContext
    from basis.core.streams import (
        InputBlocks,
        DataBlockStream,
        ensure_data_stream,
        FunctionNodeInput,
        InputStreams,
    )


# re_type_hint = re.compile(
#     r"(?P<iterable>(Iterator|Iterable|Sequence|List)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
# )
re_type_hint = re.compile(
    r"(?P<optional>(Optional)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
)

VALID_DATA_INTERFACE_TYPES = [
    "DataBlock",
    "DataSet",
    "DataFrame",
    "DictList",
    "DictListIterator",
    "DatabaseTable",
    # TODO: is this list just a list of formats? which ones are valid i/o to DFs?
    # TODO: also, are DataBlocks the only valid *input* type?
    # "DatabaseCursor",
]

SELF_REF_PARAM_NAME = "this"


@dataclass
class DataFunctionAnnotation:
    data_format_class: str
    otype_like: ObjectTypeLike
    name: Optional[str] = None
    connected_stream: Optional[DataBlockStream] = None
    # is_iterable: bool = False  # TODO: what is state of iterable support?
    is_variadic: bool = False  # TODO: what is state of variadic support?
    is_generic: bool = False
    is_optional: bool = False
    is_self_ref: bool = False
    original_annotation: Optional[str] = None

    @classmethod
    def create(cls, **kwargs) -> DataFunctionAnnotation:
        name = kwargs.get("name")
        if name:
            kwargs["is_self_ref"] = name == SELF_REF_PARAM_NAME
        otype_key = kwargs.get("otype_like")
        if isinstance(otype_key, str):
            kwargs["is_generic"] = is_generic(otype_key)
        if kwargs["data_format_class"] not in VALID_DATA_INTERFACE_TYPES:
            raise TypeError(
                f"`{kwargs['data_format_class']}` is not a valid data input type"
            )
        return DataFunctionAnnotation(**kwargs)

    @classmethod
    def from_parameter(cls, parameter: inspect.Parameter) -> DataFunctionAnnotation:
        annotation = parameter.annotation
        is_optional = parameter.default != inspect.Parameter.empty
        is_variadic = parameter.kind == inspect.Parameter.VAR_POSITIONAL
        tda = cls.from_type_annotation(
            annotation,
            name=parameter.name,
            is_optional=is_optional,
            is_variadic=is_variadic,
        )
        return tda

    @classmethod
    def from_type_annotation(cls, annotation: str, **kwargs) -> DataFunctionAnnotation:
        """
        Annotation of form `DataBlock[T]` for example
        """
        m = re_type_hint.match(annotation)
        if m is None:
            raise Exception(f"Invalid DataFunction annotation '{annotation}'")
        is_optional = bool(m.groupdict()["optional"])
        data_format_class = m.groupdict()["origin"]
        otype_key = m.groupdict()["arg"]
        args = dict(
            data_format_class=data_format_class,
            otype_like=otype_key,
            is_optional=is_optional,
            original_annotation=annotation,
        )
        args.update(**kwargs)
        return DataFunctionAnnotation.create(**args)  # type: ignore

    def otype_uri(self, env: Environment) -> ObjectTypeUri:
        if self.is_generic:
            raise  # TODO: ?? is this really an error? What is the URI of a generic otype?
        return env.get_otype(self.otype_like).uri


@dataclass
class ResolvedFunctionNodeInput:
    name: str
    data_format_class: str
    is_optional: bool
    is_self_ref: bool
    connected_stream: DataBlockStream
    parent_nodes: List[FunctionNode]
    potential_parent_nodes: List[FunctionNode]
    otype: ObjectType
    bound_data_block: Optional[DataBlockMetadata] = None


@dataclass
class ResolvedFunctionInterface:
    inputs: List[ResolvedFunctionNodeInput]
    output_otype: Optional[ObjectType]
    requires_data_function_context: bool = True
    is_bound: bool = False

    def get_input(self, name: str) -> ResolvedFunctionNodeInput:
        for input in self.inputs:
            if input.name == name:
                return input
        raise KeyError(name)

    def bind(self, input_blocks: InputBlocks):
        if self.is_bound:
            raise Exception("Already bound")
        for name, input_block in input_blocks.items():
            self.get_input(name).bound_data_block = input_block
        self.is_bound = True

    def as_kwargs(self):
        if not self.is_bound:
            raise Exception("Interface not bound")
        return {i.name: i.bound_data_block for i in self.inputs}


@dataclass
class DataFunctionInterface:
    inputs: List[DataFunctionAnnotation]
    output: Optional[DataFunctionAnnotation]
    requires_data_function_context: bool = True
    is_connected: bool = False

    @classmethod
    def from_datafunction_definition(
        cls, df: DataFunctionCallable
    ) -> DataFunctionInterface:
        requires_context = False
        signature = inspect.signature(df)
        output = None
        ret = signature.return_annotation
        if ret is not inspect.Signature.empty:
            if not isinstance(ret, str):
                raise Exception("Return type annotation not a string")
            output = DataFunctionAnnotation.from_type_annotation(ret)
        inputs = []
        for name, param in signature.parameters.items():
            a = param.annotation
            if a is not inspect.Signature.empty:
                if not isinstance(a, str):
                    raise Exception("Parameter type annotation not a string")
            try:
                a = DataFunctionAnnotation.from_parameter(param)
                inputs.append(a)
            except TypeError:
                # Not a DataBlock/Set
                if param.annotation == "DataFunctionContext":
                    requires_context = True
                else:
                    raise Exception(f"Invalid data function parameter {param}")
        dfi = DataFunctionInterface(
            inputs=inputs,
            output=output,
            requires_data_function_context=requires_context,
        )
        dfi.validate_inputs()  # TODO: let caller handle this?
        return dfi

    def get_input(self, name: str) -> DataFunctionAnnotation:
        for input in self.inputs:
            if input.name == name:
                return input
        raise KeyError(name)

    def get_non_recursive_inputs(self):
        return [i for i in self.inputs if not i.is_self_ref]

    def get_inputs_dict(self) -> Dict[str, DataFunctionAnnotation]:
        return {i.name: i for i in self.inputs if i.name}

    def validate_inputs(self):
        # TODO: review this validation. what do we want to check for?
        data_block_seen = False
        for annotation in self.inputs:
            if (
                annotation.data_format_class == "DataBlock"
                and not annotation.is_optional
            ):
                if data_block_seen:
                    raise Exception(
                        "Only one uncorrelated DataBlock input allowed to a DataFunction."
                        "Correlate the inputs or use a DataSet"
                    )
                data_block_seen = True

    def connect_upstream(self, upstream: InputStreams):
        from basis.core.streams import DataBlockStream

        if self.is_connected:
            raise Exception("Already connected")
        if isinstance(upstream, DataBlockStream):
            assert (
                len(self.get_non_recursive_inputs()) == 1
            ), f"Wrong number of inputs. (Variadic inputs not supported yet) {upstream}"
            self.get_non_recursive_inputs()[0].connected_stream = upstream
        if isinstance(upstream, dict):
            for name, node in upstream.items():
                self.get_input(name).connected_stream = node
        self.is_connected = True


class FunctionGraphCycleError(Exception):
    pass


class FunctionGraphResolver:
    def __init__(self, env: Environment, nodes: Optional[List[FunctionNode]] = None):
        self.env = env
        self._nodes = nodes or env.flattened_nodes()
        self._resolved_output_types: Dict[FunctionNode, ObjectType] = {}
        self._resolved_inputs: Dict[FunctionNode, List[ResolvedFunctionNodeInput]] = {}
        self._resolved = False

    def resolve(self):
        if self._resolved:
            return
        self.resolve_output_types()
        self.resolve_dependencies()
        self._resolved = True

    def resolve_dependencies(self):
        for node in self._nodes:
            self._resolve_node_dependencies(node)
        g = self.as_networkx_graph()
        cycles = list(nx.simple_cycles(g))
        if cycles:
            raise FunctionGraphCycleError(cycles)
        self._resolve_potential_parents()

    def _resolve_potential_parents(self):
        # TODO: when does the order in which we try potential parents matter? Is it ok if that is non-deterministic?
        for node, inputs in self._resolved_inputs.items():
            for input in inputs:
                if input.potential_parent_nodes:
                    for p in input.potential_parent_nodes:
                        # Add potential edge
                        input.parent_nodes.append(p)
                        # Check for new cycle in graph
                        g = self.as_networkx_graph()
                        cycles = list(nx.simple_cycles(g))
                        if cycles:
                            # printd(f"{node.key} - {p.key} - {cycles}")
                            # Keep it in parents only if no cycle
                            input.parent_nodes.remove(p)
                input.potential_parent_nodes = []

    def as_networkx_graph(
        self,
        resolved_inputs: Dict[FunctionNode, List[ResolvedFunctionNodeInput]] = None,
    ):
        if not resolved_inputs:
            resolved_inputs = self._resolved_inputs
        g = nx.DiGraph()
        for node in self._nodes:
            g.add_node(node)
            for input in resolved_inputs.get(node, []):
                for p in input.parent_nodes:
                    g.add_node(p)
                    g.add_edge(p, node)
        return g

    def _resolve_node_dependencies(
        self, node: FunctionNode, visited: Set[FunctionNode] = None
    ) -> List[ResolvedFunctionNodeInput]:
        if node in self._resolved_inputs:
            return self._resolved_inputs[node]
        if visited is None:
            visited = set([])
        if node in visited:
            raise FunctionGraphCycleError
        visited.add(node)
        dfi = node.get_interface()
        resolved_inputs = []
        for input in dfi.get_non_recursive_inputs():
            if not input.connected_stream:
                raise Exception(f"no input connected {input}")
            parents, potential_parents = self.resolve_stream_dependencies(
                node, input.connected_stream, visited
            )
            if input.is_generic:
                if not parents and not potential_parents:
                    raise Exception(f"No parents {node} {input}")
                sample_node = parents[0] if parents else potential_parents[0]
                resolved_otype = self._resolved_output_types[
                    sample_node
                ]  # TODO: check that all parents are the same otype
            else:
                resolved_otype = self.env.get_otype(input.otype_like)
            i = ResolvedFunctionNodeInput(
                name=input.name,
                data_format_class=input.data_format_class,
                is_optional=input.is_optional,
                is_self_ref=input.is_self_ref,
                otype=self.env.get_otype(resolved_otype),
                connected_stream=input.connected_stream,
                parent_nodes=parents,
                potential_parent_nodes=potential_parents,
            )
            resolved_inputs.append(i)
        self._resolved_inputs[node] = resolved_inputs
        return resolved_inputs

    def resolve_stream_dependencies(
        self, node: FunctionNode, stream: DataBlockStream, visited: Set[FunctionNode]
    ) -> Tuple[List[FunctionNode], List[FunctionNode]]:
        upstream_nodes = stream.get_upstream(self.env)
        if upstream_nodes:
            return upstream_nodes, []
        otypes = stream.get_otypes(self.env)
        if otypes:
            if len(otypes) > 1:
                raise NotImplementedError("Mixed otype streams not supported atm")
            otype = otypes[0]
            potential_parents = []
            for other_node, output_type in self._resolved_output_types.items():
                if other_node == node:
                    continue
                if output_type == otype:
                    potential_parents.append(other_node)
            return [], potential_parents
        raise NotImplementedError

    def resolve_output_types(self):
        for node in self._nodes:
            self.resolve_output_type(node)

    def resolve_output_type(
        self, node: FunctionNode, visited: Set[FunctionNode] = None
    ) -> Optional[ObjectTypeLike]:
        if node in self._resolved_output_types:
            return self._resolved_output_types[node]
        if visited is None:
            visited = set([])
        if node in visited:
            raise FunctionGraphCycleError
        visited.add(node)
        dfi = node.get_interface()
        if not dfi.output:
            return None
        if not dfi.output.is_generic:
            output_otype = self.env.get_otype(dfi.output.otype_like)
        else:
            resolved_generics: Dict[str, ObjectTypeLike] = {}
            for input in dfi.get_non_recursive_inputs():
                if not input.is_generic:
                    continue
                generic_otype = cast(str, input.otype_like)
                if not input.connected_stream:
                    raise Exception(f"no input connected {input}")
                otype = self.resolve_stream_output_type(
                    node, input.connected_stream, visited
                )
                resolved_generics[generic_otype] = otype
            generic_output_otype = cast(str, dfi.output.otype_like)
            output_otype = self.env.get_otype(resolved_generics[generic_output_otype])
        self._resolved_output_types[node] = output_otype
        return output_otype

    def resolve_stream_output_type(
        self, node: FunctionNode, stream: DataBlockStream, visited: Set[FunctionNode]
    ):
        if stream.upstream:
            nodes = stream.get_upstream(self.env)
            otypes = []
            for n in nodes:
                otypes.append(self.resolve_output_type(n, visited))
            if len(set(otypes)) != 1:
                raise Exception("Mixed otype streams not suppported atm")
            return otypes[0]
        elif stream.otypes:
            if len(stream.otypes) > 1:
                raise NotImplementedError("Mixed otype streams not supported atm")
            return stream.otypes[0]
        raise NotImplementedError

    def get_resolved_interface(self, node: FunctionNode) -> ResolvedFunctionInterface:
        self.resolve()
        resolved_otype = self._resolved_output_types.get(node)
        if resolved_otype is not None:
            resolved_otype = self.env.get_otype(resolved_otype)
        return ResolvedFunctionInterface(
            inputs=self._resolved_inputs.get(node, []),
            output_otype=resolved_otype,
            requires_data_function_context=node.get_interface().requires_data_function_context,
        )

    def get_all_upstream_dependencies_in_execution_order(
        self, node: FunctionNode
    ) -> List[FunctionNode]:
        self.resolve()
        # self.print_resolution()
        all_ = []
        for dep in self._resolved_inputs[node]:
            for parent in dep.parent_nodes:
                deps = self.get_all_upstream_dependencies_in_execution_order(parent)
                all_.extend(deps)
        all_.append(node)
        return all_

    # def print_resolution(self):
    #     pprint({k.key: v.key for k, v in self._resolved_output_types.items()})
    #     for node in self._nodes:
    #         print(node.key)
    #         print("  output:", self._resolved_output_types.get(node).key)
    #         print("  inputs")
    #         for i in self._resolved_inputs.get(node, []):
    #             print("    ", i.name, [n.key for n in i.parent_nodes])


class FunctionNodeInterfaceManager:
    """
    Responsible for finding and preparing DataBlocks for input to a
    FunctionNode.
    """

    def __init__(
        self, ctx: ExecutionContext, node: FunctionNode,
    ):
        self.env = ctx.env
        self.ctx = ctx
        self.node = node
        self.graph_resolver = self.env.get_function_graph_resolver()
        self.dfi = self.node.get_interface()

    def get_resolved_interface(self) -> ResolvedFunctionInterface:
        return self.graph_resolver.get_resolved_interface(self.node)

    def get_bound_interface(
        self, input_data_blocks: Optional[InputBlocks] = None
    ) -> ResolvedFunctionInterface:
        i = self.get_resolved_interface()
        if input_data_blocks is None:
            input_data_blocks = self.get_input_data_blocks()
        i.bind(input_data_blocks)
        return i

    def is_input_required(self, annotation: DataFunctionAnnotation) -> bool:
        if annotation.is_optional:
            return False
        # TODO: more complex logic? hmmmm
        return True

    def get_input_data_blocks(self) -> InputBlocks:
        from basis.core.streams import ensure_data_stream
        from basis.core.data_function import InputExhaustedException

        input_data_blocks: InputBlocks = {}
        any_unprocessed = False
        for input in self.get_resolved_interface().inputs:
            stream = input.connected_stream
            printd(f"Getting {input} for {stream}")
            stream = ensure_data_stream(stream)
            block: Optional[DataBlockMetadata] = self.get_input_data_block(
                stream, input, self.ctx.all_storages
            )
            printd("\tFound:", block)

            """
            Inputs are considered "Exhausted" if:
            - Single DR stream (and zero or more DSs): no unprocessed DRs
            - Multiple correlated DR streams: ANY stream has no unprocessed DRs
            - One or more DSs: if ALL DS streams have no unprocessed

            In other words, if ANY DR stream is empty, bail out. If ALL DS streams are empty, bail
            """
            if block is None:
                printd(
                    f"Couldnt find eligible DataBlocks for input `{input.name}` from {stream}"
                )
                if not input.is_optional:
                    # print(actual_input_node, annotation, storages)
                    raise InputExhaustedException(
                        f"    Required input '{input.name}'={stream} to DataFunction '{self.node.key}' is empty"
                    )
            else:
                input_data_blocks[input.name] = block
            if input.data_format_class == "DataBlock":
                any_unprocessed = True
            elif input.data_format_class == "DataSet":
                if block is not None:
                    any_unprocessed = any_unprocessed or stream.is_unprocessed(
                        self.ctx, block, self.node
                    )
            else:
                raise NotImplementedError

        if input_data_blocks and not any_unprocessed:
            raise InputExhaustedException("All inputs exhausted")

        return input_data_blocks

    def get_input_data_block(
        self,
        stream: DataBlockStream,
        input: ResolvedFunctionNodeInput,
        storages: List[Storage] = None,
    ) -> Optional[DataBlockMetadata]:
        # TODO: Is it necessary to filter otype? We're already filtered on the `upstream` stream
        # if not input.is_generic:
        #     stream = stream.filter_otype(input.otype_like)
        if storages:
            stream = stream.filter_storages(storages)
        block: Optional[DataBlockMetadata]
        if input.data_format_class in ("DataBlock",):
            stream = stream.filter_unprocessed(self.node, allow_cycle=input.is_self_ref)
            block = stream.get_next(self.ctx)
        elif input.data_format_class == "DataSet":
            stream = stream.filter_dataset()
            block = stream.get_most_recent(self.ctx)
            # TODO: someday probably pass in actual DataSet (not underlying DR) to function that asks
            #   for it (might want to use `name`, for instance). and then just proxy
            #   through to underlying DR
        else:
            raise NotImplementedError

        return block
