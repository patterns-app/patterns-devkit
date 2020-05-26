from __future__ import annotations

import enum
import inspect
import re
from dataclasses import asdict, dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Union

from pandas import DataFrame
from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, String, func
from sqlalchemy.orm import RelationshipProperty, relationship

from basis.core.data_format import DatabaseTable, DictList
from basis.core.data_resource import DataResource, DataResourceMetadata, DataSetMetadata
from basis.core.environment import Environment
from basis.core.metadata.orm import BaseModel
from basis.core.object_type import ObjectType, ObjectTypeLike, ObjectTypeUri, is_generic
from basis.utils.common import printd

if TYPE_CHECKING:
    from basis.core.storage import Storage
    from basis.core.runnable import DataFunctionContext, ExecutionContext
    from basis.core.streams import (
        InputResources,
        DataResourceStream,
        DataResourceStreamable,
        ensure_data_stream,
    )


# re_type_hint = re.compile(
#     r"(?P<iterable>(Iterator|Iterable|Sequence|List)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
# )
re_type_hint = re.compile(
    r"(?P<optional>(Optional)\[)?(?P<origin>\w+)(\[(?P<arg>(\w+\.)?\w+)\])?\]?"
)


VALID_DATA_INTERFACE_TYPES = [
    "DataResource",
    "DataSet",
    "DataFrame",
    "DictList",
    "DictListIterator",
    "DatabaseTable",
    # TODO: is this list just a list of formats? which ones are valid i/o to DFs?
    # TODO: also, are DataResources the only valid *input* type?
    # "DatabaseCursor",
]

SELF_REF_PARAM_NAME = "this"


@dataclass(frozen=True)
class TypedDataAnnotation:
    data_resource_class: str
    otype_like: ObjectTypeLike
    name: Optional[str] = None
    is_iterable: bool = False
    is_variadic: bool = False
    is_generic: bool = False
    is_optional: bool = False
    is_self_ref: bool = False
    original_annotation: Optional[str] = None

    @classmethod
    def create(cls, **kwargs) -> TypedDataAnnotation:
        name = kwargs.get("name")
        if name:
            kwargs["is_self_ref"] = name == SELF_REF_PARAM_NAME
        otype_key = kwargs.get("otype_like")
        if isinstance(otype_key, str):
            kwargs["is_generic"] = is_generic(otype_key)
        if kwargs["data_resource_class"] not in VALID_DATA_INTERFACE_TYPES:
            raise TypeError(
                f"`{kwargs['data_resource_class']}` is not a valid data input type"
            )
        return TypedDataAnnotation(**kwargs)

    @classmethod
    def from_parameter(cls, parameter: inspect.Parameter) -> TypedDataAnnotation:
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
    def from_type_annotation(cls, annotation: str, **kwargs) -> TypedDataAnnotation:
        """
        Annotation of form `DataResource[T]` for example
        """
        m = re_type_hint.match(annotation)
        if m is None:
            raise Exception(f"Invalid DataFunction annotation '{annotation}'")
        is_optional = bool(m.groupdict()["optional"])
        data_resource_class = m.groupdict()["origin"]
        otype_key = m.groupdict()["arg"]
        args = dict(
            data_resource_class=data_resource_class,
            otype_like=otype_key,
            is_optional=is_optional,
            original_annotation=annotation,
        )
        args.update(**kwargs)
        return TypedDataAnnotation.create(**args)  # type: ignore

    def otype_uri(self, env: Environment) -> ObjectTypeUri:
        return env.get_otype(self.otype_like).uri


@dataclass(frozen=True)
class ConcreteTypedDataAnnotation:
    data_resource_class: str
    otype: ObjectType
    name: Optional[str] = None
    is_iterable: bool = False
    is_optional: bool = False
    is_variadic: bool = False
    is_self_ref: bool = False
    original_annotation: Optional[str] = None

    @classmethod
    def from_typed_data_annotation(
        cls, annotation: TypedDataAnnotation, **kwargs: Any
    ) -> ConcreteTypedDataAnnotation:
        args = asdict(annotation)
        if isinstance(annotation.otype_like, ObjectType):
            args["otype"] = annotation.otype_like
        args.update(kwargs)
        del args["is_generic"]
        del args["otype_like"]
        return cls(**args)

    @property
    def otype_uri(self) -> ObjectTypeUri:
        return self.otype.uri


@dataclass(frozen=True)
class BoundTypedDataAnnotation:
    data_resource: DataResourceMetadata
    otype: ObjectType
    name: Optional[str] = None
    original_annotation: Optional[str] = None
    # TODO: support for duck typing
    # bound_otype: ObjectType
    # expected_otype: ObjectType

    @property
    def otype_uri(self) -> ObjectTypeUri:
        return self.otype.uri

    # TODO: variadic support

    # @classmethod
    # def from_typed_data_annotation(
    #     cls, annotation: TypedDataAnnotation, dr: DataResource, **kwargs: Any
    # ) -> BoundTypedDataAnnotation:
    #     return BoundTypedDataAnnotation(
    #         data_resource=dr,
    #         otype=dr.otype,
    #         name=annotation.name,
    #         original_annotation=annotation.original_annotation,
    #     )


@dataclass(frozen=True)
class DataFunctionInterface:
    inputs: List[TypedDataAnnotation]
    output: Optional[TypedDataAnnotation]
    requires_data_function_context: bool = True  # TODO: implement

    def resolve_generics(
        self, input_data_resources: InputResources
    ) -> Dict[str, ObjectTypeUri]:
        resolved_generics: Dict[str, ObjectTypeUri] = {}
        for input in self.inputs:
            if input.is_generic:
                generic = input.otype_like
                assert isinstance(generic, str)
                if input.name in input_data_resources:
                    dr = input_data_resources[input.name]
                    otype = dr.otype_uri
                    if (
                        generic in resolved_generics
                        and resolved_generics[generic] != otype
                    ):
                        raise Exception(
                            f"Conflicting generics: {resolved_generics} {generic} {otype}"
                        )
                    resolved_generics[generic] = otype
        return resolved_generics

    def validate_inputs(self):
        data_resource_seen = False
        for annotation in self.inputs:
            if (
                annotation.data_resource_class == "DataResource"
                and not annotation.is_optional
            ):
                if data_resource_seen:
                    raise Exception(
                        "Only one uncorrelated DataResource input allowed to a DataFunction."
                        "Correlate the inputs or use a DataSet"
                    )
                data_resource_seen = True

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
            output = TypedDataAnnotation.from_type_annotation(ret)
        inputs = []
        for name, param in signature.parameters.items():
            a = param.annotation
            if a is not inspect.Signature.empty:
                if not isinstance(a, str):
                    raise Exception("Parameter type annotation not a string")
            try:
                a = TypedDataAnnotation.from_parameter(param)
                inputs.append(a)
            except TypeError:
                # Not a DataResource/Set
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


@dataclass(frozen=True)
class BoundDataFunctionInterface:
    inputs: List[BoundTypedDataAnnotation]
    output: Optional[ConcreteTypedDataAnnotation]
    requires_data_function_context: bool = True

    def as_kwargs(self):
        return {i.name: i.data_resource for i in self.inputs}

    @classmethod
    def from_data_function_interface(
        self,
        env: Environment,
        input_data_resources: InputResources,
        dfi: DataFunctionInterface,
    ) -> BoundDataFunctionInterface:
        resolved_generics = dfi.resolve_generics(input_data_resources)

        inputs: List[BoundTypedDataAnnotation] = []
        output = None
        for input in dfi.inputs:
            if input.name is None:
                raise Exception("Invalid input")
            dtl = input.otype_like
            if input.is_generic:
                assert isinstance(dtl, str)
                dtl = resolved_generics[dtl]
            otype = env.get_otype(dtl)
            input_dr = input_data_resources.get(input.name)
            if input_dr is None:
                if input.is_optional:
                    continue
                else:
                    raise Exception("Required input is missing")
            btda = BoundTypedDataAnnotation(
                data_resource=input_dr,
                otype=otype,
                name=input.name,
                original_annotation=input.original_annotation,
            )
            inputs.append(btda)
        if dfi.output:
            dtl = dfi.output.otype_like
            if dfi.output.is_generic:
                assert isinstance(dfi.output.otype_like, str)
                dtl = resolved_generics[dfi.output.otype_like]
            otype = env.get_otype(dtl)
            output = ConcreteTypedDataAnnotation.from_typed_data_annotation(
                dfi.output, otype=otype,
            )
        bdfi = BoundDataFunctionInterface(
            inputs=inputs,
            output=output,
            requires_data_function_context=dfi.requires_data_function_context,
        )
        return bdfi


@dataclass(frozen=True)
class ConcreteDataFunctionInterface:
    inputs: List[ConcreteTypedDataAnnotation]
    output: Optional[ConcreteTypedDataAnnotation]
    requires_data_function_context: bool = True

    @classmethod
    def from_data_function_interface(
        self,
        env: Environment,
        input_data_resources: InputResources,
        dfi: DataFunctionInterface,
    ) -> ConcreteDataFunctionInterface:
        # TODO: DRY!!!!
        resolved_generics = dfi.resolve_generics(input_data_resources)

        inputs: List[ConcreteTypedDataAnnotation] = []
        output = None
        for input in dfi.inputs:
            dtl = input.otype_like
            if input.is_generic:
                assert isinstance(dtl, str)
                dtl = resolved_generics[dtl]
            otype = env.get_otype(dtl)
            a = ConcreteTypedDataAnnotation.from_typed_data_annotation(
                input, otype=otype,
            )
            inputs.append(a)
        if dfi.output:
            dtl = dfi.output.otype_like
            if dfi.output.is_generic:
                assert isinstance(dfi.output.otype_like, str)
                dtl = resolved_generics[dfi.output.otype_like]
            otype = env.get_otype(dtl)
            output = ConcreteTypedDataAnnotation.from_typed_data_annotation(
                dfi.output, otype=otype,
            )
        cdfi = ConcreteDataFunctionInterface(
            inputs=inputs,
            output=output,
            requires_data_function_context=dfi.requires_data_function_context,
        )
        return cdfi


class DataFunctionInterfaceManager:
    """
    Responsible for finding and preparing input streams for a
    ConfiguredDataFunction, including resolving associated generic types.
    """

    def __init__(
        self, ctx: ExecutionContext, cdf: ConfiguredDataFunction,
    ):
        self.env = ctx.env
        self.ctx = ctx
        self.cdf = cdf
        self.unresolved_dfi = self.cdf.get_interface()

    def get_bound_interface(
        self, input_data_resources: InputResources = None
    ) -> BoundDataFunctionInterface:
        if input_data_resources is None:
            input_data_resources = self.get_input_data_resources()
        return BoundDataFunctionInterface.from_data_function_interface(
            self.ctx.env, input_data_resources, self.unresolved_dfi
        )

    def is_input_required(self, annotation: TypedDataAnnotation) -> bool:
        if annotation.is_optional:
            return False
        # TODO: more complex logic? hmmmm
        return True

    def get_input_data_resources(self) -> InputResources:
        from basis.core.streams import ensure_data_stream

        input_data_resources: InputResources = {}
        any_unprocessed = False
        for annotation in self.unresolved_dfi.inputs:
            assert annotation.name is not None
            stream = self.cdf.get_data_stream_input(annotation.name)
            printd(f"Getting {annotation} for {stream}")
            stream = ensure_data_stream(stream)
            dr: Optional[DataResourceMetadata] = self.get_input_data_resource(
                stream, annotation, self.ctx.all_storages
            )
            printd("\tFound:", dr)

            """
            Inputs are considered "Exhausted" if:
            - Single DR stream (and zero or more DSs): no unprocessed DRs
            - Multiple correlated DR streams: ANY stream has no unprocessed DRs
            - One or more DSs: if ALL DS streams have no unprocessed

            In other words, if ANY DR stream is empty, bail out. If ALL DS streams are empty, bail
            """
            if dr is None:
                printd(
                    f"Couldnt find eligible DataResources for input `{annotation.name}` from {stream}"
                )
                if not annotation.is_optional:
                    # print(actual_input_cdf, annotation, storages)
                    raise InputExhaustedException(
                        f"    Required input '{annotation.name}'={stream} to DataFunction '{self.cdf.key}' is empty"
                    )
            else:
                input_data_resources[annotation.name] = dr
            if annotation.data_resource_class == "DataResource":
                any_unprocessed = True
            elif annotation.data_resource_class == "DataSet":
                if dr is not None:
                    any_unprocessed = any_unprocessed or stream.is_unprocessed(
                        self.ctx, dr, self.cdf
                    )
            else:
                raise NotImplementedError

        if input_data_resources and not any_unprocessed:
            raise InputExhaustedException("All inputs exhausted")

        return input_data_resources

    def get_input_data_resource(
        self,
        stream: DataResourceStream,
        annotation: TypedDataAnnotation,
        storages: List[Storage] = None,
    ) -> Optional[DataResourceMetadata]:
        if not annotation.is_generic:
            stream = stream.filter_otype(annotation.otype_like)
        if storages:
            stream = stream.filter_storages(storages)
        dr: Optional[DataResourceMetadata]
        if annotation.data_resource_class in ("DataResource",):
            stream = stream.filter_unprocessed(
                self.cdf, allow_cycle=annotation.is_self_ref
            )
            dr = stream.get_next(self.ctx)
        elif annotation.data_resource_class == "DataSet":
            stream = stream.filter_dataset()
            dr = stream.get_most_recent(self.ctx)
            # TODO: someday probably pass in actual DataSet (not underlying DR) to function that asks
            #   for it (might want to use `name`, for instance). and then just proxy
            #   through to underlying DR
        else:
            raise NotImplementedError

        return dr


class DataFunctionException(Exception):
    pass


class InputExhaustedException(DataFunctionException):
    pass


DataFunctionCallable = Callable[..., Any]

DataInterfaceType = Union[
    DataFrame, DictList, DatabaseTable, DataResourceMetadata, DataSetMetadata
]  # TODO: also input...?


class DataFunction:
    def __init__(self, key: str = None):
        self._key = key

    def __call__(
        self, *args: DataFunctionContext, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        raise NotImplementedError

    @property
    def key(self):
        return self._key

    def get_interface(self) -> DataFunctionInterface:
        raise NotImplementedError


def make_datafunction_key(data_function: DataFunctionCallable) -> str:
    # TODO: something more principled / explicit?
    if hasattr(data_function, "key"):
        return data_function.key
    if hasattr(data_function, "__name__"):
        return data_function.__name__
    if hasattr(data_function, "__class__"):
        return data_function.__class__.__name__
    raise Exception(f"Invalid DataFunction Key {data_function}")


class PythonDataFunction(DataFunction):
    def __init__(self, data_function: DataFunctionCallable, key: str = None):
        self.data_function = data_function
        if key is None:
            key = make_datafunction_key(data_function)
        super().__init__(key=key)

    def __getattr__(self, item):
        return getattr(self.data_function, item)

    def __call__(
        self, *args: DataFunctionContext, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        return self.data_function(*args, **kwargs)

    def get_interface(self) -> DataFunctionInterface:
        return DataFunctionInterface.from_datafunction_definition(self.data_function)


def datafunction(df=None, *, key=None):
    if df is None:
        return partial(datafunction, key=key)
    return PythonDataFunction(df, key)


class DataFunctionGraph:
    def __init__(self, key):
        self._key = key

    @property
    def key(self):
        return self._key


class DataFunctionChain(DataFunctionGraph):
    def __init__(self, key: str, function_chain: List[DataFunctionCallable]):
        self.function_chain = function_chain
        # if key is None:
        #     key = self.make_key()
        super().__init__(key)

    # def make_key(self) -> str:
    #     return "_".join(f.key for f in self.function_chain)


DataFunctionLike = Union[DataFunctionCallable, DataFunction, DataFunctionGraph]


def ensure_datafunction(dfl: DataFunctionLike) -> DataFunction:
    if isinstance(dfl, DataFunctionGraph):
        return dfl
    if isinstance(dfl, DataFunction):
        return dfl
    return PythonDataFunction(dfl)


class ConfiguredDataFunction:
    env: Environment
    key: str
    datafunction: DataFunction

    def __init__(
        self,
        _env: Environment,
        _key: str,
        _datafunction: DataFunctionLike,
        **kwargs: DataResourceStreamable,
    ):
        self.env = _env
        self.key = _key
        self.datafunction = ensure_datafunction(_datafunction)
        self._kwargs = self.validate_datafunction_inputs(**kwargs)
        self._children: Set[ConfiguredDataFunction] = set([])

    def __repr__(self):
        name = self.datafunction.key
        return f"<{self.__class__.__name__}(key={self.key}, datafunction={name})>"

    def __hash__(self):
        return hash(self.key)

    # def register_child_cdf(self, child: ConfiguredDataFunction):
    #     self._children.add(child)
    #
    # def get_children_cdfs(self) -> Set[ConfiguredDataFunction]:
    #     return self._children

    # @property
    # def uri(self):
    #     return self.key  # TODO

    def validate_datafunction_inputs(self, **kwargs: Any) -> Dict:
        """
        validate resource v set, and validate types
        """
        from basis.core.streams import DataResourceStream

        new_kwargs = {}
        for k, v in kwargs.items():
            if not isinstance(v, ConfiguredDataFunction) and not isinstance(
                v, DataResourceStream
            ):
                if isinstance(v, str):
                    v = self.env.get_node(v)
                else:
                    raise Exception(f"Invalid DataFunction input: {v}")
            new_kwargs[k] = v
        return new_kwargs

    def get_interface(self) -> DataFunctionInterface:
        if hasattr(self.datafunction, "get_interface"):
            return self.datafunction.get_interface()
        if callable(self.datafunction):
            return DataFunctionInterface.from_datafunction_definition(self.datafunction)
        raise Exception("No interface found for datafunction")

    def get_input(self, name: str) -> Any:
        if name == SELF_REF_PARAM_NAME:
            return self
        try:
            return self._kwargs[name]
        except KeyError:
            raise Exception(f"Missing input {name}")  # TODO: exception cleanup

    def get_inputs(self) -> Dict:
        return self._kwargs

    def get_data_stream_input(self, name: str) -> DataResourceStreamable:
        if name == SELF_REF_PARAM_NAME:
            return self
        v = self.get_input(name)
        if not self.is_data_stream_input(v):
            raise TypeError("Not a DRS")  # TODO
        return v

    def is_data_stream_input(self, v: Any) -> bool:
        from basis.core.streams import DataResourceStream

        # TODO: ignores "this" special arg (a bug/feature that build_fn_graph DEPENDS on currently [don't want recursion there])
        if isinstance(v, ConfiguredDataFunction):
            return True
        elif isinstance(v, DataResourceStream):
            return True
        # elif isinstance(v, Sequence):
        #     raise TypeError("Sequences are deprecated")
        #     if v and isinstance(v[0], ConfiguredDataFunction):
        #         return True
        # if v and isinstance(v[0], ConfiguredDataFunction):
        #     return True
        return False

    def get_data_stream_inputs(
        self, env: Environment, as_streams=False
    ) -> List[DataResourceStreamable]:

        streams: List[DataResourceStreamable] = []
        for k, v in self.get_inputs().items():
            if self.is_data_stream_input(v):
                # TODO: sequence deprecated
                # if isinstance(v, Sequence):
                #     streams.extend(v)
                # else:
                streams.append(v)
        if as_streams:
            streams = [ensure_data_stream(v) for v in streams]
        return streams

    def get_output_cdf(self) -> ConfiguredDataFunction:
        # Handle nested CDFs
        # TODO: why would it be a function of the (un-configured) self.datafunction? I don't think it is
        # if hasattr(self.datafunction, "get_output_cdf"):
        #     return self.datafunction.get_output_cdf()
        return self  # Base case is self, not a nested CDF

    def as_stream(self) -> DataResourceStream:
        from basis.core.streams import DataResourceStream

        cdf = self.get_output_cdf()
        return DataResourceStream(upstream=cdf)

    def is_graph(self) -> bool:
        return isinstance(self.datafunction, DataFunctionGraph)

    def get_latest_output(self, ctx: ExecutionContext) -> Optional[DataResource]:
        dr = (
            ctx.metadata_session.query(DataResourceMetadata)
            .join(DataResourceLog)
            .join(DataFunctionLog)
            .filter(
                DataResourceLog.direction == Direction.OUTPUT,
                DataFunctionLog.configured_data_function_key == self.key,
            )
            .order_by(DataResourceLog.created_at.desc())
            .first()
        )
        if dr is None:
            return None
        return dr.as_managed_data_resource(ctx)


class DataFunctionLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    configured_data_function_key = Column(String, nullable=False)
    runtime_url = Column(String, nullable=False)
    queued_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    data_resource_logs: RelationshipProperty = relationship(
        "DataResourceLog", backref="data_function_log"
    )

    def __repr__(self):
        return self._repr(
            id=self.id,
            configured_data_function_key=self.configured_data_function_key,
            runtime_url=self.runtime_url,
            started_at=self.started_at,
        )


class Direction(enum.Enum):
    INPUT = "input"
    OUTPUT = "output"

    @property
    def symbol(self):
        if self.value == "input":
            return "←"
        return "➞"

    @property
    def display(self):
        s = "out"
        if self.value == "input":
            s = "in"
        return self.symbol + " " + s


class DataResourceLog(BaseModel):
    id = Column(Integer, primary_key=True, autoincrement=True)
    data_function_log_id = Column(
        Integer, ForeignKey(DataFunctionLog.id), nullable=False
    )
    data_resource_id = Column(
        String, ForeignKey("basis_data_resource_metadata.id"), nullable=False
    )  # TODO table name ref ugly here. We can parameterize with orm constant at least, or tablename("DataResource.id")
    direction = Column(Enum(Direction), nullable=False)
    processed_at = Column(DateTime, default=func.now(), nullable=False)
    # Hints
    data_resource: "DataResourceMetadata"
    data_function_log: DataFunctionLog

    def __repr__(self):
        return self._repr(
            id=self.id,
            data_function_log=self.data_function_log,
            data_resource=self.data_resource,
            direction=self.direction,
            processed_at=self.processed_at,
        )


class ConfiguredDataFunctionGraph(ConfiguredDataFunction):
    def build_cdfs(
        self, input: DataResourceStreamable = None
    ) -> List[ConfiguredDataFunction]:
        raise NotImplementedError

    # TODO: Idea here is to ensure inheriting classes are making keys correctly. maybe not necessary
    # def validate_cdfs(self):
    #     for cdf in self._internal_cdfs:
    #         if cdf is self.input_cdf:
    #             continue
    #         if not cdf.name.startswith(self.get_cdf_key_prefix()):
    #             raise Exception("Child-cdf does not have parent name prefix")

    def make_child_key(self, parent_key: str, child_name: str) -> str:
        return f"{parent_key}__{child_name}"

    def get_cdfs(self) -> List[ConfiguredDataFunction]:
        raise NotImplementedError

    def get_input_cdf(self) -> ConfiguredDataFunction:
        return self.get_cdfs()[0]

    def get_output_cdf(self) -> ConfiguredDataFunction:
        return self.get_cdfs()[-1]

    def get_interface(self) -> DataFunctionInterface:
        input_interface = self.get_input_cdf().get_interface()
        return DataFunctionInterface(
            inputs=input_interface.inputs,
            output=self.get_output_cdf().get_interface().output,
            requires_data_function_context=input_interface.requires_data_function_context,
        )


class ConfiguredDataFunctionChain(ConfiguredDataFunctionGraph):
    def __init__(
        self,
        _env: Environment,
        _key: str,
        _datafunction: DataFunctionChain,
        input: DataResourceStreamable = None,
    ):
        if input is not None:
            super().__init__(_env, _key, _datafunction, input=input)
        else:
            super().__init__(_env, _key, _datafunction)
        self.data_function_chain = _datafunction
        self._cdf_chain = self.build_cdfs(input)

    def build_cdfs(
        self, input: DataResourceStreamable = None
    ) -> List[ConfiguredDataFunction]:
        cdfs = []
        for fn in self.data_function_chain.function_chain:
            child_name = make_datafunction_key(fn)
            child_key = self.make_child_key(self.key, child_name)
            if input is not None:
                cdf = ConfiguredDataFunction(self.env, child_key, fn, input=input)
            else:
                cdf = ConfiguredDataFunction(self.env, child_key, fn)
            cdfs.append(cdf)
            input = cdf
        return cdfs

    def get_cdfs(self) -> List[ConfiguredDataFunction]:
        return self._cdf_chain


def configured_data_function_factory(
    env: Environment, key: str, df_like: Any, **inputs
) -> ConfiguredDataFunction:
    if isinstance(df_like, DataFunctionGraph):
        if isinstance(df_like, DataFunctionChain):
            cdf = ConfiguredDataFunctionChain(
                env, key, df_like, **inputs
            )  # TODO: WRONG
        else:
            raise NotImplementedError
    else:
        cdf = ConfiguredDataFunction(env, key, df_like, **inputs)
    return cdf
