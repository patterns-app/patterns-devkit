from __future__ import annotations

from typing import Callable

import pytest
from pandas import DataFrame

from basis.core.data_function import (
    ConcreteDataFunctionInterface,
    ConcreteTypedDataAnnotation,
    ConfiguredDataFunction,
    ConfiguredDataFunctionChain,
    DataFunctionChain,
    DataFunctionInterface,
    DataFunctionLike,
    PythonDataFunction,
    TypedDataAnnotation,
    configured_data_function_factory,
)
from basis.core.data_resource import DataResource, DataResourceMetadata
from basis.core.sql.data_function import SqlDataFunction
from basis.core.streams import InputResources
from basis.utils.common import md5_hash
from basis.utils.registry import T, U
from tests.utils import (
    TestType1,
    df_generic,
    df_t1_sink,
    df_t1_source,
    df_t1_to_t2,
    make_test_env,
    make_test_execution_context,
)


@pytest.mark.parametrize(
    "annotation,expected",
    [
        (
            "DataResource[Type]",
            TypedDataAnnotation(
                data_resource_class="DataResource",
                otype_like="Type",
                is_iterable=False,
                is_generic=False,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataResource[Type]",
            ),
        ),
        (
            "DataSet[Type]",
            TypedDataAnnotation(
                data_resource_class="DataSet",
                otype_like="Type",
                is_iterable=False,
                is_generic=False,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataSet[Type]",
            ),
        ),
        (
            "DataResource[T]",
            TypedDataAnnotation(
                data_resource_class="DataResource",
                otype_like="T",
                is_iterable=False,
                is_generic=True,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataResource[T]",
            ),
        ),
    ],
)
def test_typed_annotation(annotation: str, expected: TypedDataAnnotation):
    tda = TypedDataAnnotation.from_type_annotation(annotation)
    assert tda == expected


def df_notworking(_1: int, _2: str, dr: DataResource[TestType1]):
    # TODO: how should DFI handle non-DR args?
    pass


def df4(
    dr: DataResource[T], dr2: DataResource[U], dr3: DataResource[U],
) -> DataFrame[T]:
    pass


def df_self(dr: DataResource[T], this: DataResource[T] = None) -> DataFrame[T]:
    pass


df_chain = DataFunctionChain("df_chain", [df_t1_to_t2, df_generic])


@pytest.mark.parametrize(
    "function,expected",
    [
        (
            df_t1_sink,
            DataFunctionInterface(
                inputs=[
                    TypedDataAnnotation(
                        data_resource_class="DataResource",
                        otype_like="TestType1",
                        name="dr",
                        is_iterable=False,
                        is_generic=False,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataResource[TestType1]",
                    )
                ],
                output=None,
                requires_data_function_context=True,
            ),
        ),
        (
            df_t1_to_t2,
            DataFunctionInterface(
                inputs=[
                    TypedDataAnnotation(
                        data_resource_class="DataResource",
                        otype_like="TestType1",
                        name="dr",
                        is_iterable=False,
                        is_generic=False,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataResource[TestType1]",
                    )
                ],
                output=TypedDataAnnotation(
                    data_resource_class="DataFrame",
                    otype_like="TestType2",
                    is_iterable=False,
                    is_generic=False,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[TestType2]",
                ),
                requires_data_function_context=False,
            ),
        ),
        (
            df_generic,
            DataFunctionInterface(
                inputs=[
                    TypedDataAnnotation(
                        data_resource_class="DataResource",
                        otype_like="T",
                        name="dr",
                        is_iterable=False,
                        is_generic=True,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataResource[T]",
                    )
                ],
                output=TypedDataAnnotation(
                    data_resource_class="DataFrame",
                    otype_like="T",
                    is_iterable=False,
                    is_generic=True,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_data_function_context=False,
            ),
        ),
        (
            df_self,
            DataFunctionInterface(
                inputs=[
                    TypedDataAnnotation(
                        data_resource_class="DataResource",
                        otype_like="T",
                        name="dr",
                        is_iterable=False,
                        is_generic=True,
                        is_optional=False,
                        is_variadic=False,
                        is_self_ref=False,
                        original_annotation="DataResource[T]",
                    ),
                    TypedDataAnnotation(
                        data_resource_class="DataResource",
                        otype_like="T",
                        name="this",
                        is_iterable=False,
                        is_generic=True,
                        is_optional=True,
                        is_variadic=False,
                        is_self_ref=True,
                        original_annotation="DataResource[T]",
                    ),
                ],
                output=TypedDataAnnotation(
                    data_resource_class="DataFrame",
                    otype_like="T",
                    is_iterable=False,
                    is_generic=True,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_data_function_context=False,
            ),
        ),
        (
            df_chain,
            DataFunctionInterface(
                inputs=[
                    TypedDataAnnotation(
                        data_resource_class="DataResource",
                        otype_like="TestType1",
                        name="dr",
                        is_iterable=False,
                        is_generic=False,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataResource[TestType1]",
                    )
                ],
                output=TypedDataAnnotation(
                    data_resource_class="DataFrame",
                    otype_like="T",
                    is_iterable=False,
                    is_generic=True,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_data_function_context=False,
            ),
        ),
    ],
)
def test_data_function_interface(
    function: DataFunctionLike, expected: DataFunctionInterface
):
    if isinstance(function, Callable):
        val = DataFunctionInterface.from_datafunction_definition(function)
        assert val == expected
    cdf = configured_data_function_factory(env, "_test", function)
    assert cdf.get_interface() == expected


env = make_test_env()


@pytest.mark.parametrize(
    "function,expected",
    [
        (
            df_t1_sink,
            ConcreteDataFunctionInterface(
                inputs=[
                    ConcreteTypedDataAnnotation(
                        data_resource_class="DataResource",
                        otype=TestType1,
                        name="dr",
                        is_iterable=False,
                        is_optional=False,
                        original_annotation="DataResource[TestType1]",
                    )
                ],
                output=None,
                requires_data_function_context=True,
            ),
        ),
        (
            df_generic,
            ConcreteDataFunctionInterface(
                inputs=[
                    ConcreteTypedDataAnnotation(
                        data_resource_class="DataResource",
                        otype=TestType1,
                        name="dr",
                        is_iterable=False,
                        is_optional=False,
                        original_annotation="DataResource[T]",
                    )
                ],
                output=ConcreteTypedDataAnnotation(
                    data_resource_class="DataFrame",
                    otype=TestType1,
                    is_iterable=False,
                    is_optional=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_data_function_context=False,
            ),
        ),
    ],
)
def test_concrete_data_function_interface(
    function: Callable, expected: ConcreteDataFunctionInterface
):
    input_data_resources: InputResources = {
        "dr": DataResourceMetadata(otype_uri="_test.TestType1"),
    }
    dfi = DataFunctionInterface.from_datafunction_definition(function)
    cdfi = ConcreteDataFunctionInterface.from_data_function_interface(
        env, input_data_resources, dfi
    )
    assert cdfi == expected


def test_python_data_function():
    df = PythonDataFunction(df_t1_sink)
    assert (
        df.key == df_t1_sink.__name__
    )  # TODO: do we really want this implicit name? As long as we error on duplicate should be ok

    k = "key1"
    df = PythonDataFunction(df_t1_sink, key=k)
    assert df.key == k

    dfi = df.get_interface()
    assert dfi is not None


def test_sql_data_function():
    sql = "select:T 1 from t:T"
    df = SqlDataFunction(sql)
    assert df.key == md5_hash(sql)

    k = "key1"
    df = SqlDataFunction(sql, key=k)
    assert df.key == k

    dfi = df.get_interface()
    assert dfi is not None

    assert len(dfi.inputs) == 1
    assert dfi.inputs[0].otype_like == "T"
    assert dfi.inputs[0].name == "t"
    assert dfi.output is not None
    assert dfi.output.otype_like == "T"


def test_sql_data_function2():
    sql = "select:T 1 from from t1:U join t2:Optional[T]"
    df = SqlDataFunction(sql)
    dfi = df.get_interface()
    assert dfi is not None

    assert len(dfi.inputs) == 2
    assert dfi.inputs[0].otype_like == "U"
    assert dfi.inputs[0].name == "t1"
    assert not dfi.inputs[0].is_optional
    assert dfi.inputs[1].otype_like == "T"
    assert dfi.inputs[1].name == "t2"
    assert dfi.inputs[1].is_optional


def test_configured_data_function_no_inputs():
    df = PythonDataFunction(df_t1_source)
    cdf1 = ConfiguredDataFunction(env, "cdf1", df)
    assert {cdf1: cdf1}[cdf1] is cdf1  # Test hash
    dfi = cdf1.get_interface()
    assert dfi.inputs == []
    assert dfi.output is not None
    assert cdf1.get_inputs() == {}
    assert cdf1.get_output_cdf() is cdf1
    assert not cdf1.is_graph()


def test_configured_data_function_inputs():
    df = PythonDataFunction(df_t1_source)
    cdf = ConfiguredDataFunction(env, "cdf", df)
    df = PythonDataFunction(df_t1_sink)
    with pytest.raises(Exception):
        # Bad input
        ConfiguredDataFunction(env, "cdf_fail", df, input="Turkey")  # type: ignore
    cdf1 = ConfiguredDataFunction(env, "cdf1", df, input=cdf)
    dfi = cdf1.get_interface()
    assert len(dfi.inputs) == 1
    assert dfi.output is None
    assert cdf1.get_inputs() == {"input": cdf}
    assert cdf1.get_input("input") is cdf
    assert cdf1.get_data_stream_input("input") is cdf


def test_configured_data_function_chain():
    df = PythonDataFunction(df_t1_source)
    cdf = ConfiguredDataFunction(env, "cdf", df)
    cdf1 = ConfiguredDataFunctionChain(env, "cdf1", df_chain, input=cdf)
    dfi = cdf1.get_interface()
    assert len(dfi.inputs) == 1
    assert dfi.output is not None
    assert cdf1.get_inputs() == {"input": cdf}
    assert cdf1.get_input("input") is cdf
    assert cdf1.get_data_stream_input("input") is cdf
    # Output CDF
    output_cdf = cdf1.get_output_cdf()
    assert output_cdf is not cdf1
    assert cdf1.key in output_cdf.key
    out_dfi = output_cdf.get_interface()
    assert len(out_dfi.inputs) == 1
    assert out_dfi.output is not None
    # Children
    children = cdf1.build_cdfs()
    assert len(children) == 2
    assert len(cdf1.get_cdfs()) == 2
