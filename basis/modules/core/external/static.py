from dataclasses import dataclass
from typing import Any, Iterator

import pandas as pd
from pandas import DataFrame

from basis.core.data_function import datafunction
from basis.core.external import (
    ConfiguredExternalProvider,
    ConfiguredExternalResource,
    ConfiguredExternalResourceState,
    ExternalDataProvider,
    ExternalDataResource,
    ExtractorResult,
)
from basis.core.runnable import DataFunctionContext
from basis.utils.common import utcnow

local_provider = ExternalDataProvider(
    name="LocalMemoryProvider",
    verbose_name="Local memory provider",
    description="Dummy provider for local memory external objects (DataFrames, etc)",
    requires_authentication=False,
)


def extract_dataframe(
    configured_provider: ConfiguredExternalProvider,
    configured_resource: ConfiguredExternalResource,
    configured_resource_state: ConfiguredExternalResourceState,
) -> Iterator[ExtractorResult]:
    if configured_resource_state.high_water_mark is not None:
        # Just emit once
        return
    yield ExtractorResult(
        records=configured_resource.get_config_value("dataframe"),
        new_high_water_mark=utcnow(),
    )


@dataclass
class DataFrameResourceConfiguration:
    dataframe: DataFrame


ExternalDataResource(
    name="DataFrameExternalResource",
    provider=local_provider,
    otype="Any",  # TODO
    verbose_name=f"Static DataFrame External Resource",
    description=f"Use a DataFrame external to Basis as a node input",
    default_extractor=extract_dataframe,
    configuration_class=DataFrameResourceConfiguration,
)


# def source_dataframe(ctx: DataFunctionContext) -> DataFrame[Any]:
#     return ctx.config

# def static_source_resource(df: DataFunctionCallable) -> DataFunctionCallable:
#     """
#     Only run function once, since source data is static and only one output
#     """
#
#     def csr_key_from_node(node: ConfiguredDataFunction) -> str:
#         return f"_mock_source_resource_from_node_{node.key}"
#
#     @wraps(df)
#     def static_source(*args, **kwargs):
#         ctx: Any = args[0]  # DataFunctionContext = args[0]
#         key = csr_key_from_node(ctx.node)
#         state = (
#             ctx._metadata_session.query(ConfiguredSourceResourceState)
#             .filter(ConfiguredSourceResourceState.configured_external_resource_key == key)
#             .first()
#         )
#         if state is not None:
#             # Already run once, don't run again
#             raise InputExhaustedException()
#         ret = df(*args, **kwargs)
#         state = ConfiguredSourceResourceState(
#             configured_external_resource_key=key, high_water_mark=utcnow(),
#         )
#         ctx._metadata_session.add(state)
#         return ret
#
#     # TODO: hmmm, we should have a unified interface for all DataFunctions
#     #   Probably has to be class / class wrapper?
#     #   wrapped either with decorator at declare time or ensured at runtime?
#     if hasattr(df, "get_interface"):
#
#         def call_(self, *args, **kwargs):
#             return static_source(*args, **kwargs)
#
#         df.__call__ = call_
#         return df
#
#         # class S:
#         #     def __call__(self, *args, **kwargs):
#         #         return static_source(*args, **kwargs)
#         #
#         #     def __getattr__(self, item):
#         #         return getattr(df, item)
#         #
#         # return S()
#
#     return static_source
