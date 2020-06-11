from __future__ import annotations

from typing import Optional

import pytest

from basis.core.conversion import StorageFormat, get_converter_lookup
from basis.core.conversion.converter import Conversion, ConversionCostLevel
from basis.core.data_format import DataFormat
from basis.core.storage import StorageType


@pytest.mark.parametrize(
    "conversion,expected_cost",
    [
        # Memory to DB
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST),
                StorageFormat(StorageType.POSTGRES_DATABASE, DataFormat.DATABASE_TABLE),
            ),
            ConversionCostLevel.OVER_WIRE.value,
        ),
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),
                StorageFormat(StorageType.POSTGRES_DATABASE, DataFormat.DATABASE_TABLE),
            ),
            ConversionCostLevel.OVER_WIRE.value
            + ConversionCostLevel.MEMORY.value,  # To DictList, then to DB
        ),
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST_GENERATOR),
                StorageFormat(StorageType.POSTGRES_DATABASE, DataFormat.DATABASE_TABLE),
            ),
            ConversionCostLevel.OVER_WIRE.value,
        ),
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATABASE_TABLE_REF),
                StorageFormat(StorageType.POSTGRES_DATABASE, DataFormat.DATABASE_TABLE),
            ),
            ConversionCostLevel.OVER_WIRE.value,  # TODO Not really over the wire! Converter doesn't understand the REF is free
        ),
        # DB to memory
        (
            (
                StorageFormat(StorageType.POSTGRES_DATABASE, DataFormat.DATABASE_TABLE),
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATABASE_TABLE_REF),
            ),
            ConversionCostLevel.OVER_WIRE.value,  # TODO see above
        ),
        (
            (
                StorageFormat(StorageType.POSTGRES_DATABASE, DataFormat.DATABASE_TABLE),
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST),
            ),
            ConversionCostLevel.OVER_WIRE.value,
        ),
        (
            (
                StorageFormat(StorageType.POSTGRES_DATABASE, DataFormat.DATABASE_TABLE),
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),
            ),
            ConversionCostLevel.OVER_WIRE.value,
        ),
        # Memory to memory
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST),
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),
            ),
            ConversionCostLevel.MEMORY.value,
        ),
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATAFRAME),
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST),
            ),
            ConversionCostLevel.MEMORY.value,
        ),
        # Memory to DB round-trip
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATABASE_TABLE_REF),
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST),
            ),
            ConversionCostLevel.OVER_WIRE.value
            * 2,  # TODO Not really 2x! Converter doesn't understand the REF is free
        ),
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DICT_LIST),
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATABASE_TABLE_REF),
            ),
            ConversionCostLevel.OVER_WIRE.value * 2,
            # TODO see above
        ),
        # DB to memory round-trip
        (
            (
                StorageFormat(StorageType.POSTGRES_DATABASE, DataFormat.DATABASE_TABLE),
                StorageFormat(StorageType.MYSQL_DATABASE, DataFormat.DATABASE_TABLE),
            ),
            ConversionCostLevel.OVER_WIRE.value * 2,
        ),
        # Unsupported conversions (currently)
        (
            (
                StorageFormat(StorageType.DICT_MEMORY, DataFormat.DATABASE_CURSOR),
                StorageFormat(StorageType.MYSQL_DATABASE, DataFormat.DATABASE_TABLE),
            ),
            None,
        ),
    ],
)
def test_conversion_costs(conversion: Conversion, expected_cost: Optional[int]):
    cp = get_converter_lookup().get_lowest_cost_path(conversion)
    if expected_cost is None:
        assert cp is None
    else:
        assert cp.total_cost == expected_cost
