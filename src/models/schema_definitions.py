from __future__ import annotations

from typing import Dict, List

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DateType,
)

TYPE_MAPPING = {
    "string": StringType,
    "double": DoubleType,
    "integer": IntegerType,
    "timestamp": TimestampType,
    "date": DateType,
}


def build_struct_type(schema_name: str, all_schemas: Dict[str, List[Dict]]) -> StructType:
    if schema_name not in all_schemas:
        raise KeyError(f"Schema {schema_name} not defined")

    fields = []
    for field_def in all_schemas[schema_name]:
        data_type = field_def["type"].lower()
        if data_type not in TYPE_MAPPING:
            raise ValueError(f"Unsupported type {data_type}")
        fields.append(
            StructField(
                field_def["name"],
                TYPE_MAPPING[data_type](),
                bool(field_def.get("nullable", True)),
            )
        )
    return StructType(fields)
