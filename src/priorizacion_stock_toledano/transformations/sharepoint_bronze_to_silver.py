from __future__ import annotations

from typing import Any

from .sap_bronze_to_silver import (
    clean_null_value,
    clean_nulls,
    remove_accents_from_strings,
    remove_accents_value,
    silver_table_path,
    validate_required_columns,
)


GRUPOS_PRIORIZACION_RENAME_MAP = {
    "Cliente": "cliente",
    "Nombre": "nombre",
    "CADENA": "cadena",
    "GRUPO": "grupo",
    "Priorización": "priorizacion",
}

PRIORIZACIONES_PREVIAS_RENAME_MAP = {
    "CODIGO": "codigo",
    "CLIENTE": "cliente",
    "PRIORIZACIÓN": "priorizacion",
}

GRUPOS_PRIORIZACION_CASTS = {
    "cliente": "int",
    "nombre": "string",
    "cadena": "string",
    "grupo": "string",
    "priorizacion": "int",
}

PRIORIZACIONES_PREVIAS_CASTS = {
    "codigo": "string",
    "cliente": "string",
    "priorizacion": "int",
}


def required_source_columns(rename_map: dict[str, str]) -> list[str]:
    return list(rename_map.keys())


def expected_output_columns(cast_map: dict[str, str]) -> list[str]:
    return list(cast_map.keys())


def rename_columns(df: Any, rename_map: dict[str, str]) -> Any:
    validate_required_columns(df.columns, required_source_columns(rename_map))
    renamed = df
    for source, target in rename_map.items():
        if source != target:
            renamed = renamed.withColumnRenamed(source, target)
    return renamed


def select_and_cast_sharepoint(
    df: Any,
    *,
    rename_map: dict[str, str],
    cast_map: dict[str, str],
    execution_id: str,
) -> Any:
    from pyspark.sql.functions import col, lit

    renamed = rename_columns(df, rename_map)
    cleaned = clean_nulls(remove_accents_from_strings(renamed.select(*expected_output_columns(cast_map))))
    return cleaned.select(
        *[col(column).cast(data_type).alias(column) for column, data_type in cast_map.items()],
        lit(execution_id).alias("execution_id"),
    )


def transform_grupos_priorizacion(df: Any, execution_id: str) -> Any:
    return select_and_cast_sharepoint(
        df,
        rename_map=GRUPOS_PRIORIZACION_RENAME_MAP,
        cast_map=GRUPOS_PRIORIZACION_CASTS,
        execution_id=execution_id,
    )


def transform_priorizaciones_previas(df: Any, execution_id: str) -> Any:
    return select_and_cast_sharepoint(
        df,
        rename_map=PRIORIZACIONES_PREVIAS_RENAME_MAP,
        cast_map=PRIORIZACIONES_PREVIAS_CASTS,
        execution_id=execution_id,
    )
