from __future__ import annotations

from typing import Any


M_OFERTAS_DOC_VENTAS_CASTS = {
    "DATE_SQL": "date",
    "ERDAT": "date",
    "ERZET": "timestamp",
    "FKDAT": "date",
    "KWMENG": "decimal(16,4)",
    "Cantidad_Pedida": "int",
    "KUNNR": "string",
    "IdMaterial": "string",
    "BRGEW": "decimal(16,4)",
}

CV_LO_PEDIDO_CASTS = {
    "MATNR": "string",
    "WERKS": "int",
    "DATE_SQL": "date",
    "EBELN": "string",
    "VBELN": "string",
    "Cantidad_Pedida": "int",
    "Peso_Pedido": "decimal(16,4)",
    "PesoLBS_Pedido": "decimal(16,4)",
    "Volumen_Pedido_LBS_DOC": "decimal(16,4)",
}

ACCENT_TRANSLATION = str.maketrans(
    {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "Á": "A",
        "É": "E",
        "Í": "I",
        "Ó": "O",
        "Ú": "U",
        "ñ": "n",
        "Ñ": "N",
    }
)


def required_columns(cast_map: dict[str, str]) -> list[str]:
    return list(cast_map.keys())


def validate_required_columns(columns: list[str], required: list[str]) -> None:
    missing = [column for column in required if column not in columns]
    if missing:
        raise ValueError(f"Faltan columnas obligatorias: {', '.join(missing)}")


def clean_null_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.lower() in {"", "null", "none", "nan", "n/a"}:
            return None
        return stripped
    return value


def remove_accents_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.translate(ACCENT_TRANSLATION)
    return value


def clean_nulls(df: Any) -> Any:
    from pyspark.sql.functions import col, trim, when
    from pyspark.sql.types import StringType

    result = df
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            result = result.withColumn(
                field.name,
                when(trim(col(field.name)).isin("", "null", "NULL", "None", "nan", "N/A"), None).otherwise(trim(col(field.name))),
            )
    return result


def remove_accents_from_strings(df: Any) -> Any:
    from pyspark.sql.functions import translate
    from pyspark.sql.types import StringType

    source_chars = "áéíóúÁÉÍÓÚñÑ"
    target_chars = "aeiouAEIOUnN"
    result = df
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            result = result.withColumn(field.name, translate(field.name, source_chars, target_chars))
    return result


def select_and_cast(df: Any, cast_map: dict[str, str], execution_id: str) -> Any:
    from pyspark.sql.functions import col, lit

    validate_required_columns(df.columns, required_columns(cast_map))
    cleaned = clean_nulls(remove_accents_from_strings(df.select(*required_columns(cast_map))))
    return cleaned.select(
        *[col(column).cast(data_type).alias(column) for column, data_type in cast_map.items()],
        lit(execution_id).alias("execution_id"),
    )


def transform_m_ofertas_doc_ventas(df: Any, execution_id: str) -> Any:
    return select_and_cast(df, M_OFERTAS_DOC_VENTAS_CASTS, execution_id)


def transform_cv_lo_pedido(df: Any, execution_id: str) -> Any:
    return select_and_cast(df, CV_LO_PEDIDO_CASTS, execution_id)


def silver_table_path(storage_account: str, relative_path: str) -> str:
    if not storage_account:
        raise ValueError("storage_account es obligatorio")
    clean_relative = relative_path.strip("/")
    return f"abfss://silver@{storage_account}.dfs.core.windows.net/{clean_relative}"
