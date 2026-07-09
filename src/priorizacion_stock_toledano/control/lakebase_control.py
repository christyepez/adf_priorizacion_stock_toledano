from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .get_control_cargas import (
    CONTROL_VIEW_NAME,
    STANDARD_COLUMNS,
    VALID_PROPIETARIOS,
    VALID_SISTEMAS_FUENTE,
    normalize_spark_dataframe,
)


LAKEBASE_CONTROL_VIEW = "control.vw_control_cargas_priorizacion_stock"
LAKEBASE_DRIVER = "org.postgresql.Driver"


@dataclass(frozen=True)
class LakebaseSecretNames:
    host: str
    port: str
    database: str
    username: str
    password: str


def validate_lakebase_inputs(proceso: str, sistema_fuente: str, propietario_fuente: str | None = None) -> tuple[str, str, str | None]:
    clean_proceso = (proceso or "").strip()
    clean_sistema = (sistema_fuente or "").strip()
    clean_owner = (propietario_fuente or "").strip() or None
    if not clean_proceso:
        raise ValueError("Proceso es obligatorio")
    if clean_sistema not in VALID_SISTEMAS_FUENTE:
        valid = ", ".join(sorted(VALID_SISTEMAS_FUENTE))
        raise ValueError(f"SistemaFuente invalido: {sistema_fuente!r}. Valores permitidos: {valid}")
    if clean_owner and clean_owner not in VALID_PROPIETARIOS:
        valid = ", ".join(sorted(VALID_PROPIETARIOS))
        raise ValueError(f"propietario_fuente invalido: {propietario_fuente!r}. Valores permitidos: {valid}")
    return clean_proceso, clean_sistema, clean_owner


def lakebase_jdbc_url(host: str, port: str | int, database: str) -> str:
    clean_host = (host or "").strip()
    clean_port = str(port or "").strip()
    clean_database = (database or "").strip()
    if not clean_host or not clean_port or not clean_database:
        raise ValueError("host, port y database son obligatorios para construir el JDBC URL Lakebase")
    return f"jdbc:postgresql://{clean_host}:{clean_port}/{clean_database}?sslmode=require"


def read_lakebase_secret_values(dbutils: Any, secret_scope: str, secret_names: LakebaseSecretNames) -> dict[str, str]:
    if not secret_scope:
        raise ValueError("secret_scope es obligatorio")
    return {
        "host": dbutils.secrets.get(secret_scope, secret_names.host),
        "port": dbutils.secrets.get(secret_scope, secret_names.port),
        "database": dbutils.secrets.get(secret_scope, secret_names.database),
        "username": dbutils.secrets.get(secret_scope, secret_names.username),
        "password": dbutils.secrets.get(secret_scope, secret_names.password),
    }


def build_lakebase_control_query(
    *,
    proceso: str,
    sistema_fuente: str,
    propietario_fuente: str | None = None,
    anio_mes_dia_inicial: str | int = 0,
    anio_mes_dia_final: str | int = 0,
    control_view: str = LAKEBASE_CONTROL_VIEW,
) -> str:
    clean_proceso, clean_sistema, clean_owner = validate_lakebase_inputs(proceso, sistema_fuente, propietario_fuente)
    initial = int(anio_mes_dia_inicial or 0)
    final = int(anio_mes_dia_final or 0)
    safe_proceso = clean_proceso.replace("'", "''")
    safe_sistema = clean_sistema.replace("'", "''")
    filters = [
        f"proceso = '{safe_proceso}'",
        f"sistema_fuente = '{safe_sistema}'",
        "activo = TRUE",
        f"valid_from <= CASE WHEN {initial} = 0 THEN 99991231 ELSE {initial} END",
        f"valid_to >= CASE WHEN {final} = 0 THEN 0 ELSE {final} END",
    ]
    if clean_owner:
        safe_owner = clean_owner.replace("'", "''")
        filters.append(f"propietario_fuente = '{safe_owner}'")

    return (
        "SELECT "
        "proceso, sistema_fuente, propietario_fuente, columnas_archivo_fuente, "
        "ruta_archivo_fuente, nombre_archivo_fuente, filtros_archivo_fuente, "
        "ruta_archivo_destino, nombre_archivo_destino, extension_archivo_destino, "
        "tipo_carga, activo, orden_ejecucion "
        f"FROM {control_view} "
        f"WHERE {' AND '.join(filters)} "
        "ORDER BY orden_ejecucion"
    )


def read_lakebase_control_jdbc(
    spark: Any,
    *,
    url: str,
    username: str,
    password: str,
    query: str,
) -> Any:
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("query", query)
        .option("user", username)
        .option("password", password)
        .option("driver", LAKEBASE_DRIVER)
        .load()
    )


def normalize_lakebase_control_dataframe(df: Any) -> Any:
    return normalize_spark_dataframe(df).select(*STANDARD_COLUMNS)


def create_control_temp_view(df: Any, view_name: str = CONTROL_VIEW_NAME) -> None:
    df.createOrReplaceTempView(view_name)
