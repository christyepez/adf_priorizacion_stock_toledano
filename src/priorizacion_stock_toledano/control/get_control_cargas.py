from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


CONTROL_PROCEDURE = "conf.GetControlCargas"
CONTROL_VIEW_NAME = "vw_control_cargas_priorizacion_stock"
VALID_SISTEMAS_FUENTE = {"SapHana", "sharepoint"}
VALID_PROPIETARIOS = {"VistasSapHana", "DatosPortalDeInformacion"}

STANDARD_COLUMNS = [
    "proceso",
    "sistema_fuente",
    "propietario_fuente",
    "columnas_archivo_fuente",
    "ruta_archivo_fuente",
    "nombre_archivo_fuente",
    "filtros_archivo_fuente",
    "ruta_archivo_destino",
    "nombre_archivo_destino",
    "extension_archivo_destino",
    "tipo_carga",
    "activo",
    "orden_ejecucion",
]

ORIGINAL_TO_STANDARD = {
    "Proceso": "proceso",
    "SistemaFuente": "sistema_fuente",
    "PropietarioFuente": "propietario_fuente",
    "ColumnasArchivoFuente": "columnas_archivo_fuente",
    "RutaArchivoFuente": "ruta_archivo_fuente",
    "NombreArchivoFuente": "nombre_archivo_fuente",
    "FiltrosArchivoFuente": "filtros_archivo_fuente",
    "RutaArchivoDestino": "ruta_archivo_destino",
    "NombreArchivoDestino": "nombre_archivo_destino",
    "ExtencionArchivoDestino": "extension_archivo_destino",
    "ExtensionArchivoDestino": "extension_archivo_destino",
    "TipoCarga": "tipo_carga",
    "Activo": "activo",
    "OrdenEjecucion": "orden_ejecucion",
}


@dataclass(frozen=True)
class SqlSecretNames:
    server: str
    database: str
    username: str
    password: str
    server_value: str | None = None
    database_value: str | None = None


def _clean(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    return value


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return True
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "si", "s", "activo", "active"}


def _to_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    return int(value)


def validate_inputs(proceso: str, sistema_fuente: str) -> tuple[str, str]:
    clean_proceso = (proceso or "").strip()
    clean_sistema = (sistema_fuente or "").strip()
    if not clean_proceso:
        raise ValueError("Proceso es obligatorio")
    if clean_sistema not in VALID_SISTEMAS_FUENTE:
        valid = ", ".join(sorted(VALID_SISTEMAS_FUENTE))
        raise ValueError(f"SistemaFuente invalido: {sistema_fuente!r}. Valores permitidos: {valid}")
    return clean_proceso, clean_sistema


def build_get_control_cargas_query(
    *,
    anio_mes_dia_inicial: str,
    anio_mes_dia_final: str,
    proceso: str,
    sistema_fuente: str,
) -> str:
    clean_proceso, clean_sistema = validate_inputs(proceso, sistema_fuente)
    inicial = _to_int(anio_mes_dia_inicial)
    final = _to_int(anio_mes_dia_final)
    safe_proceso = clean_proceso.replace("'", "''")
    safe_sistema = clean_sistema.replace("'", "''")
    return (
        f"EXEC {CONTROL_PROCEDURE} "
        f"@AñoMesDiaInicial = {inicial}, "
        f"@AñoMesDiaFinal = {final}, "
        f"@Proceso = N'{safe_proceso}', "
        f"@SistemaFuente = N'{safe_sistema}'"
    )


def normalize_control_record(record: Mapping[str, Any]) -> dict[str, Any]:
    normalized = {column: None for column in STANDARD_COLUMNS}
    for key, value in record.items():
        target = ORIGINAL_TO_STANDARD.get(key, key)
        if target in normalized:
            normalized[target] = _clean(value)

    normalized["activo"] = _to_bool(normalized.get("activo"))
    normalized["orden_ejecucion"] = _to_int(normalized.get("orden_ejecucion"), default=0)
    normalized["tipo_carga"] = normalized.get("tipo_carga") or ""
    normalized["filtros_archivo_fuente"] = normalized.get("filtros_archivo_fuente") or ""
    return normalized


def normalize_control_records(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [normalize_control_record(record) for record in records]


def filter_by_owner(records: Iterable[Mapping[str, Any]], propietario_fuente: str | None) -> list[dict[str, Any]]:
    if not propietario_fuente:
        return [dict(record) for record in records]
    owner = propietario_fuente.strip()
    if owner not in VALID_PROPIETARIOS:
        valid = ", ".join(sorted(VALID_PROPIETARIOS))
        raise ValueError(f"propietario_fuente invalido: {propietario_fuente!r}. Valores permitidos: {valid}")
    return [dict(record) for record in records if record.get("propietario_fuente") == owner]


def validate_active_records(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    active = [dict(record) for record in records if _to_bool(record.get("activo"))]
    if not active:
        raise ValueError("GetControlCargas no devolvio registros activos para los parametros indicados")
    return active


def jdbc_url(
    server: str,
    database: str,
    *,
    encrypt: str | bool = "true",
    trust_server_certificate: str | bool = "false",
) -> str:
    if not server or not database:
        raise ValueError("server y database son obligatorios para construir el JDBC URL")
    encrypt_value = str(encrypt).strip().lower()
    trust_value = str(trust_server_certificate).strip().lower()
    return (
        f"jdbc:sqlserver://{server};"
        f"databaseName={database};"
        f"encrypt={encrypt_value};"
        f"trustServerCertificate={trust_value};"
        "loginTimeout=30;"
    )


def _get_secret(dbutils: Any, secret_scope: str, secret_name: str, logical_name: str) -> str:
    try:
        return dbutils.secrets.get(secret_scope, secret_name)
    except Exception as exc:
        available = ""
        try:
            keys = sorted(secret.key for secret in dbutils.secrets.list(secret_scope))
            if keys:
                available = f" Secret keys visibles en el scope: {', '.join(keys)}."
        except Exception:
            pass
        raise ValueError(
            f"No se pudo leer el secreto '{logical_name}' con scope '{secret_scope}' y key '{secret_name}'. "
            "Valida que el scope exista, que tu usuario o job cluster tenga permiso READ sobre el scope, "
            "que la key exista dentro del scope y que el notebook/job este ejecutando la version desplegada "
            "mas reciente de databricks.yml/config.py."
            f"{available}"
        ) from exc


def read_sql_secret_values(dbutils: Any, secret_scope: str, secret_names: SqlSecretNames) -> dict[str, str]:
    if not secret_scope:
        raise ValueError("secret_scope es obligatorio")
    server = (secret_names.server_value or "").strip()
    database = (secret_names.database_value or "").strip()
    return {
        "server": server or _get_secret(dbutils, secret_scope, secret_names.server, "sql_control_server"),
        "database": database or _get_secret(dbutils, secret_scope, secret_names.database, "sql_control_database"),
        "username": _get_secret(dbutils, secret_scope, secret_names.username, "sql_control_username"),
        "password": _get_secret(dbutils, secret_scope, secret_names.password, "sql_control_password"),
    }


def read_get_control_cargas_jdbc(
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
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )


def normalize_spark_dataframe(df: Any) -> Any:
    from pyspark.sql.functions import col, lit

    selected = []
    source_columns = set(df.columns)
    for standard in STANDARD_COLUMNS:
        candidates = [standard] + [
            original for original, mapped in ORIGINAL_TO_STANDARD.items() if mapped == standard
        ]
        source = next((candidate for candidate in candidates if candidate in source_columns), None)
        if source:
            selected.append(col(source).alias(standard))
        else:
            selected.append(lit(None).alias(standard))
    return df.select(*selected)
