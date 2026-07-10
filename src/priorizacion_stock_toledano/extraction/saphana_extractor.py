from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


SAP_METRICS_VIEW_NAME = "vw_metrics_ext_saphana_priorizacion_stock"
SAP_SOURCE_SYSTEM = "SapHana"


@dataclass(frozen=True)
class SapHanaSecretNames:
    server: str
    username: str
    password: str
    port: str | None = None


def _required(record: Mapping[str, Any], key: str) -> str:
    value = record.get(key)
    if value is None or str(value).strip() == "":
        raise ValueError(f"Campo requerido ausente para SAP HANA: {key}")
    return str(value).strip()


def _quote_hana_identifier(identifier: str) -> str:
    clean = (identifier or "").strip()
    if not clean:
        raise ValueError("Identificador SAP HANA vacio")
    if ";" in clean or "\x00" in clean:
        raise ValueError(f"Identificador SAP HANA invalido: {identifier!r}")
    return '"' + clean.replace('"', '""') + '"'


def _clean_filter(filter_sql: Any) -> str:
    value = "" if filter_sql is None else str(filter_sql).strip()
    if not value:
        return ""
    if ";" in value or "--" in value or "/*" in value:
        raise ValueError("FiltrosArchivoFuente contiene tokens no permitidos")
    return value


def build_saphana_query(record: Mapping[str, Any]) -> str:
    columns = _required(record, "columnas_archivo_fuente")
    schema = _required(record, "ruta_archivo_fuente")
    source_name = _required(record, "nombre_archivo_fuente")
    filters = _clean_filter(record.get("filtros_archivo_fuente"))

    if ";" in columns or "\x00" in columns:
        raise ValueError("ColumnasArchivoFuente contiene tokens no permitidos")

    query = (
        f"SELECT {columns}\n"
        f"FROM {_quote_hana_identifier(schema)}.{_quote_hana_identifier(source_name)}"
    )
    if filters:
        query = f"{query}\n{filters}"
    return query


def build_bronze_path(
    *,
    storage_account: str,
    folder: str,
    filename: str,
    extension: str,
    container: str = "bronze",
) -> str:
    if not storage_account:
        raise ValueError("storage_account es obligatorio")
    clean_folder = (folder or "").strip().strip("/")
    clean_filename = _required({"filename": filename}, "filename")
    clean_extension = "" if extension is None else str(extension).strip()
    if clean_extension and not clean_extension.startswith("."):
        clean_extension = "." + clean_extension
    relative_path = f"{clean_folder}/{clean_filename}{clean_extension}" if clean_folder else f"{clean_filename}{clean_extension}"
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{relative_path}"


def choose_output_format(record: Mapping[str, Any]) -> str:
    extension = str(record.get("extension_archivo_destino") or "").strip().lower()
    tipo_carga = str(record.get("tipo_carga") or "").strip().lower()
    if "parquet" in {extension.lstrip("."), tipo_carga}:
        return "parquet"
    if "delta" in {extension.lstrip("."), tipo_carga}:
        return "delta"
    return "delta"


def hana_jdbc_url(server: str, port: str | None = None) -> str:
    clean_server = (server or "").strip()
    if not clean_server:
        raise ValueError("Servidor SAP HANA obligatorio")
    clean_port = (port or "").strip() or "30015"
    return f"jdbc:sap://{clean_server}:{clean_port}"


def read_sap_secret_values(dbutils: Any, secret_scope: str, secret_names: SapHanaSecretNames) -> dict[str, str]:
    if not secret_scope:
        raise ValueError("secret_scope es obligatorio")
    values = {
        "server": dbutils.secrets.get(secret_scope, secret_names.server),
        "username": dbutils.secrets.get(secret_scope, secret_names.username),
        "password": dbutils.secrets.get(secret_scope, secret_names.password),
    }
    if secret_names.port:
        values["port"] = dbutils.secrets.get(secret_scope, secret_names.port)
    else:
        values["port"] = "30015"
    return values


def read_saphana_jdbc(
    spark: Any,
    *,
    url: str,
    username: str,
    password: str,
    query: str,
    driver: str = "com.sap.db.jdbc.Driver",
) -> Any:
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("query", query)
        .option("user", username)
        .option("password", password)
        .option("driver", driver)
        .load()
    )


def add_technical_columns(
    df: Any,
    *,
    source_system: str,
    process_name: str,
    execution_id: str,
    source_object: str,
) -> Any:
    from pyspark.sql.functions import current_timestamp, lit

    return (
        df.withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_system", lit(source_system))
        .withColumn("process_name", lit(process_name))
        .withColumn("execution_id", lit(execution_id))
        .withColumn("source_object", lit(source_object))
    )


def write_bronze(df: Any, *, path: str, output_format: str) -> int:
    rows_written = df.count()
    writer = df.write.format(output_format).mode("overwrite")
    if output_format == "delta":
        writer = writer.option("mergeSchema", "true")
    writer.save(path)
    return rows_written


def metric_record(
    *,
    process_name: str,
    sistema_fuente: str,
    source_object: str,
    target_path: str,
    rows_read: int,
    rows_written: int,
    status: str,
    error_message: str = "",
) -> dict[str, Any]:
    return {
        "process_name": process_name,
        "sistema_fuente": sistema_fuente,
        "source_object": source_object,
        "target_path": target_path,
        "rows_read": int(rows_read),
        "rows_written": int(rows_written),
        "status": status,
        "error_message": error_message,
        "metric_timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
