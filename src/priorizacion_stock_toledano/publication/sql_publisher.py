from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


REQUIRED_PUBLICATION_COLUMNS = ["cod_cliente", "prioridad", "Estatus", "create_timestamp"]
ALLOWED_PUBLICATION_MODES = {"append", "overwrite", "truncate_insert"}
SQL_SERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
DEFAULT_TARGET_SCHEMA = "dbo"
DEFAULT_TARGET_TABLE = "Int_Prioriza_Clientes"


@dataclass(frozen=True)
class SqlPublicationSecretNames:
    username: str
    password: str


@dataclass(frozen=True)
class PublicationMetrics:
    rows_read: int
    rows_written: int
    target_table: str
    status: str
    error_message: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def jdbc_url(
    server: str,
    database: str,
    *,
    encrypt: str | bool = "true",
    trust_server_certificate: str | bool = "false",
) -> str:
    clean_server = (server or "").strip()
    clean_database = (database or "").strip()
    if not clean_server or not clean_database:
        raise ValueError("server y database son obligatorios para construir el JDBC URL")
    encrypt_value = str(encrypt).strip().lower()
    trust_value = str(trust_server_certificate).strip().lower()
    return (
        f"jdbc:sqlserver://{clean_server};"
        f"databaseName={clean_database};"
        f"encrypt={encrypt_value};"
        f"trustServerCertificate={trust_value};"
        "loginTimeout=30;"
    )


def target_table_identifier(schema: str = DEFAULT_TARGET_SCHEMA, table: str = DEFAULT_TARGET_TABLE) -> str:
    clean_schema = (schema or "").strip()
    clean_table = (table or "").strip()
    if not clean_schema or not clean_table:
        raise ValueError("schema y table son obligatorios")
    return f"{clean_schema}.{clean_table}"


def validate_publication_mode(mode: str) -> str:
    clean_mode = (mode or "").strip().lower()
    if clean_mode not in ALLOWED_PUBLICATION_MODES:
        allowed = ", ".join(sorted(ALLOWED_PUBLICATION_MODES))
        raise ValueError(f"Modo de publicacion invalido: {mode!r}. Valores permitidos: {allowed}")
    return clean_mode


def validate_publication_columns(df: Any, required_columns: list[str] | None = None) -> None:
    required = required_columns or REQUIRED_PUBLICATION_COLUMNS
    current = set(getattr(df, "columns", []))
    missing = [column for column in required if column not in current]
    if missing:
        raise ValueError(f"Faltan columnas requeridas para publicar a SQL Server: {', '.join(missing)}")


def select_publication_columns(df: Any) -> Any:
    validate_publication_columns(df)
    return df.select(*REQUIRED_PUBLICATION_COLUMNS)


def read_sql_publication_secret_values(
    dbutils: Any,
    secret_scope: str,
    secret_names: SqlPublicationSecretNames,
) -> dict[str, str]:
    if not secret_scope:
        raise ValueError("secret_scope es obligatorio")
    return {
        "username": dbutils.secrets.get(secret_scope, secret_names.username),
        "password": dbutils.secrets.get(secret_scope, secret_names.password),
    }


def _jdbc_write_mode(mode: str) -> tuple[str, bool]:
    clean_mode = validate_publication_mode(mode)
    if clean_mode == "truncate_insert":
        return "overwrite", True
    return clean_mode, False


def write_publication_jdbc(
    df: Any,
    *,
    url: str,
    username: str,
    password: str,
    target_table: str,
    mode: str,
) -> None:
    spark_mode, truncate = _jdbc_write_mode(mode)
    writer = (
        df.write.format("jdbc")
        .mode(spark_mode)
        .option("url", url)
        .option("dbtable", target_table)
        .option("user", username)
        .option("password", password)
        .option("driver", SQL_SERVER_DRIVER)
        .option("batchsize", "10000")
    )
    if truncate:
        writer = writer.option("truncate", "true")
    writer.save()


def sanitize_error_message(message: str, *sensitive_values: str | None) -> str:
    sanitized = message or ""
    for value in sensitive_values:
        if value:
            sanitized = sanitized.replace(value, "<redacted>")
    return sanitized


def publish_results_to_sql(
    spark: Any,
    *,
    source_table: str,
    url: str,
    username: str,
    password: str,
    target_schema: str = DEFAULT_TARGET_SCHEMA,
    target_table: str = DEFAULT_TARGET_TABLE,
    mode: str = "append",
) -> PublicationMetrics:
    target = target_table_identifier(target_schema, target_table)
    try:
        validate_publication_mode(mode)
        df_source = spark.table(source_table)
        df_publication = select_publication_columns(df_source)
        rows_read = df_publication.count()
        write_publication_jdbc(
            df_publication,
            url=url,
            username=username,
            password=password,
            target_table=target,
            mode=mode,
        )
        return PublicationMetrics(
            rows_read=rows_read,
            rows_written=rows_read,
            target_table=target,
            status="success",
        )
    except Exception as exc:
        error_message = sanitize_error_message(str(exc), username, password)
        return PublicationMetrics(
            rows_read=0,
            rows_written=0,
            target_table=target,
            status="error",
            error_message=error_message,
        )
