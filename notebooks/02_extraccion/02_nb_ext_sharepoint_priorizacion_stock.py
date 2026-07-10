# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Extraccion Bronze - Ext Sharepoint Priorizacion Stock
# MAGIC
# MAGIC Priorizacion de Stock Toledano.

# COMMAND ----------
# Bootstrap del paquete del proyecto para ejecuciones como Workspace Files o Bundle.
import sys
from pathlib import Path


def _add_project_src_to_path() -> None:
    candidates = []
    cwd = Path.cwd()
    candidates.extend([
        cwd / "src",
        cwd.parent / "src",
        cwd.parent.parent / "src",
        cwd.parent.parent.parent / "src",
    ])
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        workspace_file = Path("/Workspace") / notebook_path.lstrip("/")
        candidates.extend([
            workspace_file.parent / "src",
            workspace_file.parent.parent / "src",
            workspace_file.parent.parent.parent / "src",
            workspace_file.parent.parent.parent.parent / "src",
        ])
    except Exception:
        pass

    for candidate in candidates:
        if (candidate / "priorizacion_stock_toledano").exists():
            candidate_str = str(candidate)
            if candidate_str not in sys.path:
                sys.path.insert(0, candidate_str)
            return

    raise ModuleNotFoundError(
        "No se encontro src/priorizacion_stock_toledano. "
        "Despliega el bundle completo o instala el paquete wheel en el job cluster."
    )


_add_project_src_to_path()

dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("Proceso", "Modelo_Priorizacion_Stock")
dbutils.widgets.text("SistemaFuente", "sharepoint")
dbutils.widgets.text("AñoMesDiaInicial", "0")
dbutils.widgets.text("AñoMesDiaFinal", "0")
dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("fail_fast", "true")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("sql_control_server", "")
dbutils.widgets.text("sql_control_database", "")
dbutils.widgets.text("sql_control_server_secret", "")
dbutils.widgets.text("sql_control_database_secret", "")
dbutils.widgets.text("sql_control_username_secret", "")
dbutils.widgets.text("sql_control_password_secret", "")
dbutils.widgets.text("sql_control_encrypt", "true")
dbutils.widgets.text("sql_control_trust_server_certificate", "false")
dbutils.widgets.text("sharepoint_base_url", "")
dbutils.widgets.text("sharepoint_auth_mode", "graph_client_credentials")
dbutils.widgets.text("sharepoint_token_secret", "")
dbutils.widgets.text("sharepoint_client_id_secret", "")
dbutils.widgets.text("sharepoint_client_secret_secret", "")
dbutils.widgets.text("sharepoint_tenant_id_secret", "")
dbutils.widgets.text("sharepoint_site_id_secret", "")
dbutils.widgets.text("sharepoint_drive_id_secret", "")
dbutils.widgets.text("sharepoint_propietario_fuente", "DatosPortalDeInformacion")
dbutils.widgets.text("metrics_delta_table", "")

from uuid import uuid4

import requests

from priorizacion_stock_toledano.control.get_control_cargas import (
    SqlSecretNames,
    build_get_control_cargas_query,
    jdbc_url,
    normalize_spark_dataframe,
    read_get_control_cargas_jdbc,
    read_sql_secret_values,
)
from priorizacion_stock_toledano.extraction.sharepoint_extractor import (
    SHAREPOINT_METRICS_VIEW_NAME,
    SharePointSecretNames,
    assert_https_url,
    build_bronze_path,
    build_sharepoint_download_url,
    build_source_file_name,
    count_rows_if_applicable,
    infer_file_type,
    metric_record,
    read_sharepoint_secret_values,
    reject_signed_or_secret_url,
    write_bytes_to_path,
)

from pyspark.sql.functions import col

ambiente = dbutils.widgets.get("ambiente")
process_name = dbutils.widgets.get("Proceso")
sistema_fuente = dbutils.widgets.get("SistemaFuente")
anio_mes_dia_inicial = dbutils.widgets.get("AñoMesDiaInicial")
anio_mes_dia_final = dbutils.widgets.get("AñoMesDiaFinal")
execution_id = dbutils.widgets.get("execution_id").strip() or str(uuid4())
fail_fast = dbutils.widgets.get("fail_fast").strip().lower() == "true"
secret_scope = dbutils.widgets.get("secret_scope").strip()
storage_account_name = dbutils.widgets.get("storage_account_name").strip()
sql_control_server = dbutils.widgets.get("sql_control_server").strip()
sql_control_database = dbutils.widgets.get("sql_control_database").strip()
sql_control_encrypt = dbutils.widgets.get("sql_control_encrypt").strip() or "true"
sql_control_trust_server_certificate = (
    dbutils.widgets.get("sql_control_trust_server_certificate").strip() or "false"
)
sharepoint_base_url = dbutils.widgets.get("sharepoint_base_url").strip()
sharepoint_propietario_fuente = (
    dbutils.widgets.get("sharepoint_propietario_fuente").strip() or "DatosPortalDeInformacion"
)
metrics_delta_table = dbutils.widgets.get("metrics_delta_table").strip()

if not secret_scope.strip():
    raise ValueError(
        "secret_scope es obligatorio. Ejecuta el notebook desde el Databricks Job/Bundle "
        "o configura el widget secret_scope con el scope respaldado por Key Vault."
    )

if not storage_account_name.strip():
    raise ValueError(
        "storage_account_name es obligatorio. Ejecuta el notebook desde el Databricks Job/Bundle "
        "o configura el widget storage_account_name."
    )

if not sharepoint_base_url:
    raise ValueError(
        "sharepoint_base_url es obligatorio. Ejecuta el notebook desde el Databricks Job/Bundle "
        "o configura una URL base publica sin firmas ni tokens."
    )

control_secrets = read_sql_secret_values(
    dbutils,
    secret_scope,
    SqlSecretNames(
        server=dbutils.widgets.get("sql_control_server_secret").strip(),
        database=dbutils.widgets.get("sql_control_database_secret").strip(),
        username=dbutils.widgets.get("sql_control_username_secret").strip(),
        password=dbutils.widgets.get("sql_control_password_secret").strip(),
        server_value=sql_control_server,
        database_value=sql_control_database,
    ),
)

control_query = build_get_control_cargas_query(
    anio_mes_dia_inicial=anio_mes_dia_inicial,
    anio_mes_dia_final=anio_mes_dia_final,
    proceso=process_name,
    sistema_fuente=sistema_fuente,
)

df_control_raw = read_get_control_cargas_jdbc(
    spark,
    url=jdbc_url(
        control_secrets["server"],
        control_secrets["database"],
        encrypt=sql_control_encrypt,
        trust_server_certificate=sql_control_trust_server_certificate,
    ),
    username=control_secrets["username"],
    password=control_secrets["password"],
    query=control_query,
)

df_control = (
    normalize_spark_dataframe(df_control_raw)
    .filter(col("activo") == True)
    .filter(col("propietario_fuente") == sharepoint_propietario_fuente)
    .orderBy(col("orden_ejecucion").asc(), col("nombre_archivo_fuente").asc())
)

control_rows = [row.asDict(recursive=True) for row in df_control.collect()]
if not control_rows:
    raise ValueError(
        f"No existen registros activos de control para PropietarioFuente={sharepoint_propietario_fuente}"
    )

sp_secret_values = read_sharepoint_secret_values(
    dbutils,
    secret_scope,
    SharePointSecretNames(
        token=dbutils.widgets.get("sharepoint_token_secret").strip() or None,
        client_id=dbutils.widgets.get("sharepoint_client_id_secret").strip() or None,
        client_secret=dbutils.widgets.get("sharepoint_client_secret_secret").strip() or None,
        tenant_id=dbutils.widgets.get("sharepoint_tenant_id_secret").strip() or None,
    ),
)

base_url = sharepoint_base_url
assert_https_url(base_url)
reject_signed_or_secret_url(base_url)

token = sp_secret_values.get("token")
if not token:
    tenant_id = sp_secret_values["tenant_id"]
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_response = requests.post(
        token_url,
        data={
            "client_id": sp_secret_values["client_id"],
            "client_secret": sp_secret_values["client_secret"],
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        },
        timeout=60,
    )
    token_response.raise_for_status()
    token = token_response.json()["access_token"]

headers = {"Authorization": f"Bearer {token}"}
metrics = []
errors = []

for record in control_rows:
    archivo_origen = build_source_file_name(record)
    archivo_destino = build_bronze_path(
        storage_account=storage_account_name,
        folder=record["ruta_archivo_destino"],
        filename=record["nombre_archivo_destino"],
        extension=record["extension_archivo_destino"],
    )
    try:
        download_url = build_sharepoint_download_url(base_url, record)
        response = requests.get(download_url, headers=headers, timeout=300)
        response.raise_for_status()
        content = response.content
        file_type = infer_file_type(record)
        rows_read = count_rows_if_applicable(content, file_type)
        write_bytes_to_path(spark, archivo_destino, content)
        metrics.append(
            metric_record(
                archivo_origen=archivo_origen,
                archivo_destino=archivo_destino,
                bytes_read=len(content),
                rows_read=rows_read,
                status="success",
            )
        )
    except Exception as exc:
        error_message = str(exc)
        errors.append((archivo_origen, error_message))
        metrics.append(
            metric_record(
                archivo_origen=archivo_origen,
                archivo_destino=archivo_destino,
                bytes_read=0,
                rows_read=None,
                status="failed",
                error_message=error_message,
            )
        )
        if fail_fast:
            break

df_metrics = spark.createDataFrame(metrics)
df_metrics.createOrReplaceTempView(SHAREPOINT_METRICS_VIEW_NAME)

if metrics_delta_table:
    df_metrics.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(metrics_delta_table)

if errors:
    formatted = "; ".join([f"{source}: {message}" for source, message in errors])
    raise RuntimeError(f"Errores durante extraccion SharePoint: {formatted}")
