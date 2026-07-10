# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Control De Cargas - Get Control Cargas
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
dbutils.widgets.text("SistemaFuente", "SapHana")
dbutils.widgets.text("AñoMesDiaInicial", "0")
dbutils.widgets.text("AñoMesDiaFinal", "0")
dbutils.widgets.text("propietario_fuente", "")
dbutils.widgets.text("secret_scope", "kv-bigd-toledano-dev-01")
dbutils.widgets.text("sql_control_server", "")
dbutils.widgets.text("sql_control_database", "")
dbutils.widgets.text("sql_control_server_secret", "sc-sqlbigdatatoledano-server")
dbutils.widgets.text("sql_control_database_secret", "sc-sqlbigdatatoledano-database")
dbutils.widgets.text("sql_control_username_secret", "sc-sqlbigdatatoledano-username")
dbutils.widgets.text("sql_control_password_secret", "sc-sqlbigdatatoledano-password")
dbutils.widgets.text("sql_control_encrypt", "true")
dbutils.widgets.text("sql_control_trust_server_certificate", "false")
dbutils.widgets.text("audit_delta_enabled", "false")
dbutils.widgets.text("audit_delta_table", "")

from priorizacion_stock_toledano.control.get_control_cargas import (
    CONTROL_VIEW_NAME,
    SqlSecretNames,
    build_get_control_cargas_query,
    jdbc_url,
    normalize_spark_dataframe,
    read_get_control_cargas_jdbc,
    read_sql_secret_values,
)

from pyspark.sql.functions import col

ambiente = dbutils.widgets.get("ambiente")
proceso = dbutils.widgets.get("Proceso")
sistema_fuente = dbutils.widgets.get("SistemaFuente")
anio_mes_dia_inicial = dbutils.widgets.get("AñoMesDiaInicial")
anio_mes_dia_final = dbutils.widgets.get("AñoMesDiaFinal")
propietario_fuente = dbutils.widgets.get("propietario_fuente").strip()
secret_scope = dbutils.widgets.get("secret_scope").strip() or "kv-bigd-toledano-dev-01"
sql_control_server = dbutils.widgets.get("sql_control_server").strip()
sql_control_database = dbutils.widgets.get("sql_control_database").strip()
sql_control_encrypt = dbutils.widgets.get("sql_control_encrypt").strip() or "true"
sql_control_trust_server_certificate = (
    dbutils.widgets.get("sql_control_trust_server_certificate").strip() or "false"
)
audit_delta_enabled = dbutils.widgets.get("audit_delta_enabled").strip().lower() == "true"
audit_delta_table = dbutils.widgets.get("audit_delta_table").strip()

query = build_get_control_cargas_query(
    anio_mes_dia_inicial=anio_mes_dia_inicial,
    anio_mes_dia_final=anio_mes_dia_final,
    proceso=proceso,
    sistema_fuente=sistema_fuente,
)

secret_values = read_sql_secret_values(
    dbutils,
    secret_scope,
    SqlSecretNames(
        server=dbutils.widgets.get("sql_control_server_secret").strip() or "sc-sqlbigdatatoledano-server",
        database=dbutils.widgets.get("sql_control_database_secret").strip() or "sc-sqlbigdatatoledano-database",
        username=dbutils.widgets.get("sql_control_username_secret").strip() or "sc-sqlbigdatatoledano-username",
        password=dbutils.widgets.get("sql_control_password_secret").strip() or "sc-sqlbigdatatoledano-password",
        server_value=sql_control_server,
        database_value=sql_control_database,
    ),
)

df_raw = read_get_control_cargas_jdbc(
    spark,
    url=jdbc_url(
        secret_values["server"],
        secret_values["database"],
        encrypt=sql_control_encrypt,
        trust_server_certificate=sql_control_trust_server_certificate,
    ),
    username=secret_values["username"],
    password=secret_values["password"],
    query=query,
)

df_control = normalize_spark_dataframe(df_raw).filter(col("activo") == True)

if propietario_fuente:
    if propietario_fuente not in {"VistasSapHana", "DatosPortalDeInformacion"}:
        raise ValueError("propietario_fuente debe ser VistasSapHana o DatosPortalDeInformacion")
    df_control = df_control.filter(col("propietario_fuente") == propietario_fuente)

if df_control.limit(1).count() == 0:
    raise ValueError("GetControlCargas no devolvio registros activos para los parametros indicados")

df_control.createOrReplaceTempView(CONTROL_VIEW_NAME)

if audit_delta_enabled:
    if not audit_delta_table:
        raise ValueError("audit_delta_table es obligatorio cuando audit_delta_enabled=true")
    df_control.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(audit_delta_table)
