# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Control De Cargas - Get Control Lakebase
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

from priorizacion_stock_toledano.config import define_text_widget

define_text_widget(dbutils, "ambiente", "dev")
define_text_widget(dbutils, "Proceso", "Modelo_Priorizacion_Stock")
define_text_widget(dbutils, "SistemaFuente", "SapHana")
define_text_widget(dbutils, "AñoMesDiaInicial", "0")
define_text_widget(dbutils, "AñoMesDiaFinal", "0")
define_text_widget(dbutils, "propietario_fuente", "")
define_text_widget(dbutils, "secret_scope", "")
define_text_widget(dbutils, "lakebase_host_secret", "")
define_text_widget(dbutils, "lakebase_port_secret", "")
define_text_widget(dbutils, "lakebase_database_secret", "")
define_text_widget(dbutils, "lakebase_username_secret", "")
define_text_widget(dbutils, "lakebase_password_secret", "")
define_text_widget(dbutils, "audit_delta_enabled", "false")
define_text_widget(dbutils, "audit_delta_table", "")

from pyspark.sql.functions import col

from priorizacion_stock_toledano.control.get_control_cargas import CONTROL_VIEW_NAME
from priorizacion_stock_toledano.control.lakebase_control import (
    LakebaseSecretNames,
    build_lakebase_control_query,
    create_control_temp_view,
    lakebase_jdbc_url,
    normalize_lakebase_control_dataframe,
    read_lakebase_control_jdbc,
    read_lakebase_secret_values,
)

proceso = dbutils.widgets.get("Proceso")
sistema_fuente = dbutils.widgets.get("SistemaFuente")
anio_mes_dia_inicial = dbutils.widgets.get("AñoMesDiaInicial")
anio_mes_dia_final = dbutils.widgets.get("AñoMesDiaFinal")
propietario_fuente = dbutils.widgets.get("propietario_fuente").strip() or None
secret_scope = dbutils.widgets.get("secret_scope").strip()
audit_delta_enabled = dbutils.widgets.get("audit_delta_enabled").strip().lower() == "true"
audit_delta_table = dbutils.widgets.get("audit_delta_table").strip()

query = build_lakebase_control_query(
    anio_mes_dia_inicial=anio_mes_dia_inicial,
    anio_mes_dia_final=anio_mes_dia_final,
    proceso=proceso,
    sistema_fuente=sistema_fuente,
    propietario_fuente=propietario_fuente,
)

secret_values = read_lakebase_secret_values(
    dbutils,
    secret_scope,
    LakebaseSecretNames(
        host=dbutils.widgets.get("lakebase_host_secret").strip(),
        port=dbutils.widgets.get("lakebase_port_secret").strip(),
        database=dbutils.widgets.get("lakebase_database_secret").strip(),
        username=dbutils.widgets.get("lakebase_username_secret").strip(),
        password=dbutils.widgets.get("lakebase_password_secret").strip(),
    ),
)

df_raw = read_lakebase_control_jdbc(
    spark,
    url=lakebase_jdbc_url(secret_values["host"], secret_values["port"], secret_values["database"]),
    username=secret_values["username"],
    password=secret_values["password"],
    query=query,
)

df_control = normalize_lakebase_control_dataframe(df_raw).filter(col("activo") == True)

if df_control.limit(1).count() == 0:
    raise ValueError("Lakebase control no devolvio registros activos para los parametros indicados")

create_control_temp_view(df_control, CONTROL_VIEW_NAME)

if audit_delta_enabled:
    if not audit_delta_table:
        raise ValueError("audit_delta_table es obligatorio cuando audit_delta_enabled=true")
    df_control.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(audit_delta_table)
