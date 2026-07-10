# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Publicacion - Publicar Resultados Sql
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
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_atlas", "atlas")
dbutils.widgets.text("secret_scope", "kv-bigd-toledano-dev-01")
dbutils.widgets.text("sql_publication_server", "PTSVR2003")
dbutils.widgets.text("sql_publication_database", "ORC_SAP")
dbutils.widgets.text("sql_publication_username_secret", "sc-sql-orcpanama-username")
dbutils.widgets.text("sql_publication_password_secret", "sc-sql-orcpanama-password")
dbutils.widgets.text("sql_publication_encrypt", "true")
dbutils.widgets.text("sql_publication_trust_server_certificate", "true")
dbutils.widgets.text("modo_publicacion", "append")
dbutils.widgets.text("target_schema", "dbo")
dbutils.widgets.text("target_table", "Int_Prioriza_Clientes")

from priorizacion_stock_toledano.model.model_parameters import obtener_tablas
from priorizacion_stock_toledano.publication.sql_publisher import (
    SqlPublicationSecretNames,
    jdbc_url,
    publish_results_to_sql,
    read_sql_publication_secret_values,
)

ambiente = dbutils.widgets.get("ambiente").strip()
catalog_gold = dbutils.widgets.get("catalog_gold").strip() or None
schema_atlas = dbutils.widgets.get("schema_atlas").strip() or "atlas"
secret_scope = dbutils.widgets.get("secret_scope").strip() or "kv-bigd-toledano-dev-01"
sql_publication_server = dbutils.widgets.get("sql_publication_server").strip() or "PTSVR2003"
sql_publication_database = dbutils.widgets.get("sql_publication_database").strip() or "ORC_SAP"
sql_publication_encrypt = dbutils.widgets.get("sql_publication_encrypt").strip() or "true"
sql_publication_trust_server_certificate = (
    dbutils.widgets.get("sql_publication_trust_server_certificate").strip() or "true"
)
modo_publicacion = dbutils.widgets.get("modo_publicacion").strip() or "append"
target_schema = dbutils.widgets.get("target_schema").strip() or "dbo"
target_table = dbutils.widgets.get("target_table").strip() or "Int_Prioriza_Clientes"

if not ambiente:
    raise ValueError("El parametro 'ambiente' es requerido")

secret_names = SqlPublicationSecretNames(
    username=dbutils.widgets.get("sql_publication_username_secret").strip() or "sc-sql-orcpanama-username",
    password=dbutils.widgets.get("sql_publication_password_secret").strip() or "sc-sql-orcpanama-password",
)

secret_values = read_sql_publication_secret_values(dbutils, secret_scope, secret_names)
tablas_modelo = obtener_tablas(
    ambiente,
    catalog_gold=catalog_gold,
    schema_atlas=schema_atlas,
)

metrics = publish_results_to_sql(
    spark,
    source_table=tablas_modelo["TBL_OUTPUT_INDICE_PRIORIZACION"],
    url=jdbc_url(
        sql_publication_server,
        sql_publication_database,
        encrypt=sql_publication_encrypt,
        trust_server_certificate=sql_publication_trust_server_certificate,
    ),
    username=secret_values["username"],
    password=secret_values["password"],
    target_schema=target_schema,
    target_table=target_table,
    mode=modo_publicacion,
)

print(metrics.as_dict())

if metrics.status != "success":
    raise RuntimeError(f"Fallo publicacion SQL Server: {metrics.error_message}")
