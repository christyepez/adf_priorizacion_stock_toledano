# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Publicacion - Publicar Resultados Sql
# MAGIC
# MAGIC Publica el resultado Gold hacia SQL Server mediante JDBC y Secret Scope.
# MAGIC
# MAGIC **Proyecto:** Priorizacion de Stock Toledano.
# MAGIC **Migracion:** Azure Data Factory a Databricks Asset Bundles.

# COMMAND ----------
# Comentarios de mantenimiento:
# - Mantener este notebook como orquestador de la etapa correspondiente.
# - Ubicar la logica reutilizable en src/priorizacion_stock_toledano.
# - Resolver credenciales, endpoints y tokens exclusivamente desde Secret Scope.
# - No imprimir secretos ni URLs firmadas en logs o salidas del notebook.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parametros y configuracion de entrada
# MAGIC Los widgets definidos a continuacion son inyectados por Databricks Jobs o por ejecuciones manuales controladas.

dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_atlas", "atlas")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("sql_publication_server_secret", "")
dbutils.widgets.text("sql_publication_database_secret", "")
dbutils.widgets.text("sql_publication_username_secret", "")
dbutils.widgets.text("sql_publication_password_secret", "")
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
secret_scope = dbutils.widgets.get("secret_scope").strip()
modo_publicacion = dbutils.widgets.get("modo_publicacion").strip() or "append"
target_schema = dbutils.widgets.get("target_schema").strip() or "dbo"
target_table = dbutils.widgets.get("target_table").strip() or "Int_Prioriza_Clientes"

if not ambiente:
    raise ValueError("El parametro 'ambiente' es requerido")

secret_names = SqlPublicationSecretNames(
    server=dbutils.widgets.get("sql_publication_server_secret").strip(),
    database=dbutils.widgets.get("sql_publication_database_secret").strip(),
    username=dbutils.widgets.get("sql_publication_username_secret").strip(),
    password=dbutils.widgets.get("sql_publication_password_secret").strip(),
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
    url=jdbc_url(secret_values["server"], secret_values["database"]),
    username=secret_values["username"],
    password=secret_values["password"],
    target_schema=target_schema,
    target_table=target_table,
    mode=modo_publicacion,
)

print(metrics.as_dict())

if metrics.status != "success":
    raise RuntimeError(f"Fallo publicacion SQL Server: {metrics.error_message}")
