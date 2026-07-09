# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Transformacion Bronze To Silver - Bronze To Silver Cv Lo Pedido
# MAGIC
# MAGIC Aplica limpieza, tipado y escritura Delta para transformar datos Bronze hacia Silver.
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
dbutils.widgets.text("catalog_bronze", "")
dbutils.widgets.text("catalog_silver", "")
dbutils.widgets.text("schema_sap", "sap")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("execution_id", "")

from uuid import uuid4

from priorizacion_stock_toledano.transformations.sap_bronze_to_silver import (
    silver_table_path,
    transform_cv_lo_pedido,
)

ambiente = dbutils.widgets.get("ambiente")
catalog_bronze = dbutils.widgets.get("catalog_bronze") or f"toledano_bronze_{ambiente}"
catalog_silver = dbutils.widgets.get("catalog_silver") or f"toledano_silver_{ambiente}"
schema_sap = dbutils.widgets.get("schema_sap") or "sap"
storage_account = dbutils.widgets.get("storage_account")
execution_id = dbutils.widgets.get("execution_id").strip() or str(uuid4())

source_table = f"{catalog_bronze}.{schema_sap}.fact_cv_lo_pedido"
target_table = f"{catalog_silver}.{schema_sap}.fact_cv_lo_pedido"
target_path = silver_table_path(storage_account, "sap/fact_cv_lo_pedido")

df_source = spark.read.table(source_table)
rows_read = df_source.count()
df_target = transform_cv_lo_pedido(df_source, execution_id)

if not spark.catalog.tableExists(target_table):
    df_target.limit(0).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        target_table,
        path=target_path,
    )

df_target.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
rows_written = df_target.count()

print(
    {
        "source_table": source_table,
        "target_table": target_table,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "execution_id": execution_id,
    }
)
