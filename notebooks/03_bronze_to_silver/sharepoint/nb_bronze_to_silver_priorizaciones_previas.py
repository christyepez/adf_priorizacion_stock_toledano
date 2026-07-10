# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Transformacion Bronze To Silver - Bronze To Silver Priorizaciones Previas
# MAGIC
# MAGIC Priorizacion de Stock Toledano.

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("catalog_bronze", "")
dbutils.widgets.text("catalog_silver", "")
dbutils.widgets.text("schema_sharepoint", "sharepoint")
dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("execution_id", "")

from uuid import uuid4

from priorizacion_stock_toledano.transformations.sharepoint_bronze_to_silver import transform_priorizaciones_previas
from priorizacion_stock_toledano.transformations.sap_bronze_to_silver import silver_table_path

ambiente = dbutils.widgets.get("ambiente")
catalog_bronze = dbutils.widgets.get("catalog_bronze") or f"toledano_bronze_{ambiente}"
catalog_silver = dbutils.widgets.get("catalog_silver") or f"toledano_silver_{ambiente}"
schema_sharepoint = dbutils.widgets.get("schema_sharepoint") or "sharepoint"
storage_account_name = dbutils.widgets.get("storage_account_name")
execution_id = dbutils.widgets.get("execution_id").strip() or str(uuid4())

source_table = f"{catalog_bronze}.{schema_sharepoint}.priorizaciones_previas"
target_table = f"{catalog_silver}.{schema_sharepoint}.priorizaciones_previas"
target_path = silver_table_path(storage_account_name, "sharepoint/datos_portal_de_informacion/priorizaciones_previas")

df_source = spark.read.table(source_table)
rows_read = df_source.count()
df_target = transform_priorizaciones_previas(df_source, execution_id)

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
