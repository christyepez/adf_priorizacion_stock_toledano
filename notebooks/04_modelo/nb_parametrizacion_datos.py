# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Modelo De Priorizacion - Parametrizacion Datos
# MAGIC
# MAGIC Orquesta la parametrizacion y ejecucion del modelo de indice de priorizacion.
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
dbutils.widgets.text("catalog_silver", "")
dbutils.widgets.text("catalog_gold", "")
dbutils.widgets.text("schema_sap", "sap")
dbutils.widgets.text("schema_sharepoint", "sharepoint")
dbutils.widgets.text("schema_atlas", "atlas")

from priorizacion_stock_toledano.model.model_parameters import obtener_tablas

ambiente = dbutils.widgets.get("ambiente")
catalog_silver = dbutils.widgets.get("catalog_silver").strip() or None
catalog_gold = dbutils.widgets.get("catalog_gold").strip() or None
schema_sap = dbutils.widgets.get("schema_sap").strip() or "sap"
schema_sharepoint = dbutils.widgets.get("schema_sharepoint").strip() or "sharepoint"
schema_atlas = dbutils.widgets.get("schema_atlas").strip() or "atlas"

tablas_modelo = obtener_tablas(
    ambiente,
    catalog_silver=catalog_silver,
    catalog_gold=catalog_gold,
    schema_sap=schema_sap,
    schema_sharepoint=schema_sharepoint,
    schema_atlas=schema_atlas,
)
